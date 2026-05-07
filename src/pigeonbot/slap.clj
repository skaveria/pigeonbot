(ns pigeonbot.slap
  (:require [clojure.edn :as edn]
            [clojure.string :as str]
            [org.httpkit.client :as http]
            [cheshire.core :as json]
            [pigeonbot.config :as config]
            [pigeonbot.context :as ctx]
            [pigeonbot.db :as db]))

;; -----------------------------------------------------------------------------
;; OpenAI
;; -----------------------------------------------------------------------------

(defn- openai-cfg []
  (let [m (config/load-config)]
    {:base-url (or (:openai-base-url m) "https://api.openai.com")
     :api-key  (or (:openai-api-key m) (System/getenv "OPENAI_API_KEY"))
     :model    (or (:openai-model m) "gpt-5.5")
     :timeout  (long (or (:brain-timeout-ms m) 90000))
     :max-output-tokens (long (or (:openai-max-output-tokens m) 1400))
     :temperature (if (number? (:openai-temperature m))
                    (:openai-temperature m)
                    0)}))

(defn- endpoint [base path]
  (str (str/replace (or base "") #"/+$" "") path))

(defn- decode-body [body]
  (cond
    (nil? body) {}
    (map? body) body
    (string? body) (try (json/decode body true) (catch Throwable _ {}))
    :else {}))

(defn- extract-output-text [resp-body]
  (let [m (decode-body resp-body)]
    (or
     (when (string? (:output_text m)) (:output_text m))
     (let [out (or (:output m) [])
           texts (->> out
                      (mapcat
                       (fn [item]
                         (let [content (:content item)]
                           (cond
                             (sequential? content)
                             (keep (fn [c]
                                     (or (:text c)
                                         (:output_text c)))
                                   content)

                             (string? content)
                             [content]

                             :else []))))
                      (map str)
                      (remove str/blank?)
                      vec)]
       (when (seq texts) (str/join "" texts)))
     (get-in m [:error :message])
     (str resp-body))))

(defn- openai-edn! [instructions user-text]
  (let [{:keys [base-url api-key model timeout max-output-tokens temperature]} (openai-cfg)
        _ (when-not (and (string? api-key) (seq api-key))
            (throw (ex-info "Missing OpenAI API key" {})))
        ep (endpoint base-url "/v1/responses")

        payload {:model model
         :instructions (str instructions)
         :input (str user-text)
         :max_output_tokens max-output-tokens}
        headers {"Authorization" (str "Bearer " api-key)
                 "Content-Type" "application/json"}
        {:keys [status body error]}
        @(http/post ep {:headers headers
                        :body (json/encode payload)
                        :timeout timeout})]
    (cond
      error
      (throw (ex-info "OpenAI request failed" {:error (str error) :endpoint ep}))

      (or (nil? status) (not (<= 200 status 299)))
      (throw (ex-info "OpenAI non-2xx" {:status status :body body :endpoint ep}))

      :else
      (-> (extract-output-text body) str str/trim))))

;; -----------------------------------------------------------------------------
;; Basic helpers
;; -----------------------------------------------------------------------------

(defn- now-iso [] (str (java.time.Instant/now)))

(defn- new-id [] (str (java.util.UUID/randomUUID)))

(defn- clamp-chars [s n]
  (let [s (str (or s ""))]
    (if (<= (count s) (long n))
      s
      (str (subs s 0 (max 0 (dec (long n)))) "…"))))

(defn- safe-subs [s n]
  (subs (str s) 0 (min n (count (str s)))))

(defn- iso-or-str [x]
  (cond
    (nil? x) ""
    (instance? java.util.Date x) (str (.toInstant ^java.util.Date x))
    :else (str x)))

;; -----------------------------------------------------------------------------
;; Instructions
;; -----------------------------------------------------------------------------

(defn- slap-system-instructions []
  ["You are a SLAP responder."
   "You MUST output EDN only. No markdown. No prose outside EDN."
   "Output MUST be exactly one EDN map with keys:"
   "  :slap/version :packet/id :sufficient? :answer :extract :query-back :meta"
   "Use :slap/version \"0.1\"."
   ""
   "CRITICAL EVIDENCE POLICY:"
   "- The packet has explicit evidence lanes."
   "- :current-turn is the user's actual message."
   "- :recent-window is continuity-only. Treat it as vibes unless confirmed elsewhere."
   "- :durable-extracts, :repo, and :fts-history are stronger factual grounding than recency."
   "- Recent chat MUST NOT dominate just because it is recent."
   "- Do not make hard claims from recent-window alone unless the user is asking about immediate context."
   "- If a claim comes only from recent-window, hedge it or say it is based on immediate chat context."
   "- Prefer durable extracts, repo snippets, and FTS history for memory/factual answers."
   ""
   "Deictic/immediate-context exception:"
   "- Recent-window may be primary ONLY when the user clearly asks about immediate context:"
   "  examples: \"that\", \"this\", \"what did they just say\", \"continue\", \"summarize this\", replies, or followups."
   ""
   "Output shape:"
   "- :extract must be a vector."
   "- Each extract item must be either a string OR a map."
   "- Extract maps must include :text, :kind, :confidence, :tags."
   "- :kind is a keyword like :note, :fact, :decision, :todo."
   "- :confidence is 0.0–1.0."
   "- :tags is a vector of 3–8 lowercase strings, no spaces."
   "- :query-back must be a vector, possibly empty."
   "- :meta must be a map."
   ""
   "Query-back:"
   "- ONLY use tool :datalevin/fts. It is a compatibility name backed by SQLite FTS."
   "- Query-back shape:"
   "{:query/id \"q1\" :tool :datalevin/fts :query {:fts \"concrete tokens\"} :expected {:limit 10} :purpose \"...\" :priority 1}"
   "- Use concrete nouns, model names, usernames, project names, and short phrases."
   ""
   "Answer length:"
   "- Default to 4–8 sentences."
   "- Be concise but not empty."
   "- Follow the persona voice."])

(defn- persona-instructions []
  (let [cfg (config/load-config)
        p (some-> (:persona-prompt cfg) str str/trim)]
    (str/join
     "\n"
     (cond-> (vec (slap-system-instructions))
       (seq p)
       (into [""
              "PERSONA:"
              p
              ""
              "Style reminder: playful, sassy pigeon entity; avoid generic assistant tone."])))))

;; -----------------------------------------------------------------------------
;; EDN parsing, lenient for newer models
;; -----------------------------------------------------------------------------

(defn- strip-codefences [s]
  (-> (str (or s ""))
      (str/replace #"(?s)^\s*```[a-zA-Z0-9_-]*\s*" "")
      (str/replace #"(?s)\s*```\s*$" "")
      str/trim))

(defn- try-edn [s]
  (try
    (edn/read-string {:readers {} :default (fn [_tag v] v)} s)
    (catch Throwable _ nil)))

(defn- balanced-map-snippets [s]
  (let [s (str (or s ""))]
    (loop [i 0 depth 0 start nil acc []]
      (if (>= i (count s))
        acc
        (let [ch (.charAt ^String s i)]
          (cond
            (= ch \{)
            (recur (inc i)
                   (inc depth)
                   (or start i)
                   acc)

            (= ch \})
            (let [depth' (dec depth)]
              (if (and start (zero? depth'))
                (recur (inc i) 0 nil (conj acc (subs s start (inc i))))
                (recur (inc i) depth' start acc)))

            :else
            (recur (inc i) depth start acc)))))))

(defn- safe-edn-read [raw]
  (let [s (strip-codefences raw)]
    (or (try-edn s)
        (some try-edn (reverse (sort-by count (balanced-map-snippets s))))
        (throw (ex-info "SLAP: model returned non-EDN"
                        {:raw (safe-subs raw 1600)})))))

(defn- validate-response! [resp packet-id]
  (when-not (map? resp)
    (throw (ex-info "SLAP: response not a map" {:resp resp})))
  (when-not (= "0.1" (:slap/version resp))
    (throw (ex-info "SLAP: wrong version" {:got (:slap/version resp)})))
  (when-not (= packet-id (:packet/id resp))
    (throw (ex-info "SLAP: packet id mismatch"
                    {:expected packet-id :got (:packet/id resp)})))
  (doseq [k [:sufficient? :answer :extract :query-back :meta]]
    (when-not (contains? resp k)
      (throw (ex-info "SLAP: missing key" {:missing k :resp resp}))))
  (when-not (vector? (:extract resp))
    (throw (ex-info "SLAP: :extract must be vector" {:extract (:extract resp)})))
  (when-not (or (nil? (:query-back resp)) (vector? (:query-back resp)))
    (throw (ex-info "SLAP: :query-back must be vector or nil" {:query-back (:query-back resp)})))
  (when-not (map? (:meta resp))
    (throw (ex-info "SLAP: :meta must be map" {:meta (:meta resp)})))
  resp)

;; -----------------------------------------------------------------------------
;; Persona
;; -----------------------------------------------------------------------------

(defn- bot-persona []
  (let [cfg (config/load-config)]
    {:bot/name (or (:bot-name cfg) "pigeonbot")
     :bot/version (or (:bot-version cfg) "dev")
     :persona/id (or (:persona-id cfg) :pigeonbot/haunted-cozy)
     :persona/style (or (:persona-style cfg) #{:cozy :wry :haunted :concise})
     :persona/voice {:typing-indicator? true}
     :persona/prompt (:persona-prompt cfg)}))

;; -----------------------------------------------------------------------------
;; Tags / deictic detection
;; -----------------------------------------------------------------------------

(def ^:private stopwords
  #{"the" "a" "an" "and" "or" "to" "of" "in" "on" "for" "with" "is" "it" "that"
    "this" "be" "are" "was" "were" "as" "at" "by" "from" "but" "so" "if" "you"
    "we" "i" "they" "he" "she" "them" "their" "our" "your" "its" "about" "what"
    "saying" "said" "talking" "discuss" "discussion" "summarize" "quick" "test"
    "tell" "me" "please" "could" "would" "should" "explain" "how" "does" "work"})

(defn- deictic-query? [msg question]
  (let [q (str/lower-case (str (or question "")))
        reply? (boolean (or (get-in msg [:message_reference :message_id])
                            (get-in msg [:message_reference :message-id])
                            (get-in msg [:message-reference :message_id])
                            (get-in msg [:message-reference :message-id])
                            (get-in msg [:referenced_message :id])))]
    (or reply?
        (boolean
         (re-find #"\b(that|this|these|those|it|they|them|above|previous|last|earlier|just|continue|summarize this|what did .*say|what was .*saying)\b" q)))))

(defn- candidate-tags-from-question [s]
  (let [s (-> (or s "") str/lower-case)
        modelish-a (re-seq #"\b[a-z]{1,8}\d{1,8}[a-z]?\b" s)
        modelish-b (re-seq #"\b\d{1,8}[a-z]{1,8}\b" s)
        words (->> (re-seq #"[a-z][a-z0-9\-_]{2,}" s)
                   (remove stopwords))
        compounds (cond-> []
                    (and (re-find #"\btrigger\b" s)
                         (re-find #"\bguard\b" s))
                    (conj "trigger-guard")
                    (and (re-find #"\b3d\b" s)
                         (re-find #"\bprint" s))
                    (conj "3d-printing"))]
    (->> (concat modelish-a modelish-b compounds words)
         (map #(str/replace % #"[^a-z0-9\-]+" ""))
         (remove str/blank?)
         distinct
         (take 12)
         vec)))

;; -----------------------------------------------------------------------------
;; Evidence lanes
;; -----------------------------------------------------------------------------

(defn- recent-window [msg question]
  (let [cfg (config/load-config)
        deictic? (deictic-query? msg question)
        n (long (or (:slap-recent-max-messages cfg)
                    (if deictic? 8 3)))
        maxc (long (or (:slap-recent-context-chars cfg)
                       (if deictic? 1200 450)))
        cid (some-> (:channel-id msg) str)
        rows (if (seq cid)
               (->> (db/recent-channel-messages cid n)
                    reverse
                    vec)
               [])]
    {:lane/type :recent-window
     :evidence/priority :low
     :evidence/policy :continuity-only
     :recency/deictic? deictic?
     :recent/max-messages n
     :recent/context (clamp-chars (ctx/context-text msg) maxc)
     :rows rows}))

(defn- current-turn [msg question]
  (let [{:keys [channel-id guild-id id author]} msg
        author-id (some-> (or (:id author) (get-in author [:user :id])) str)
        author-name (or (:global_name author)
                        (:username author)
                        (get-in author [:user :username])
                        "unknown")]
    {:lane/type :current-turn
     :evidence/priority :highest-for-user-intent
     :message/id (some-> id str)
     :guild/id (some-> guild-id str)
     :channel/id (some-> channel-id str)
     :author/id author-id
     :author/name author-name
     :content (str question)}))

(defn- durable-extracts-lane [msg question]
  (let [cfg (config/load-config)
        gid (some-> (:guild-id msg) str)
        limit (long (or (:slap-related-extract-limit cfg) 20))
        tags (candidate-tags-from-question question)
        rows (if (and (seq gid) (seq tags))
               (db/extracts-by-tag-overlap gid tags limit)
               [])]
    {:lane/type :durable-extracts
     :evidence/priority :high
     :evidence/policy :durable-grounding
     :tags tags
     :rows rows}))

(defn- repo-lane [question]
  (let [cfg (config/load-config)
        limit (long (or (:slap-repo-preseed-limit cfg) 6))
        snip (long (or (:slap-repo-snippet-chars cfg) 900))
        q (-> (or question "") str str/trim)
        rows (if (str/blank? q)
               []
               (db/repo-search q {:limit limit :snippet-chars snip :min-score 2}))]
    {:lane/type :repo
     :evidence/priority :high-for-code
     :evidence/policy :durable-grounding
     :query q
     :rows rows}))

(defn- fts-history-lane [msg question]
  (let [cfg (config/load-config)
        limit (long (or (:slap-preseed-limit cfg) 25))
        gid (some-> (:guild-id msg) str)
        cid (some-> (:channel-id msg) str)
        q (-> (or question "") str str/trim)
        hits (if (str/blank? q)
               []
               (db/fulltext q {:limit limit}))
        rows (->> hits
                  (keep (fn [[id attr _txt]]
                          (when (= attr :message/content)
                            (db/pull-message id))))
                  (filter (fn [m]
                            (cond
                              (seq gid) (= (some-> (:message/guild-id m) str) gid)
                              (seq cid) (= (some-> (:message/channel-id m) str) cid)
                              :else true)))
                  vec)]
    {:lane/type :fts-history
     :evidence/priority :medium-high
     :evidence/policy :historical-grounding
     :query q
     :rows rows}))

(defn- topic-padding-lane [msg question]
  (let [cfg (config/load-config)
        gid (some-> (:guild-id msg) str)
        limit (long (or (:slap-topic-related-limit cfg) 20))
        topics (candidate-tags-from-question question)
        rows (if (and (seq gid) (seq topics))
               (db/topic-related-message-rows gid topics limit)
               [])]
    {:lane/type :topic-padding
     :evidence/priority :medium
     :evidence/policy :discovery-padding
     :topics topics
     :rows rows}))

(defn- vocab-lane [msg _question]
  (let [cfg (config/load-config)
        enabled? (not= false (:slap-vocab-enabled? cfg))
        limit (long (or (:slap-vocab-limit cfg) 25))
        gid (some-> (:guild-id msg) str)]
    (if (and enabled? (seq gid))
      {:lane/type :vocab
       :evidence/priority :low-medium
       :evidence/policy :query-planning-only
       :top/extract-tags (db/top-extract-tags gid {:limit limit})
       :top/topic-tags (db/top-topic-tags gid {:limit limit})}
      {:lane/type :vocab
       :evidence/priority :low-medium
       :evidence/policy :query-planning-only
       :top/extract-tags []
       :top/topic-tags []})))

(defn- evidence-lanes [msg question]
  {:evidence/policy {:recent/context :continuity-only
                     :recent/max-messages (long (or (:slap-recent-max-messages (config/load-config)) 3))
                     :hard-claims-require [:durable-extracts :repo :fts-history]
                     :recency/governor :penalize-unless-deictic}
   :current-turn (current-turn msg question)
   :recent-window (recent-window msg question)
   :durable-extracts (durable-extracts-lane msg question)
   :repo (repo-lane question)
   :fts-history (fts-history-lane msg question)
   :topic-padding (topic-padding-lane msg question)
   :vocab (vocab-lane msg question)})

;; -----------------------------------------------------------------------------
;; Packet
;; -----------------------------------------------------------------------------

(defn- build-request [{:keys [packet-id conversation-id depth msg evidence]}]
  (let [{:keys [channel-id guild-id author content]} msg
        author-id (some-> (or (:id author) (get-in author [:user :id])) str)
        author-name (or (:global_name author)
                        (:username author)
                        (get-in author [:user :username])
                        "unknown")]
    {:slap/version "0.1"
     :packet/id packet-id
     :conversation/id (or conversation-id packet-id)
     :depth depth
     :identity {:actor {:platform :discord
                        :user/id author-id
                        :user/name author-name}
                :bot (bot-persona)
                :scope {:guild/id (some-> guild-id str)
                        :channel/id (some-> channel-id str)}}
     :temporal {:now (now-iso)}
     :knowledge {:capabilities (db/slap-capabilities {:guild-id guild-id
                                                      :channel-id channel-id})
                 :lanes evidence}
     :task {:goal "Answer the user's message using the evidence lanes. Use recent-window only for continuity unless deictic/immediate-context."
            :user-message (str (or content ""))}}))

;; -----------------------------------------------------------------------------
;; Query-back
;; -----------------------------------------------------------------------------

(defn- fts-evidence [msg item]
  (let [qid (or (:query/id item) (new-id))
        q (or (get-in item [:query :fts]) "")
        limit (long (or (get-in item [:expected :limit]) 10))
        gid (some-> (:guild-id msg) str)
        cid (some-> (:channel-id msg) str)
        hits (db/fulltext q {:limit limit})
        rows (->> hits
                  (keep (fn [[id attr _txt]]
                          (case attr
                            :message/content (db/pull-message id)
                            :extract/text {:extract/id id :extract/text _txt}
                            nil)))
                  (filter (fn [m]
                            (if (:message/id m)
                              (cond
                                (seq gid) (= (some-> (:message/guild-id m) str) gid)
                                (seq cid) (= (some-> (:message/channel-id m) str) cid)
                                :else true)
                              true)))
                  vec)]
    {:lane/type :query-back-fts
     :evidence/priority :medium-high
     :evidence/policy :historical-grounding
     :query/id qid
     :purpose (or (:purpose item) "")
     :fts q
     :rows rows}))

(defn- run-query-back [msg query-back]
  (->> (or query-back [])
       (sort-by (fn [q] (or (:priority q) 999)))
       (mapcat (fn [q]
                 (case (:tool q)
                   :datalevin/fts [(fts-evidence msg q)]
                   [])))
       vec))

(defn- auto-query-back [question]
  (let [q (-> (or question "") str str/trim)
        tags (candidate-tags-from-question q)
        tag-q (when (seq tags) (str/join " " (take 6 tags)))
        words (->> (re-seq #"[A-Za-z0-9][A-Za-z0-9\-\_]{2,}" q)
                   (map str/lower-case)
                   (remove stopwords)
                   (take 8)
                   vec)]
    (->> [(when (seq tag-q)
            {:query/id "auto1"
             :tool :datalevin/fts
             :query {:fts tag-q}
             :expected {:limit 12}
             :purpose "Auto recursion: search concrete tokens from the question."
             :priority 1})
          (when (seq words)
            {:query/id "auto2"
             :tool :datalevin/fts
             :query {:fts (str/join " " words)}
             :expected {:limit 10}
             :purpose "Auto recursion: search filtered non-stopword phrase."
             :priority 2})]
         (remove nil?)
         vec)))

(defn- merge-query-back-lanes [evidence new-lanes]
  (update evidence :query-back-history
          (fnil into [])
          new-lanes))

;; -----------------------------------------------------------------------------
;; Extract normalization
;; -----------------------------------------------------------------------------

(defn- normalize-extract-item [x]
  (cond
    (nil? x) nil

    (string? x)
    (let [s (str/trim x)]
      (when (seq s)
        {:text s :kind :note :confidence 0.65 :tags []}))

    (map? x)
    (let [txt (or (:text x) (:content x) (:extract/text x) (:extract/content x))
          txt (some-> txt str str/trim)
          kind (or (:kind x) (:extract/kind x) :note)
          conf (double (or (:confidence x) (:extract/confidence x) 0.75))
          tags (or (:tags x) (:extract/tags x) [])]
      (when (seq txt)
        {:text txt
         :kind (if (keyword? kind) kind (keyword (str kind)))
         :confidence conf
         :tags (->> tags
                    (map str)
                    (map str/lower-case)
                    (map str/trim)
                    (remove str/blank?)
                    distinct
                    vec)}))

    :else nil))

(defn- normalize-extracts [extract]
  (->> (or extract [])
       (map normalize-extract-item)
       (remove nil?)
       vec))

;; -----------------------------------------------------------------------------
;; Public API
;; -----------------------------------------------------------------------------

(defn build-first-packet
  "REPL helper: show the first SLAP packet."
  [msg]
  (db/ensure-conn!)
  (let [packet-id (new-id)
        conversation-id (new-id)
        question (str (or (:content msg) ""))
        lanes (evidence-lanes msg question)]
    (build-request {:packet-id packet-id
                    :conversation-id conversation-id
                    :depth 0
                    :msg msg
                    :evidence lanes})))

(defn run-slap!
  "Run SLAP loop for a Discord message map.
  Returns {:answer \"...\" :resp <full slap response> :depth n}."
  ([msg] (run-slap! msg {}))
  ([msg {:keys [max-depth max-queries]
         :or {max-depth 3 max-queries 8}}]
   (db/ensure-conn!)
   (let [{:keys [model]} (openai-cfg)
         packet-id (new-id)
         conversation-id (new-id)
         question (str (or (:content msg) ""))
         seed (evidence-lanes msg question)
         write-ctx {:guild-id (:guild-id msg)
                    :channel-id (:channel-id msg)
                    :message-id (:id msg)
                    :packet-id packet-id
                    :model (str model)}]
     (loop [depth 0
            evidence seed
            queries-used 0]
       (let [req (build-request {:packet-id packet-id
                                 :conversation-id conversation-id
                                 :depth depth
                                 :msg msg
                                 :evidence evidence})
             raw (openai-edn! (persona-instructions) (pr-str req))
             resp0 (-> raw safe-edn-read (validate-response! packet-id))
             ex (normalize-extracts (:extract resp0))
             qb0 (vec (or (:query-back resp0) []))
             qb (if (and (not (true? (:sufficient? resp0)))
                         (empty? qb0)
                         (< queries-used max-queries))
                  (auto-query-back question)
                  qb0)
             resp (assoc resp0 :extract ex :query-back qb)
             sufficient? (true? (:sufficient? resp))]

         (try
           (db/upsert-extracts! write-ctx ex)
           (catch Throwable t
             (println "extract write error:" (.getMessage t))))

         (cond
           sufficient?
           {:answer (str (:answer resp)) :resp resp :depth depth}

           (>= depth max-depth)
           {:answer (str (:answer resp)) :resp resp :depth depth}

           (>= queries-used max-queries)
           {:answer (str (:answer resp)) :resp resp :depth depth}

           (empty? qb)
           {:answer (str (:answer resp)) :resp resp :depth depth}

           :else
           (let [new-lanes (run-query-back msg qb)]
             (if (empty? new-lanes)
               {:answer (str (:answer resp)) :resp resp :depth depth}
               (recur (inc depth)
                      (merge-query-back-lanes evidence new-lanes)
                      (+ queries-used (count qb)))))))))))

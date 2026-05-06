(ns pigeonbot.slap
  (:require [clojure.edn :as edn]
            [clojure.string :as str]
            [org.httpkit.client :as http]
            [cheshire.core :as json]
            [pigeonbot.config :as config]
            [pigeonbot.context :as ctx]
            [pigeonbot.db :as db])
  (:import (java.time Instant)
           (java.util UUID)))

;; -----------------------------------------------------------------------------
;; Small helpers
;; -----------------------------------------------------------------------------

(defn- now-iso [] (str (Instant/now)))
(defn- new-id [] (str (UUID/randomUUID)))

(defn- clamp-chars [s n]
  (let [s (str (or s ""))]
    (if (<= (count s) (long n))
      s
      (str (subs s 0 (max 0 (dec (long n)))) "…"))))

(defn- trim-str [x]
  (some-> x str str/trim))

(defn- present? [x]
  (boolean (seq (str (or x "")))))

(defn- endpoint [base path]
  (str (str/replace (or base "") #"/+$" "") path))

(defn- openai-cfg []
  (let [m (config/load-config)]
    {:base-url (or (:openai-base-url m) "https://api.openai.com")
     :api-key  (or (:openai-api-key m) (System/getenv "OPENAI_API_KEY"))
     :model    (or (:openai-model m) "gpt-5.5")
     :timeout  (long (or (:brain-timeout-ms m) 90000))
     :max-output-tokens (or (:openai-max-output-tokens m) 1600)
     :temperature (when (number? (:openai-temperature m))
                    (:openai-temperature m))}))

;; -----------------------------------------------------------------------------
;; Instructions
;; -----------------------------------------------------------------------------

(defn- slap-system-instructions []
  ["You are a SLAP responder for pigeonbot."
   "You are running inside a Discord server."
   ""
   "OUTPUT FORMAT:"
   "- Prefer EDN."
   "- Output a single map."
   "- Do not include markdown fences."
   "- Do not include prose outside the map."
   ""
   "Required shape:"
   "{:slap/version \"0.1\""
   " :packet/id \"...\""
   " :sufficient? true"
   " :answer \"...\""
   " :extract []"
   " :query-back []"
   " :meta {}}"
   ""
   "LENIENCY NOTE:"
   "- If you accidentally think in JSON, still keep the same keys conceptually."
   "- The caller will attempt to parse both EDN and JSON, but EDN is preferred."
   ""
   "Evidence priority:"
   "- Treat :temporal/recent/context as low-priority vibes."
   "- Prefer :knowledge/evidence and :knowledge/capabilities."
   "- Never cross guild boundaries."
   ""
   "Query-back:"
   "- ONLY use tool :datalevin/fts."
   "- The name is legacy; it is backed by SQLite FTS5 now."
   "- Query-back item shape:"
   "{:query/id \"q1\" :tool :datalevin/fts :query {:fts \"...\"} :expected {:limit 10} :purpose \"...\" :priority 1}"
   ""
   "Extracts:"
   "- :extract must be a vector."
   "- Extract items may be strings or maps."
   "- Extract maps should include :text, :kind, :confidence, :tags."
   "- Tags are lowercase short tokens, no spaces."
   ""
   "Answer length:"
   "- Default to 3–8 useful sentences."
   "- Be playful and pigeon-like, but do not sacrifice correctness."
   "- Never include secrets."])

(defn- persona-instructions []
  (let [cfg (config/load-config)
        p (some-> (:persona-prompt cfg) str str/trim)]
    (cond-> (vec (slap-system-instructions))
      (seq p)
      (into [""
             "PERSONA:"
             p
             ""
             "Style reminder: playful, sassy pigeon entity; avoid generic assistant tone."]))))

(defn- bot-persona []
  (let [cfg (config/load-config)]
    {:bot/name (or (:bot-name cfg) "pigeonbot")
     :bot/version (or (:bot-version cfg) "dev")
     :persona/id (or (:persona-id cfg) :pigeonbot/haunted-cozy)
     :persona/style (or (:persona-style cfg) #{:cozy :wry :haunted :concise})
     :persona/voice {:typing-indicator? true}
     :persona/prompt (:persona-prompt cfg)}))

;; -----------------------------------------------------------------------------
;; OpenAI Responses API
;; -----------------------------------------------------------------------------

(defn- decode-body [body]
  (cond
    (nil? body) {}
    (map? body) body
    (string? body) (try (json/decode body true) (catch Throwable _ {}))
    :else {}))

(defn- extract-output-text [resp-body]
  (let [m (decode-body resp-body)]
    (or
     (when (string? (:output_text m))
       (:output_text m))
     (let [out (or (:output m) [])
           texts (->> out
                      (mapcat
                       (fn [item]
                         (let [content (:content item)]
                           (cond
                             (sequential? content)
                             (keep (fn [c]
                                     (or (:text c)
                                         (:output_text c)
                                         (get-in c [:text :value])))
                                   content)

                             (string? content)
                             [content]

                             :else
                             []))))
                      (map str)
                      (remove str/blank?)
                      vec)]
       (when (seq texts)
         (str/join "" texts)))
     (get-in m [:error :message])
     (str resp-body))))

(defn- openai-edn! [instructions user-text]
  (let [{:keys [base-url api-key model timeout max-output-tokens temperature]} (openai-cfg)
        _ (when-not (and (string? api-key) (seq api-key))
            (throw (ex-info "Missing OpenAI API key" {})))
        ep (endpoint base-url "/v1/responses")
        payload (cond-> {:model model
                         :instructions (str/join "\n" instructions)
                         :input (str user-text)
                         :max_output_tokens (long max-output-tokens)}
                  (number? temperature) (assoc :temperature temperature))
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
      (throw (ex-info "OpenAI returned non-2xx" {:status status :body body :endpoint ep}))

      :else
      (-> (extract-output-text body) str str/trim))))

;; -----------------------------------------------------------------------------
;; Lenient response parsing
;; -----------------------------------------------------------------------------

(defn- strip-code-fences [s]
  (-> (str (or s ""))
      (str/replace #"(?s)^\s*```[a-zA-Z0-9_-]*\s*" "")
      (str/replace #"(?s)\s*```\s*$" "")
      str/trim))

(defn- map-snippet [s]
  (when-let [ms (seq (re-seq #"(?s)\{.*\}" (str s)))]
    (last (sort-by count ms))))

(defn- keywordize-deep [x]
  (cond
    (map? x)
    (into {}
          (map (fn [[k v]]
                 [(cond
                    (keyword? k) k
                    (string? k) (keyword (str/replace k #"^:" ""))
                    :else k)
                  (keywordize-deep v)]))
          x)

    (vector? x) (mapv keywordize-deep x)
    (sequential? x) (mapv keywordize-deep x)
    :else x))

(defn- try-edn [s]
  (try
    (edn/read-string {:readers {} :default (fn [_tag v] v)} s)
    (catch Throwable _ nil)))

(defn- try-json [s]
  (try
    (keywordize-deep (json/decode s true))
    (catch Throwable _ nil)))

(defn- safe-read-response [raw]
  (let [s (strip-code-fences raw)
        snippet (map-snippet s)]
    (or (try-edn s)
        (try-json s)
        (when snippet (try-edn snippet))
        (when snippet (try-json snippet))
        {:slap/version "0.1"
         :packet/id nil
         :sufficient? true
         :answer (str s)
         :extract []
         :query-back []
         :meta {:parse/fallback true}})))

(defn- normalize-query-back [x]
  (cond
    (nil? x) []
    (vector? x) x
    (sequential? x) (vec x)
    (map? x) [x]
    :else []))

(defn- normalize-extract-item [x]
  (cond
    (nil? x) nil

    (string? x)
    (let [s (str/trim x)]
      (when (seq s) s))

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
         :tags (vec tags)}))

    :else nil))

(defn- normalize-extracts [extract]
  (->> (cond
         (nil? extract) []
         (vector? extract) extract
         (sequential? extract) extract
         :else [extract])
       (map normalize-extract-item)
       (remove nil?)
       vec))

(defn- validate-response [resp packet-id]
  (let [resp (if (map? resp) resp {})
        resp (assoc resp
                    :slap/version (or (:slap/version resp) "0.1")
                    :packet/id (or (:packet/id resp) packet-id)
                    :sufficient? (boolean (:sufficient? resp))
                    :answer (str (or (:answer resp) ""))
                    :extract (normalize-extracts (:extract resp))
                    :query-back (normalize-query-back (:query-back resp))
                    :meta (if (map? (:meta resp)) (:meta resp) {}))]
    resp))

;; -----------------------------------------------------------------------------
;; Tag generation
;; -----------------------------------------------------------------------------

(def ^:private stopwords
  #{"the" "a" "an" "and" "or" "to" "of" "in" "on" "for" "with" "is" "it" "that"
    "this" "be" "are" "was" "were" "as" "at" "by" "from" "but" "so" "if" "you"
    "we" "i" "they" "he" "she" "them" "their" "our" "your" "its" "about" "what"
    "saying" "said" "talking" "discuss" "discussion" "summarize" "quick" "test"
    "tell" "me" "please" "could" "would" "should" "explain" "how" "does" "work"})

(defn- candidate-tags-from-question [s]
  (let [s (-> (or s "") str/lower-case)
        modelish-a (re-seq #"\b[a-z]{1,8}\d{1,8}[a-z]?\b" s)
        modelish-b (re-seq #"\b\d{1,8}[a-z]{1,8}\b" s)
        words (->> (re-seq #"[a-z][a-z0-9\-_]{2,}" s)
                   (remove stopwords))
        compounds (cond-> []
                    (and (re-find #"\btrigger\b" s)
                         (re-find #"\bguard\b" s))
                    (conj "trigger-guard"))]
    (->> (concat modelish-a modelish-b compounds words)
         (map #(str/replace % #"[^a-z0-9\-]+" ""))
         (remove str/blank?)
         distinct
         (take 16)
         vec)))

;; -----------------------------------------------------------------------------
;; Evidence builders: SQLite db API only
;; -----------------------------------------------------------------------------

(defn- recent-messages-from-db [msg]
  (let [cfg (config/load-config)
        n (long (or (:slap-recent-datalevin-limit cfg) 12))
        cid (some-> (:channel-id msg) str)]
    (if-not (seq cid)
      []
      (->> (db/recent-channel-messages cid n)
           reverse
           (map (fn [m]
                  {:message/id (:message/id m)
                   :ts (:message/ts m)
                   :author/name (:message/author-name m)
                   :content (:message/content m)
                   :channel/id (:message/channel-id m)}))
           vec))))

(defn- trimmed-recent-context [msg]
  (let [cfg (config/load-config)
        maxc (long (or (:slap-recent-context-chars cfg) 700))]
    (clamp-chars (ctx/context-text msg) maxc)))

(defn- preseed-evidence [msg question]
  (let [cfg (config/load-config)
        limit (long (or (:slap-preseed-limit cfg) 25))
        q (trim-str question)
        gid (some-> (:guild-id msg) str)
        cid (some-> (:channel-id msg) str)]
    (if-not (seq q)
      []
      (let [rows (db/search-messages q {:guild-id gid
                                        :channel-id nil
                                        :limit limit})]
        (if (seq rows)
          [{:evidence/type :sqlite/preseed
            :purpose "Top relevant chat messages from SQLite FTS for the current question."
            :fts q
            :rows rows}]
          [])))))

(defn- related-extract-evidence [msg question]
  (let [cfg (config/load-config)
        gid (some-> (:guild-id msg) str)
        limit (long (or (:slap-related-extract-limit cfg) 20))
        tags (candidate-tags-from-question question)]
    (if (or (not (seq gid)) (empty? tags))
      []
      (let [rows (db/extracts-by-tag-overlap gid tags limit)]
        (if (seq rows)
          [{:evidence/type :sqlite/extract-related
            :purpose "Related prior extracts in this guild matched by tag overlap."
            :tags tags
            :rows rows}]
          [])))))

(defn- topic-related-message-evidence [msg question]
  (let [cfg (config/load-config)
        gid (some-> (:guild-id msg) str)
        limit (long (or (:slap-topic-related-limit cfg) 20))
        topics (candidate-tags-from-question question)]
    (if (or (not (seq gid)) (empty? topics))
      []
      (let [rows (db/topic-related-message-rows gid topics limit)]
        (if (seq rows)
          [{:evidence/type :sqlite/topic-related
            :purpose "Guild-wide messages matched by topic overlap."
            :topics topics
            :rows rows}]
          [])))))

(defn- vocab-padding-evidence [msg _question]
  (let [cfg (config/load-config)
        enabled? (not= false (:slap-vocab-enabled? cfg))
        limit (long (or (:slap-vocab-limit cfg) 25))
        gid (some-> (:guild-id msg) str)]
    (if (or (not enabled?) (not (seq gid)))
      []
      (let [extract-tags (db/top-extract-tags gid {:limit limit})
            topic-tags (db/top-topic-tags gid {:limit limit})]
        (if (or (seq extract-tags) (seq topic-tags))
          [{:evidence/type :sqlite/vocab
            :purpose "Vocabulary hints: common tags/topics in this guild."
            :guild/id gid
            :top/extract-tags extract-tags
            :top/topic-tags topic-tags}]
          [])))))

(defn- repo-preseed-evidence [question]
  (let [cfg (config/load-config)
        limit (long (or (:slap-repo-preseed-limit cfg) 6))
        snip (long (or (:slap-repo-snippet-chars cfg) 900))
        q (trim-str question)]
    (if-not (seq q)
      []
      (let [rows (db/repo-search q {:limit limit
                                    :snippet-chars snip
                                    :min-score 2})]
        (if (seq rows)
          [{:evidence/type :sqlite/repo-preseed
            :purpose "Relevant repo files retrieved from local indexed repo."
            :query q
            :rows rows}]
          [])))))

(defn- seed-evidence [msg question]
  (vec
   (concat
    (related-extract-evidence msg question)
    (topic-related-message-evidence msg question)
    (vocab-padding-evidence msg question)
    (repo-preseed-evidence question)
    (preseed-evidence msg question))))

;; -----------------------------------------------------------------------------
;; Packet builder
;; -----------------------------------------------------------------------------

(defn- build-request
  [{:keys [packet-id conversation-id depth msg evidence]}]
  (let [{:keys [channel-id guild-id id content author]} msg
        author-id (some-> (or (:id author) (get-in author [:user :id])) str)
        author-name (or (:global_name author)
                        (:username author)
                        (get-in author [:user :username])
                        "unknown")
        capabilities (db/slap-capabilities {:guild-id (some-> guild-id str)
                                            :channel-id (some-> channel-id str)})]
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
     :temporal {:now (now-iso)
                :recent/messages [{:message/id (some-> id str)
                                   :ts (now-iso)
                                   :author/id author-id
                                   :author/name author-name
                                   :content (str (or content ""))}]
                :recent/context (trimmed-recent-context msg)
                :recent/sqlite (recent-messages-from-db msg)}
     :knowledge {:capabilities capabilities
                 :evidence (vec evidence)}
     :task {:goal "Answer the user's message using available evidence. If insufficient, request query-back."}}))

;; -----------------------------------------------------------------------------
;; Query-back
;; -----------------------------------------------------------------------------

(defn- fts-evidence [msg item]
  (let [qid (or (:query/id item) (new-id))
        purpose (or (:purpose item) "")
        q (or (get-in item [:query :fts]) "")
        limit (long (or (get-in item [:expected :limit]) 10))
        gid (some-> (:guild-id msg) str)
        rows (db/search-messages q {:guild-id gid :limit limit})]
    {:evidence/type :sqlite/fts
     :query/id qid
     :purpose purpose
     :fts q
     :rows rows}))

(defn- run-query-back [msg query-back]
  (->> (or query-back [])
       (sort-by (fn [q] (or (:priority q) 999)))
       (mapcat
        (fn [q]
          (case (:tool q)
            :datalevin/fts [(fts-evidence msg q)]
            :sqlite/fts [(fts-evidence msg q)]
            [])))
       vec))

(defn- auto-query-back [question]
  (let [q (trim-str question)
        tags (candidate-tags-from-question q)
        tag-q (when (seq tags) (str/join " " (take 6 tags)))
        words (->> (re-seq #"[A-Za-z0-9][A-Za-z0-9\-\_]{2,}" (or q ""))
                   (map str/lower-case)
                   (remove stopwords)
                   (take 8)
                   vec)
        phrase-q (when (seq words) (str/join " " (take 5 words)))]
    (->> [(when (seq tag-q)
            {:query/id "auto1"
             :tool :datalevin/fts
             :query {:fts tag-q}
             :expected {:limit 12}
             :purpose "Auto recursion: search concrete tokens from the question."
             :priority 1})
          (when (seq phrase-q)
            {:query/id "auto2"
             :tool :datalevin/fts
             :query {:fts phrase-q}
             :expected {:limit 10}
             :purpose "Auto recursion: search phrase from the question."
             :priority 2})]
         (remove nil?)
         vec)))

;; -----------------------------------------------------------------------------
;; Public API
;; -----------------------------------------------------------------------------

(defn build-first-packet
  "REPL helper: show the first SLAP request packet."
  [msg]
  (db/ensure-conn!)
  (let [packet-id (new-id)
        conversation-id (new-id)
        question (str (or (:content msg) ""))
        seed (seed-evidence msg question)]
    (build-request {:packet-id packet-id
                    :conversation-id conversation-id
                    :depth 0
                    :msg msg
                    :evidence seed})))

(defn run-slap!
  "Run SLAP loop for a Discord message map.
  Returns {:answer \"...\" :resp <full response> :depth n}."
  ([msg] (run-slap! msg {}))
  ([msg {:keys [max-depth max-queries]
         :or {max-depth 3 max-queries 8}}]
   (db/ensure-conn!)
   (let [{:keys [model]} (openai-cfg)
         packet-id (new-id)
         conversation-id (new-id)
         question (str (or (:content msg) ""))
         seed (seed-evidence msg question)
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
             resp0 (-> raw safe-read-response (validate-response packet-id))
             qb0 (vec (or (:query-back resp0) []))
             qb (if (and (not (true? (:sufficient? resp0)))
                         (empty? qb0)
                         (< queries-used max-queries))
                  (auto-query-back question)
                  qb0)
             resp (assoc resp0 :query-back qb)
             sufficient? (true? (:sufficient? resp))]

         (try
           (db/upsert-extracts! write-ctx (:extract resp))
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
           (let [new-evidence (run-query-back msg qb)
                 qb-count (count qb)]
             (if (empty? new-evidence)
               {:answer (str (:answer resp)) :resp resp :depth depth}
               (recur (inc depth)
                      (into (vec evidence) new-evidence)
                      (+ queries-used qb-count))))))))))

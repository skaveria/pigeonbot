(ns pigeonbot.slap
  (:require [clojure.edn :as edn]
            [clojure.string :as str]
            [org.httpkit.client :as http]
            [cheshire.core :as json]
            [pigeonbot.config :as config]
            [pigeonbot.context :as ctx]
            [pigeonbot.db :as db]))

(defn- new-id [] (str (java.util.UUID/randomUUID)))
(defn- now-iso [] (.toString (java.time.Instant/now)))

(defn- clamp-chars [s n]
  (let [s (str (or s ""))]
    (if (<= (count s) (long n))
      s
      (str (subs s 0 (max 0 (dec (long n)))) "…"))))

(defn- openai-cfg []
  (let [m (config/load-config)]
    {:base-url (or (:openai-base-url m) "https://api.openai.com")
     :api-key  (or (:openai-api-key m) (System/getenv "OPENAI_API_KEY"))
     :model    (or (:openai-model m) "gpt-5.5")
     :timeout  (long (or (:brain-timeout-ms m) 90000))
     :temperature (:openai-temperature m)
     :max-output-tokens (or (:openai-max-output-tokens m) 1200)}))

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
                                         (:output_text c)
                                         (get-in c [:text :value])))
                                   content)

                             (string? content)
                             [content]

                             :else []))))
                      (map str)
                      (remove str/blank?)
                      vec)]
       (when (seq texts) (str/join "" texts)))
     (get-in m [:choices 0 :message :content])
     (get-in m [:error :message])
     (str resp-body))))

(defn- openai-edn! [instructions user-text]
  (let [{:keys [base-url api-key model timeout temperature max-output-tokens]} (openai-cfg)
        _ (when-not (and (string? api-key) (seq api-key))
            (throw (ex-info "Missing OpenAI API key" {})))
        ep (endpoint base-url "/v1/responses")
        payload (cond-> {:model model
                         :instructions (str instructions)
                         :input (str user-text)
                         :max_output_tokens (long max-output-tokens)}
                  (number? temperature)
                  (assoc :temperature temperature))
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

(defn- slap-system-instructions []
  ["You are a SLAP responder."
   "You MUST output EDN only."
   "Output a single EDN map with keys:"
   "  :slap/version :packet/id :sufficient? :answer :extract :query-back :meta"
   "Use :slap/version \"0.1\"."
   ""
   "IMPORTANT: no markdown, no code fences, no prose outside EDN."
   "If you accidentally need to explain uncertainty, put it inside :answer."
   ""
   ":extract must be a vector."
   "Each extract item may be a string or a map."
   "Map extracts should include :text :kind :confidence :tags."
   ""
   ":query-back must be a vector."
   "Use ONLY tool :datalevin/fts for compatibility, even though storage is now SQLite."
   "Query-back item shape:"
   "{:query/id \"q1\" :tool :datalevin/fts :query {:fts \"...\"} :expected {:limit 10} :purpose \"...\" :priority 1}"
   ""
   "Evidence priority:"
   "- Prefer :knowledge/evidence over :temporal."
   "- Treat recent context as low-priority vibe unless directly relevant."
   "- Never cross guild boundaries."
   ""
   "Answer length:"
   "- Usually 3–10 sentences."
   "- Be concrete and useful."
   ""
   "Never include secrets."])

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

(defn- strip-codefences [s]
  (-> (str (or s ""))
      (str/replace #"(?s)^\s*```[a-zA-Z0-9_-]*\s*" "")
      (str/replace #"(?s)\s*```\s*$" "")
      str/trim))

(defn- try-edn [s]
  (try
    (edn/read-string {:readers {} :default (fn [_ v] v)} s)
    (catch Throwable _ nil)))

(defn- best-edn-map-snippet [s]
  (let [s (str (or s ""))]
    (when-let [ms (seq (re-seq #"(?s)\{.*\}" s))]
      (last (sort-by count ms)))))

(defn- coerce-response-map [raw packet-id]
  (let [raw (str (or raw ""))
        s (strip-codefences raw)
        parsed (or (try-edn s)
                   (when-let [m (best-edn-map-snippet s)]
                     (try-edn m)))]
    (if (map? parsed)
      parsed
      {:slap/version "0.1"
       :packet/id packet-id
       :sufficient? true
       :answer (str/trim raw)
       :extract []
       :query-back []
       :meta {:coerced-non-edn? true}})))

(defn- validate-response! [resp packet-id]
  (let [resp (cond-> resp
               (not (contains? resp :slap/version)) (assoc :slap/version "0.1")
               (not (contains? resp :packet/id)) (assoc :packet/id packet-id)
               (not (contains? resp :sufficient?)) (assoc :sufficient? true)
               (not (contains? resp :answer)) (assoc :answer "")
               (not (contains? resp :extract)) (assoc :extract [])
               (not (contains? resp :query-back)) (assoc :query-back [])
               (not (contains? resp :meta)) (assoc :meta {}))]
    (assoc resp
           :slap/version "0.1"
           :packet/id packet-id
           :extract (vec (or (:extract resp) []))
           :query-back (vec (or (:query-back resp) []))
           :meta (if (map? (:meta resp)) (:meta resp) {}))))

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
        words (->> (re-seq #"[a-z]{3,}" s)
                   (remove stopwords))
        compounds (cond-> []
                    (and (re-find #"\btrigger\b" s)
                         (re-find #"\bguard\b" s))
                    (conj "trigger-guard"))]
    (->> (concat modelish-a modelish-b compounds words)
         (map #(str/replace % #"[^a-z0-9\-]+" ""))
         (remove str/blank?)
         distinct
         (take 12)
         vec)))

(defn- bot-persona []
  (let [cfg (config/load-config)]
    {:bot/name (or (:bot-name cfg) "pigeonbot")
     :bot/version (or (:bot-version cfg) "dev")
     :persona/id (or (:persona-id cfg) :pigeonbot/haunted-cozy)
     :persona/style (or (:persona-style cfg) #{:cozy :wry :haunted :concise})
     :persona/voice {:typing-indicator? true}
     :persona/prompt (:persona-prompt cfg)}))

(defn- trimmed-recent-context [msg]
  (let [cfg (config/load-config)
        maxc (long (or (:slap-recent-context-chars cfg) 700))]
    (clamp-chars (ctx/context-text msg) maxc)))

(defn- recent-messages-from-db [msg]
  (let [cfg (config/load-config)
        n (long (or (:slap-recent-datalevin-limit cfg) 12))
        cid (some-> (:channel-id msg) str)]
    (if (seq cid)
      (db/recent-messages cid n)
      [])))

(defn- preseed-evidence [msg question]
  (let [cfg (config/load-config)
        limit (long (or (:slap-preseed-limit cfg) 25))
        q (-> (or question "") str str/trim)
        gid (some-> (:guild-id msg) str)
        cid (some-> (:channel-id msg) str)
        rows (when (seq q)
               (db/search-messages q {:guild-id gid :channel-id nil :limit limit}))]
    (if (seq rows)
      [{:evidence/type :sqlite/preseed
        :compat/type :datalevin/preseed
        :purpose "Top relevant chat messages from SQLite FTS for the current question."
        :fts q
        :rows rows}]
      [])))

(defn- related-extract-evidence [msg question]
  (let [cfg (config/load-config)
        gid (some-> (:guild-id msg) str)
        limit (long (or (:slap-related-extract-limit cfg) 20))
        tags (candidate-tags-from-question question)
        rows (when (and (seq gid) (seq tags))
               (db/related-extracts-by-tags gid tags {:limit limit}))]
    (if (seq rows)
      [{:evidence/type :sqlite/extract-related
        :compat/type :datalevin/extract-related
        :purpose "Related prior extracts in this guild matched by tag overlap."
        :tags tags
        :rows rows}]
      [])))

(defn- topic-related-message-evidence [msg question]
  (let [cfg (config/load-config)
        gid (some-> (:guild-id msg) str)
        limit (long (or (:slap-topic-related-limit cfg) 20))
        topics (candidate-tags-from-question question)
        rows (when (and (seq gid) (seq topics))
               (db/messages-by-topics gid topics {:limit limit}))]
    (if (seq rows)
      [{:evidence/type :sqlite/topic-related
        :compat/type :datalevin/topic-related
        :purpose "Guild-wide messages matched by topic overlap."
        :topics topics
        :rows rows}]
      [])))

(defn- repo-preseed-evidence [question]
  (let [cfg (config/load-config)
        limit (long (or (:slap-repo-preseed-limit cfg) 6))
        snip (long (or (:slap-repo-snippet-chars cfg) 900))
        q (-> (or question "") str str/trim)
        rows (when (seq q)
               (db/repo-search q {:limit limit :snippet-chars snip :min-score 2}))]
    (if (seq rows)
      [{:evidence/type :sqlite/repo-preseed
        :compat/type :datalevin/repo-preseed
        :purpose "Relevant repo files retrieved from local indexed repo."
        :query q
        :rows rows}]
      [])))

(defn- vocab-padding-evidence [msg _question]
  (let [cfg (config/load-config)
        enabled? (not= false (:slap-vocab-enabled? cfg))
        limit (long (or (:slap-vocab-limit cfg) 25))
        gid (some-> (:guild-id msg) str)]
    (if (and enabled? (seq gid))
      (let [extract-tags (db/top-extract-tags gid {:limit limit})
            topic-tags (db/top-topic-tags gid {:limit limit})]
        (if (or (seq extract-tags) (seq topic-tags))
          [{:evidence/type :sqlite/vocab
            :purpose "Vocabulary hints: common tags/topics in this guild."
            :guild/id gid
            :top/extract-tags extract-tags
            :top/topic-tags topic-tags}]
          []))
      [])))

(defn- build-request [{:keys [packet-id conversation-id depth msg evidence]}]
  (let [{:keys [channel-id guild-id id content author]} msg
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
     :temporal {:now (now-iso)
                :recent/messages [{:message/id (some-> id str)
                                   :ts (now-iso)
                                   :author/id author-id
                                   :author/name author-name
                                   :content (str (or content ""))}]
                :recent/context (trimmed-recent-context msg)
                :recent/sqlite (recent-messages-from-db msg)}
     :knowledge {:capabilities (db/slap-capabilities {:guild-id guild-id
                                                       :channel-id channel-id})
                 :evidence (vec evidence)}
     :task {:goal "Answer the user's message using available evidence. If insufficient, request query-back."}}))

(defn- fts-evidence [msg item]
  (let [qid (or (:query/id item) (new-id))
        purpose (or (:purpose item) "")
        q (or (get-in item [:query :fts]) "")
        limit (long (or (get-in item [:expected :limit]) 10))
        gid (some-> (:guild-id msg) str)
        rows (if (seq q)
               (db/search-messages q {:guild-id gid :limit limit})
               [])]
    {:evidence/type :sqlite/fts
     :compat/type :datalevin/fts
     :query/id qid
     :purpose purpose
     :fts q
     :rows rows}))

(defn- run-query-back [msg query-back]
  (->> (or query-back [])
       (sort-by #(or (:priority %) 999))
       (mapcat
        (fn [q]
          (case (:tool q)
            :datalevin/fts [(fts-evidence msg q)]
            :sqlite/fts [(fts-evidence msg q)]
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
             :purpose "Auto recursion: search important words from the question."
             :priority 2})]
         (remove nil?)
         vec)))

(defn- normalize-extract-item [x]
  (cond
    (nil? x) nil

    (string? x)
    (let [s (str/trim x)]
      (when (seq s) s))

    (map? x)
    (let [txt (some-> (or (:text x) (:content x) (:extract/text x) (:extract/content x))
                      str str/trim)
          kind (or (:kind x) (:extract/kind x) :note)
          conf (double (or (:confidence x) (:extract/confidence x) 0.75))
          tags (or (:tags x) (:extract/tags x) [])]
      (when (seq txt)
        {:text txt :kind kind :confidence conf :tags tags}))

    :else nil))

(defn- normalize-extracts [extract]
  (->> (or extract [])
       (map normalize-extract-item)
       (remove nil?)
       vec))

(defn- seed-evidence [msg question]
  (vec (concat
        (related-extract-evidence msg question)
        (topic-related-message-evidence msg question)
        (vocab-padding-evidence msg question)
        (repo-preseed-evidence question)
        (preseed-evidence msg question))))

(defn build-first-packet [msg]
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
  Returns {:answer \"...\" :resp <full slap response> :depth n}."
  ([msg] (run-slap! msg {}))
  ([msg {:keys [max-depth max-queries]
         :or {max-depth 3 max-queries 8}}]
   (db/ensure-conn!)
   (let [{:keys [model]} (openai-cfg)
         packet-id (new-id)
         conversation-id (new-id)
         question (str (or (:content msg) ""))
         write-ctx {:guild-id (:guild-id msg)
                    :channel-id (:channel-id msg)
                    :message-id (:id msg)
                    :packet-id packet-id
                    :model (str model)}]
     (loop [depth 0
            evidence (seed-evidence msg question)
            queries-used 0]
       (let [req (build-request {:packet-id packet-id
                                 :conversation-id conversation-id
                                 :depth depth
                                 :msg msg
                                 :evidence evidence})
             raw (openai-edn! (persona-instructions) (pr-str req))
             resp0 (-> raw
                       (coerce-response-map packet-id)
                       (validate-response! packet-id))
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
           (let [new-evidence (run-query-back msg qb)
                 qb-count (count qb)]
             (if (empty? new-evidence)
               {:answer (str (:answer resp)) :resp resp :depth depth}
               (recur (inc depth)
                      (into (vec evidence) new-evidence)
                      (+ queries-used qb-count)))))))))))

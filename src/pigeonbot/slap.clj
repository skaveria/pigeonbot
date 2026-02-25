(ns pigeonbot.slap
  (:require [clojure.edn :as edn]
            [clojure.string :as str]
            [org.httpkit.client :as http]
            [cheshire.core :as json]
            [datalevin.core :as dlv]
            [pigeonbot.config :as config]
            [pigeonbot.context :as ctx]
            [pigeonbot.db :as db]))

;; -----------------------------------------------------------------------------
;; OpenAI (Responses API) - strict EDN responder
;; -----------------------------------------------------------------------------

(defn- openai-cfg []
  (let [m (config/load-config)]
    {:base-url (or (:openai-base-url m) "https://api.openai.com")
     :api-key  (or (:openai-api-key m) (System/getenv "OPENAI_API_KEY"))
     :model    (or (:openai-model m) "gpt-4.1-mini")
     :timeout  (long (or (:brain-timeout-ms m) 60000))}))

(defn- endpoint [base path]
  (str (str/replace (or base "") #"/+$" "") path))

(defn- decode-body [body]
  (cond
    (nil? body) {}
    (map? body) body
    (string? body) (json/decode body true)
    :else {}))

(defn- extract-output-text
  "Extract assistant text from OpenAI Responses API result."
  [resp-body]
  (let [m (decode-body resp-body)]
    (or
      (when (string? (:output_text m)) (:output_text m))
      (let [out (or (:output m) [])
            texts (->> out
                       (mapcat (fn [item]
                                 (let [content (:content item)]
                                   (cond
                                     (sequential? content)
                                     (->> content
                                          (keep (fn [c]
                                                  (cond
                                                    (string? (:text c)) (:text c)
                                                    (string? (:output_text c)) (:output_text c)
                                                    :else nil))))
                                     (string? content) [content]
                                     :else []))))
                       (map str)
                       (remove str/blank?)
                       vec)]
        (when (seq texts) (str/join "" texts)))
      (get-in m [:error :message])
      (str resp-body))))

(defn- openai-edn!
  "Call OpenAI, forcing EDN-only output."
  [system-lines user-text]
  (let [{:keys [base-url api-key model timeout]} (openai-cfg)
        _ (when-not (and (string? api-key) (seq api-key))
            (throw (ex-info "Missing OpenAI API key (set OPENAI_API_KEY or :openai-api-key in config.edn)" {})))
        ep (endpoint base-url "/v1/responses")
        payload {:model model
                 :instructions (str/join "\n" system-lines)
                 :input (str user-text)}
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
;; SLAP helpers
;; -----------------------------------------------------------------------------

(defn- now-iso []
  (.toString (java.time.OffsetDateTime/now)))

(defn- new-id [] (str (java.util.UUID/randomUUID)))

(defn- iso-or-str
  "Render timestamps nicely. Datalevin stores instants as java.util.Date in our setup."
  [x]
  (cond
    (instance? java.util.Date x) (.toInstant ^java.util.Date x) ;; prints like 2026-...
    :else (str x)))

(defn- slap-system-instructions []
  ["You are a SLAP responder."
   "You MUST output EDN only. Do not output any prose outside EDN."
   "Output MUST be a single EDN map with keys:"
   "  :slap/version :packet/id :sufficient? :answer :extract :query-back :meta"
   "Use :slap/version \"0.1\"."
   ""
   "IMPORTANT OUTPUT SHAPE:"
   "- :extract must be a vector (possibly empty)."
   "- :query-back must be a vector (possibly empty), never nil."
   "- :meta must be a map (possibly empty)."
   ""
   "Evidence usage:"
   "- You may be given :knowledge {:evidence [...]}, containing chat messages from Datalevin."
   "- Prefer grounding your answer in those messages."
   "- If evidence is missing or insufficient, set :sufficient? false and request :query-back."
   ""
   "Query-back:"
   "- ONLY use tool :datalevin/fts."
   "- Each query-back item should look like:"
   "{:query/id \"q1\" :tool :datalevin/fts :query {:fts \"...\"} :expected {:limit 10} :purpose \"...\" :priority 1}"
   ""
   "Persona:"
   "- You will be given :identity/bot persona details. Follow that voice."
   ""
   "Style:"
   "- Keep :answer concise (1-3 sentences unless asked for more)."
   "- Never include secrets."])

(defn- safe-edn-read [s]
  (try
    (edn/read-string {:readers {} :default (fn [_tag v] v)} s)
    (catch Throwable t
      (throw (ex-info "SLAP: model returned non-EDN"
                      {:raw (subs (str s) 0 (min 600 (count (str s))))}
                      t)))))

(defn- validate-response!
  [resp packet-id]
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
  ;; tolerate nil from model but normalize later; still warn via validation error if you want strict:
  (when-not (or (nil? (:query-back resp)) (vector? (:query-back resp)))
    (throw (ex-info "SLAP: :query-back must be vector or nil" {:query-back (:query-back resp)})))
  (when-not (map? (:meta resp))
    (throw (ex-info "SLAP: :meta must be map" {:meta (:meta resp)})))
  resp)

(defn- bot-persona
  "Persona block for :identity/bot. Configurable, with sane defaults."
  []
  (let [cfg (config/load-config)]
    {:bot/name (or (:bot-name cfg) "pigeonbot")
     :bot/version (or (:bot-version cfg) "dev")
     :persona/id (or (:persona-id cfg) :pigeonbot/haunted-cozy)
     :persona/style (or (:persona-style cfg) #{:cozy :wry :haunted :concise})
     :persona/voice {:typing-indicator? true}
     ;; Optional: you can set this to your Gef/pigeonbot voice prompt
     :persona/prompt (:persona-prompt cfg)}))

(defn- scope-match?
  "Keep evidence scoped. Prefer guild scope if msg has guild-id, otherwise channel scope."
  [msg m]
  (let [gid (some-> (:guild-id msg) str)
        cid (some-> (:channel-id msg) str)
        mgid (some-> (:message/guild-id m) str)
        mcid (some-> (:message/channel-id m) str)]
    (cond
      (seq gid) (= gid mgid)
      (seq cid) (= cid mcid)
      :else true)))

(defn- pull-message
  "Pull a compact message map by eid."
  [dbv eid]
  (try
    (dlv/pull dbv
              [:message/id
               :message/ts
               :message/guild-id
               :message/channel-id
               :message/author-id
               :message/author-name
               :message/bot?
               :message/content]
              eid)
    (catch Throwable _ nil)))

(defn- recent-messages-from-db
  "Fetch last N messages in this scope from Datalevin for temporal context."
  [msg n]
  (let [dbv (db/db)
        gid (some-> (:guild-id msg) str)
        cid (some-> (:channel-id msg) str)
        rows (cond
               (seq gid)
               (dlv/q '[:find ?ts ?author ?txt ?mid ?chid
                        :in $ ?gid
                        :where
                        [?e :message/guild-id ?gid]
                        [?e :message/ts ?ts]
                        [?e :message/author-name ?author]
                        [?e :message/content ?txt]
                        [?e :message/id ?mid]
                        [?e :message/channel-id ?chid]]
                      dbv gid)

               (seq cid)
               (dlv/q '[:find ?ts ?author ?txt ?mid
                        :in $ ?cid
                        :where
                        [?e :message/channel-id ?cid]
                        [?e :message/ts ?ts]
                        [?e :message/author-name ?author]
                        [?e :message/content ?txt]
                        [?e :message/id ?mid]]
                      dbv cid)

               :else [])]
    (->> rows
         (sort-by first)
         (take-last (long n))
         (map (fn [row]
                (let [[ts author txt mid chid] row]
                  (cond-> {:message/id (str mid)
                           :ts (str (iso-or-str ts))
                           :author/name (str author)
                           :content (str txt)}
                    chid (assoc :channel/id (str chid))))))
         vec)))

(defn- preseed-evidence
  "Initial 'slap spice' from Datalevin.
  - Run FTS on the question
  - Pull top N messages
  - Filter to same guild/channel scope"
  [msg question]
  (let [cfg (config/load-config)
        limit (long (or (:slap-preseed-limit cfg) 25))
        q (-> (or question "") str str/trim)]
    (if (str/blank? q)
      []
      (let [hits (take limit (db/fulltext q))
            eids (->> hits (map first) distinct vec)
            dbv (db/db)
            rows (->> eids
                      (map (fn [eid] (pull-message dbv eid)))
                      (remove nil?)
                      (filter (partial scope-match? msg))
                      vec)]
        (if (seq rows)
          [{:evidence/type :datalevin/preseed
            :purpose "Top relevant chat messages from Datalevin for the current question."
            :fts q
            :rows rows}]
          [])))))

(defn- build-request
  [{:keys [packet-id conversation-id depth msg evidence]}]
  (let [{:keys [channel-id guild-id id content author]} msg
        author-id (some-> (or (:id author) (get-in author [:user :id])) str)
        author-name (or (:global_name author)
                        (:username author)
                        (get-in author [:user :username])
                        "unknown")
        recent-context (ctx/context-text msg)
        recent-db (recent-messages-from-db msg (long (or (:slap-recent-limit (config/load-config)) 30)))]
    {:slap/version "0.1"
     :packet/id packet-id
     :conversation/id (or conversation-id packet-id)
     :depth depth

     ;; Identity layer includes both actor and bot persona
     :identity {:actor {:platform :discord
                        :user/id author-id
                        :user/name author-name}
                :bot (bot-persona)
                :scope {:guild/id (some-> guild-id str)
                        :channel/id (some-> channel-id str)}}

     ;; Temporal is now richer + explicit
     :temporal {:now (now-iso)
                :recent/messages [{:message/id (some-> id str)
                                   :ts (now-iso)
                                   :author/id author-id
                                   :author/name author-name
                                   :content (str (or content ""))}]
                :recent/context recent-context
                :recent/datalevin recent-db}

     :knowledge {:evidence (vec evidence)}
     :task {:goal "Answer the user's message using available evidence. If insufficient, request query-back."}}))

(defn- fts-evidence
  "Execute a single :datalevin/fts query-back item."
  [msg item]
  (let [qid (or (:query/id item) (new-id))
        purpose (or (:purpose item) "")
        q (or (get-in item [:query :fts]) "")
        limit (long (or (get-in item [:expected :limit]) 10))
        hits (take limit (db/fulltext q))
        eids (->> hits (map first) distinct vec)
        dbv (db/db)
        rows (->> eids
                  (map (fn [eid] (pull-message dbv eid)))
                  (remove nil?)
                  (filter (partial scope-match? msg))
                  vec)]
    {:evidence/type :datalevin/fts
     :query/id qid
     :purpose purpose
     :fts q
     :rows rows}))

(defn- run-query-back
  "Execute :query-back items. Returns vector of evidence maps."
  [msg query-back]
  (->> (or query-back [])
       (sort-by (fn [q] (or (:priority q) 999)))
       (mapcat
        (fn [q]
          (case (:tool q)
            :datalevin/fts [(fts-evidence msg q)]
            [])))
       vec))

;; -----------------------------------------------------------------------------
;; Public API
;; -----------------------------------------------------------------------------

(defn build-first-packet
  "REPL helper: show the exact first request packet (with preseed + temporal)."
  [msg]
  (db/ensure-conn!)
  (let [packet-id (new-id)
        conversation-id (new-id)
        question (str (or (:content msg) ""))
        seed (preseed-evidence msg question)]
    (build-request {:packet-id packet-id
                    :conversation-id conversation-id
                    :depth 0
                    :msg msg
                    :evidence seed})))

(defn run-slap!
  "Run SLAP loop for a discord message map.
  Returns {:answer \"...\" :resp <full slap response> :depth n}."
  ([msg] (run-slap! msg {}))
  ([msg {:keys [max-depth max-queries]
         :or {max-depth 3 max-queries 8}}]
   (db/ensure-conn!)
   (let [packet-id (new-id)
         conversation-id (new-id)
         question (str (or (:content msg) ""))
         seed (preseed-evidence msg question)]
     (loop [depth 0
            evidence (vec seed)
            queries-used 0]
       (let [req (build-request {:packet-id packet-id
                                 :conversation-id conversation-id
                                 :depth depth
                                 :msg msg
                                 :evidence evidence})
             raw (openai-edn! (slap-system-instructions) (pr-str req))
             resp (-> raw safe-edn-read (validate-response! packet-id))
             ;; normalize query-back to a vector even if model returns nil
             qb (vec (or (:query-back resp) []))
             resp (assoc resp :query-back qb)
             sufficient? (true? (:sufficient? resp))]
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
           (let [new-evidence (run-query-back msg qb)]
             (if (empty? new-evidence)
               {:answer (str (:answer resp)) :resp resp :depth depth}
               (recur (inc depth)
                      (into evidence new-evidence)
                      (+ queries-used (count qb)))))))))))

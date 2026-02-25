(ns pigeonbot.slap
  (:require [clojure.edn :as edn]
            [clojure.string :as str]
            [org.httpkit.client :as http]
            [cheshire.core :as json]
            [pigeonbot.config :as config]
            [pigeonbot.context :as ctx]
            [pigeonbot.db :as db]))

;; -----------------------------------------------------------------------------
;; OpenAI (Responses API) - EDN-only responder
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
                 ;; Responses API accepts an `instructions` string (system-like)
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
      (-> (extract-output-text body)
          str
          str/trim))))

;; -----------------------------------------------------------------------------
;; Datalevin query-back tool
;; -----------------------------------------------------------------------------

(defn- fts-messages
  "Return up to N message maps for an FTS query."
  [q n]
  ;; db/fulltext returns datoms [e a v]; we want to pull the rest.
  (let [hits (take n (db/fulltext q))
        eids (->> hits (map first) distinct vec)
        dbv  (db/db)]
    (->> eids
         (map (fn [e]
                (let [m (db/pull-message dbv e)]
                  ;; keep payload small
                  (select-keys m [:message/id :message/ts :message/channel-id :message/guild-id
                                  :message/author-id :message/author-name :message/bot?
                                  :message/content]))))
         (remove nil?)
         vec)))

;; -----------------------------------------------------------------------------
;; SLAP packet building
;; -----------------------------------------------------------------------------

(defn- now-iso []
  (.toString (java.time.OffsetDateTime/now)))

(defn- new-id [] (str (java.util.UUID/randomUUID)))

(defn- slap-system-instructions
  "System prompt for the SLAP responder."
  []
  ["You are a SLAP responder."
   "You MUST output EDN only. Do not output any prose outside EDN."
   "Your EDN MUST be a map with keys: :slap/version :packet/id :sufficient? :answer :extract :query-back :meta."
   "Use :slap/version \"0.1\"."
   "If you need more info, set :sufficient? false and include :query-back items."
   "For :query-back, only use tool :datalevin/fts for now."
   "Keep :answer concise; do not include code fences; do not include markdown unless asked."
   "Never include secrets in output."])

(defn- build-request
  "Minimal SLAP request packet used for our orchestrator."
  [{:keys [packet-id conversation-id depth msg evidence]}]
  (let [{:keys [channel-id guild-id id content author]} msg
        author-id (some-> (or (:id author) (get-in author [:user :id])) str)
        author-name (or (:global_name author)
                        (:username author)
                        (get-in author [:user :username])
                        "unknown")
        recent (ctx/context-text msg)]
    {:slap/version "0.1"
     :packet/id packet-id
     :conversation/id (or conversation-id packet-id)
     :depth depth
     :identity {:actor {:platform :discord
                        :user/id author-id
                        :user/name author-name}
                :scope {:guild/id (some-> guild-id str)
                        :channel/id (some-> channel-id str)}}
     :temporal {:now (now-iso)
                :recent/messages [{:message/id (some-> id str)
                                   :content (str (or content ""))}]
                :recent/context recent}
     :knowledge {:evidence (vec evidence)}
     :task {:goal "Answer the user's message using available evidence. If insufficient, request query-back."}}))

(defn- safe-edn-read
  "Parse EDN output; throws with context if invalid."
  [s]
  (try
    (edn/read-string {:readers {} :default (fn [_tag v] v)} s)
    (catch Throwable t
      (throw (ex-info "SLAP: model returned non-EDN" {:raw (subs (str s) 0 (min 500 (count (str s))))}
                      t)))))

(defn- validate-response
  [resp packet-id]
  (when-not (and (map? resp)
                 (= "0.1" (:slap/version resp))
                 (= packet-id (:packet/id resp))
                 (contains? resp :sufficient?)
                 (contains? resp :answer)
                 (contains? resp :extract)
                 (contains? resp :query-back)
                 (contains? resp :meta))
    (throw (ex-info "SLAP: invalid response shape" {:resp resp})))
  resp)

(defn- run-query-back
  "Execute :query-back items; returns vector of evidence maps."
  [query-back]
  (->> query-back
       (sort-by (fn [q] (or (:priority q) 999)))
       (mapcat
        (fn [{:keys [tool query expected query/id purpose] :as q}]
          (case tool
            :datalevin/fts
            (let [fts (or (get-in query [:fts]) (get query :fts) "")
                  limit (or (get-in expected [:limit]) 10)
                  rows (fts-messages fts limit)]
              [{:evidence/type :datalevin/fts
                :query/id (or query/id (new-id))
                :purpose (or purpose "")
                :fts fts
                :rows rows}])

            ;; Unknown tool -> ignore for now
            [])))
       vec))

;; -----------------------------------------------------------------------------
;; Orchestrator
;; -----------------------------------------------------------------------------

(defn run-slap!
  "Run SLAP loop for a discord message map.
  Returns {:answer \"...\" :resp <full slap response> :depth n}."
  ([msg] (run-slap! msg {}))
  ([msg {:keys [max-depth max-queries]
         :or {max-depth 3 max-queries 8}}]
   (db/ensure-conn!)
   (let [packet-id (new-id)
         conversation-id (new-id)]
     (loop [depth 0
            evidence []
            queries-used 0]
       (when (> depth max-depth)
         (return {:answer "I can answer, but I hit my evidence recursion limit." :resp nil :depth depth}))

       (let [req (build-request {:packet-id packet-id
                                 :conversation-id conversation-id
                                 :depth depth
                                 :msg msg
                                 :evidence evidence})
             raw (openai-edn! (slap-system-instructions) (pr-str req))
             resp (-> raw safe-edn-read (validate-response packet-id))
             sufficient? (true? (:sufficient? resp))
             qb (vec (:query-back resp))]

         (if (or sufficient? (empty? qb) (>= queries-used max-queries))
           {:answer (str (:answer resp))
            :resp resp
            :depth depth}

           (let [new-evidence (run-query-back qb)
                 ;; stop if no new evidence
                 progressed? (pos? (count new-evidence))]
             (if-not progressed?
               {:answer (str (:answer resp))
                :resp resp
                :depth depth}
               (recur (inc depth)
                      (into evidence new-evidence)
                      (+ queries-used (count qb)))))))))))

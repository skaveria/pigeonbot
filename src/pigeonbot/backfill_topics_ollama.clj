(ns pigeonbot.backfill-topics-ollama
  (:require [clojure.string :as str]
            [cheshire.core :as json]
            [org.httpkit.client :as http]
            [datalevin.core :as dlv]
            [pigeonbot.db :as db]))

(def ^:private default-ollama-url "http://localhost:11434")

(defn- json->map [s]
  (try (json/decode s true) (catch Throwable _ nil)))

(defn- endpoint [base path]
  (str (str/replace (or base "") #"/+$" "") path))

(defn- sleep! [ms]
  (when (pos? (long ms))
    (Thread/sleep (long ms))))

(defn- normalize-topics [xs]
  (->> (or xs [])
       (map (fn [t] (-> (str t) str/lower-case str/trim)))
       (remove str/blank?)
       distinct
       (take 12)
       vec))

(defn- ollama-topics
  "Call Ollama /api/chat and return {:topics [...]}.
  Prompt is strict JSON-only."
  [{:keys [ollama-url model text]
    :or {ollama-url default-ollama-url}}]
  (let [ep (endpoint ollama-url "/api/chat")
        prompt (str
                "Return ONLY valid JSON like: {\"topics\":[\"93r\",\"foregrip\",\"trigger-guard\"]}\n"
                "Rules:\n"
                "- lowercase\n"
                "- 3 to 8 topics\n"
                "- no spaces (use hyphen)\n"
                "- be specific (models, parts, concepts)\n"
                "- no commentary\n\n"
                "TEXT:\n" (str text))
        payload {:model model
                 :stream false
                 :messages [{:role "system" :content "You are a topic tagger. Output JSON only."}
                            {:role "user" :content prompt}]}
        {:keys [status body error]}
        @(http/post ep {:headers {"Content-Type" "application/json"}
                        :body (json/encode payload)
                        :timeout 60000})]
    (cond
      error
      (throw (ex-info "Ollama request failed" {:error (str error)}))

      (not (<= 200 status 299))
      (throw (ex-info "Ollama non-2xx" {:status status :body body}))

      :else
      (let [m (json->map body)
            txt (or (get-in m [:message :content])
                    ;; fallback: some older shapes
                    (get-in m [:response])
                    (str body))
            parsed (or (json->map txt) (json->map (str/trim txt)))]
        (or parsed {:topics []})))))

(defn- topic-exists?
  "True if a topic entity exists for message-id (uses Datalevin directly; no db helper needed)."
  [dbv mid]
  (boolean (ffirst (dlv/q '[:find ?e
                            :in $ ?mid
                            :where
                            [?e :topic/message-id ?mid]]
                          dbv (str mid)))))

(defn- upsert-message-topics!
  "Upsert topics for a message-id into a separate topic entity.

  Uses :topic/message-id as identity (so reruns are idempotent-ish).
  Writes topics as cardinality-many :topic/topic datoms."
  [{:keys [message-id guild-id channel-id model topics]}]
  (let [conn (db/ensure-conn!)
        dbv (dlv/db conn)
        mid (some-> message-id str)
        topics (normalize-topics topics)]
    (when (and (seq mid) (seq topics))
      ;; Find existing topic entity, else use tempid
      (let [eid (or (ffirst (dlv/q '[:find ?e
                                     :in $ ?mid
                                     :where
                                     [?e :topic/message-id ?mid]]
                                   dbv mid))
                    (dlv/tempid :db.part/user))
            ent {:db/id            eid
                 :topic/message-id mid
                 :topic/ts         (java.util.Date.)
                 :topic/guild-id   (some-> guild-id str)
                 :topic/channel-id (some-> channel-id str)
                 :topic/model      (str (or model "ollama"))}
            ;; NOTE: we *add* topics; if you want exact overwrite semantics later,
            ;; we can retract old topics first.
            ops (for [t topics]
                  [:db/add eid :topic/topic t])]
        (dlv/transact! conn (vec (concat [ent] ops)))
        true))))

(defn- message-rows-in-channel
  "Return vector of [mid txt gid cid] for a channel."
  [channel-id]
  (let [dbv (db/db)]
    (->> (dlv/q '[:find ?mid ?txt ?gid ?cid
                  :in $ ?cid
                  :where
                  [?e :message/channel-id ?cid]
                  [?e :message/id ?mid]
                  [?e :message/content ?txt]
                  [?e :message/guild-id ?gid]
                  [?e :message/channel-id ?cid]]
                dbv (str channel-id))
         vec)))

(defn backfill-channel-topics!
  "Backfill per-message topic tags for a channel using Ollama.
  Writes to :topic/* layer, does NOT touch spine.

  Options:
  - :model (required) e.g. \"llama3.2:latest\"
  - :ollama-url default http://localhost:11434
  - :sleep-ms default 100
  - :limit default nil

  Skips messages already tagged (by :topic/message-id existence)."
  [channel-id {:keys [model ollama-url sleep-ms limit]
               :or {sleep-ms 100}}]
  (when-not (and (string? model) (seq model))
    (throw (ex-info "backfill-channel-topics! requires :model" {:model model})))
  (db/ensure-conn!)
  (let [rows (message-rows-in-channel channel-id)
        rows (if limit (take (long limit) rows) rows)]
    (loop [xs rows
           seen 0
           written 0
           skipped 0]
      (if (empty? xs)
        {:channel-id (str channel-id) :seen seen :written written :skipped skipped}
        (let [[mid txt gid cid] (first xs)
              mid (str mid)
              dbv (db/db)]
          (if (topic-exists? dbv mid)
            (recur (rest xs) (inc seen) written (inc skipped))
            (let [res (ollama-topics {:ollama-url (or ollama-url default-ollama-url)
                                      :model model
                                      :text (str txt)})
                  topics (normalize-topics (:topics res))]
              (when (seq topics)
                (upsert-message-topics!
                 {:message-id mid
                  :guild-id gid
                  :channel-id cid
                  :model (str "ollama:" model)
                  :topics topics}))
              (sleep! sleep-ms)
              (recur (rest xs)
                     (inc seen)
                     (if (seq topics) (inc written) written)
                     skipped))))))))

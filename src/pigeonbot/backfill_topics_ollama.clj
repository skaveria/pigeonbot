(ns pigeonbot.backfill-topics-ollama
  (:require [clojure.string :as str]
            [cheshire.core :as json]
            [org.httpkit.client :as http]
            [pigeonbot.db :as db]))

(def ^:private default-ollama-url
  "http://localhost:11434")

(defn- endpoint [base path]
  (str (str/replace (or base "") #"/+$" "") path))

(defn- sleep! [ms]
  (when (pos? (long ms))
    (Thread/sleep (long ms))))

(defn- json->map [s]
  (try
    (json/decode s true)
    (catch Throwable _
      nil)))

(defn- extract-json-object [s]
  (when-let [m (re-find #"(?s)\{.*\}" (str s))]
    (json->map m)))

(defn- normalize-topics [xs]
  (->> (or xs [])
       (map #(-> (str %) str/lower-case str/trim))
       (remove str/blank?)
       distinct
       (take 12)
       vec))

(defn- ollama-topics
  [{:keys [ollama-url model text]
    :or {ollama-url default-ollama-url}}]
  (let [ep (endpoint ollama-url "/api/chat")
        payload {:model model
                 :stream false
                 :messages
                 [{:role "system"
                   :content "You are a topic tagger. Return JSON only."}
                  {:role "user"
                   :content
                   (str
                    "Return ONLY valid JSON like:\n"
                    "{\"topics\":[\"clojure\",\"datalevin\",\"discord-bot\"]}\n\n"
                    "Rules:\n"
                    "- lowercase\n"
                    "- 3 to 8 topics\n"
                    "- no spaces, use hyphen\n"
                    "- concrete nouns, model names, projects, parts, systems\n"
                    "- no prose, no markdown\n\n"
                    "TEXT:\n"
                    (str text))}]}
        {:keys [status body error]}
        @(http/post ep
                    {:headers {"Content-Type" "application/json"}
                     :body (json/encode payload)
                     :timeout 60000})]
    (cond
      error
      (throw (ex-info "Ollama request failed"
                      {:error (str error)
                       :endpoint ep
                       :model model}))

      (not (<= 200 status 299))
      (throw (ex-info "Ollama non-2xx"
                      {:status status
                       :body body
                       :endpoint ep
                       :model model}))

      :else
      (let [m (json->map body)
            raw (or (get-in m [:message :content])
                    (:response m)
                    body)
            parsed (or (json->map raw)
                       (extract-json-object raw)
                       {:topics []})]
        {:topics (normalize-topics (:topics parsed))
         :raw raw}))))

(defn- message-rows-in-channel
  [channel-id]
  (db/messages-for-channel (str channel-id)))

(defn backfill-channel-topics!
  "Backfill per-message topic tags for a channel using Ollama.

  SQLite-safe and idempotent:
  - reads messages via db/messages-for-channel
  - skips messages already present in topics
  - never recurs across try/catch"
  [channel-id {:keys [model ollama-url sleep-ms limit]
               :or {sleep-ms 100}}]
  (when-not (and (string? model) (seq model))
    (throw (ex-info "backfill-channel-topics! requires :model" {:model model})))
  (db/ensure-conn!)
  (let [rows0 (db/messages-for-channel channel-id)
        rows  (cond->> rows0
                limit (take (long limit)))]
    (loop [xs (seq rows)
           seen 0
           written 0
           skipped 0
           errors 0]
      (if-not xs
        {:channel-id (str channel-id)
         :seen seen
         :written written
         :skipped skipped
         :errors errors}
        (let [m (first xs)
              mid (:message/id m)
              txt (:message/content m)
              gid (:message/guild-id m)
              cid (:message/channel-id m)
              result
              (try
                (if (db/topic-exists? mid)
                  {:status :skipped}
                  (let [res (ollama-topics {:ollama-url (or ollama-url default-ollama-url)
                                            :model model
                                            :text (str txt)})
                        topics (normalize-topics (:topics res))]
                    (if (seq topics)
                      (do
                        (db/upsert-message-topics!
                         {:message-id mid
                          :guild-id gid
                          :channel-id cid
                          :model (str "ollama:" model)
                          :topics topics})
                        {:status :written})
                      {:status :skipped})))
                (catch Throwable t
                  (println "topic backfill error:"
                           {:channel-id (str channel-id)
                            :message-id (str mid)
                            :error (.getMessage t)
                            :data (ex-data t)})
                  {:status :error}))]
          (when (and (not= (:status result) :skipped)
                     (pos? (long sleep-ms)))
            (Thread/sleep (long sleep-ms)))
          (recur (next xs)
                 (inc seen)
                 (if (= (:status result) :written) (inc written) written)
                 (if (= (:status result) :skipped) (inc skipped) skipped)
                 (if (= (:status result) :error) (inc errors) errors)))))))

(defn backfill-guild-topics!
  "Backfill missing topics for every channel in a guild."
  [guild-id {:keys [model ollama-url sleep-ms limit-per-channel]
             :or {sleep-ms 100}}]
  (db/ensure-conn!)
  (let [channels (db/guild-channel-ids (str guild-id))]
    (println "topic backfill channels:" (count channels))
    (reduce
     (fn [acc cid]
       (println "\n=== topics for channel" cid "===")
       (let [res (backfill-channel-topics!
                  cid
                  {:model model
                   :ollama-url ollama-url
                   :sleep-ms sleep-ms
                   :limit limit-per-channel})]
         (println res)
         (conj acc res)))
     []
     channels)))

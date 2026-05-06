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
  "Backfill missing per-message topic tags for one channel.

  Skips messages that already have topics, so this is safe to rerun."
  [channel-id {:keys [model ollama-url sleep-ms limit]
               :or {sleep-ms 100}}]
  (when-not (and (string? model) (seq model))
    (throw (ex-info "backfill-channel-topics! requires :model"
                    {:model model})))

  (db/ensure-conn!)

  (let [rows0 (message-rows-in-channel channel-id)
        rows (cond->> rows0
               limit (take (long limit)))]
    (loop [xs rows
           seen 0
           written 0
           skipped 0
           failed 0]
      (if (empty? xs)
        {:channel-id (str channel-id)
         :seen seen
         :written written
         :skipped skipped
         :failed failed}

        (let [{:keys [message/id
                      message/content
                      message/guild-id
                      message/channel-id]} (first xs)
              mid (str id)]
          (if (db/topic-exists? mid)
            (recur (rest xs)
                   (inc seen)
                   written
                   (inc skipped)
                   failed)

            (try
              (let [{:keys [topics]} (ollama-topics
                                      {:ollama-url (or ollama-url default-ollama-url)
                                       :model model
                                       :text content})]
                (when (seq topics)
                  (db/upsert-message-topics!
                   {:message-id mid
                    :guild-id guild-id
                    :channel-id channel-id
                    :model (str "ollama:" model)
                    :topics topics}))
                (sleep! sleep-ms)
                (recur (rest xs)
                       (inc seen)
                       (if (seq topics) (inc written) written)
                       skipped
                       failed))

              (catch Throwable t
                (println "[topics] failed message"
                         mid
                         "|"
                         (.getMessage t)
                         (or (ex-data t) {}))
                (sleep! sleep-ms)
                (recur (rest xs)
                       (inc seen)
                       written
                       skipped
                       (inc failed))))))))))

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

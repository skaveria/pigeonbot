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
      error (throw (ex-info "Ollama request failed" {:error (str error)}))
      (not (<= 200 status 299)) (throw (ex-info "Ollama non-2xx" {:status status :body body}))
      :else
      (let [m (json->map body)
            txt (or (get-in m [:message :content])
                    (get-in m [:messages 0 :content])
                    (get-in m [:response])
                    ;; ollama typically uses {:message {:content ...}}
                    (get-in m [:message :content])
                    ;; fallback: whole body
                    (str body))
            parsed (or (json->map txt) (json->map (str/trim txt)))]
        (or parsed {:topics []})))))

(defn- message-ids-in-channel
  "Return vector of message ids for a channel (newest first)."
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
         ;; rows are sets; keep as vector
         vec)))

(defn backfill-channel-topics!
  "Backfill per-message topic tags for a channel using Ollama.
  Writes to :topic/* layer, does NOT touch spine.

  Options:
  - :model (required)
  - :ollama-url default http://localhost:11434
  - :sleep-ms default 100
  - :limit default nil

  Skips messages already tagged."
  [channel-id {:keys [model ollama-url sleep-ms limit]
               :or {sleep-ms 100}}]
  (when-not (and (string? model) (seq model))
    (throw (ex-info "backfill-channel-topics! requires :model" {})))
  (db/ensure-conn!)
  (let [rows (message-ids-in-channel channel-id)
        rows (if limit (take (long limit) rows) rows)]
    (loop [xs rows
           n 0
           written 0
           skipped 0]
      (if (empty? xs)
        {:channel-id (str channel-id) :seen n :written written :skipped skipped}
        (let [[mid txt gid cid] (first xs)
              mid (str mid)]
          (if (db/topic-exists? mid)
            (recur (rest xs) (inc n) written (inc skipped))
            (let [res (ollama-topics {:ollama-url ollama-url
                                      :model model
                                      :text (str txt)})
                  topics (vec (or (:topics res) []))]
              (when (seq topics)
                (db/upsert-message-topics!
                 {:message-id mid
                  :guild-id gid
                  :channel-id cid
                  :model (str "ollama:" model)
                  :topics topics}))
              (when (pos? (long sleep-ms)) (Thread/sleep (long sleep-ms)))
              (recur (rest xs)
                     (inc n)
                     (if (seq topics) (inc written) written)
                     skipped))))))))

(ns pigeonbot.backfill-extracts
  (:require [clojure.edn :as edn]
            [clojure.string :as str]
            [org.httpkit.client :as http]
            [cheshire.core :as json]
            [datalevin.core :as dlv]
            [pigeonbot.config :as config]
            [pigeonbot.db :as db]))

(defn- openai-cfg []
  (let [m (config/load-config)]
    {:base-url (or (:openai-base-url m) "https://api.openai.com")
     :api-key  (or (:openai-api-key m) (System/getenv "OPENAI_API_KEY"))
     :model    (or (:openai-model m) "gpt-4.1-mini")
     :timeout  (long (or (:brain-timeout-ms m) 60000))}))

(defn- endpoint [base path]
  (str (str/replace (or base "") #"/+$" "") path))

(defn- extract-output-text [resp-body]
  (let [m (cond
            (string? resp-body) (json/decode resp-body true)
            (map? resp-body) resp-body
            :else {})]
    (or (:output_text m)
        (get-in m [:choices 0 :message :content])
        (get-in m [:error :message])
        (str resp-body))))

(defn- openai-edn! [instructions user-text]
  (let [{:keys [base-url api-key model timeout]} (openai-cfg)
        _ (when-not (and (string? api-key) (seq api-key))
            (throw (ex-info "Missing OpenAI API key" {})))
        ep (endpoint base-url "/v1/responses")
        payload {:model model
                 :instructions instructions
                 :input (str user-text)}
        headers {"Authorization" (str "Bearer " api-key)
                 "Content-Type" "application/json"}
        {:keys [status body error]}
        @(http/post ep {:headers headers
                        :body (json/encode payload)
                        :timeout timeout})]
    (cond
      error (throw (ex-info "OpenAI request failed" {:error (str error)}))
      (not (<= 200 status 299)) (throw (ex-info "OpenAI non-2xx" {:status status :body body}))
      :else (-> (extract-output-text body) str str/trim))))

(defn- safe-edn [s]
  (edn/read-string {:readers {} :default (fn [_ v] v)} s))

(defn- chunk-transcript
  "Turn a chunk of message tuples into a compact transcript."
  [rows]
  (->> rows
       (map (fn [[_ts author txt mid]]
              (let [txt (-> (or txt "") (str/replace #"\s+" " ") str/trim)]
                (str author " (" mid "): " txt))))
       (remove str/blank?)
       (str/join "\n")))

(defn- instructions []
  (str/join "\n"
            ["You are generating memory extracts from a Discord chat transcript."
             "Return EDN only."
             "Output a single EDN map: {:extract [...]}"
             ":extract must be a vector of maps."
             "Each extract map MUST include: :text :kind :confidence :tags"
             "Rules for :tags:"
             "- vector of 3-8 strings"
             "- lowercase, no spaces (use hyphen)"
             "- prefer concrete nouns/models"
             "Keep :text concise (one sentence)."
             "Do not include markdown."]))

(defn- fetch-channel-messages
  "Return vector of [ts author content message-id] for a channel, sorted by ts."
  [channel-id]
  (let [dbv (db/db)]
    (->> (dlv/q '[:find ?ts ?author ?txt ?mid
                  :in $ ?cid
                  :where
                  [?e :message/channel-id ?cid]
                  [?e :message/ts ?ts]
                  [?e :message/author-name ?author]
                  [?e :message/content ?txt]
                  [?e :message/id ?mid]]
                dbv (str channel-id))
         (sort-by first)
         vec)))

(defn backfill-channel-extracts!
  "Backfill extracts for a single channel by chunking messages and calling OpenAI.

  Options:
  - :chunk-size (default 30)
  - :sleep-ms (default 300)
  - :max-chunks (default nil) for testing

  Returns {:channel-id .. :chunks .. :extract-writes ..}"
  [guild-id channel-id & {:keys [chunk-size sleep-ms max-chunks]
                          :or {chunk-size 30 sleep-ms 300}}]
  (db/ensure-conn!)
  (let [rows (fetch-channel-messages channel-id)
        chunks (partition-all (long chunk-size) rows)]
    (loop [i 0
           chunks-left chunks
           writes 0]
      (cond
        (empty? chunks-left)
        {:channel-id (str channel-id) :chunks i :extract-writes writes}

        (and max-chunks (>= i (long max-chunks)))
        {:channel-id (str channel-id) :chunks i :extract-writes writes}

        :else
        (let [chunk (first chunks-left)
              transcript (chunk-transcript chunk)
              packet {:channel-id (str channel-id)
                      :guild-id (str guild-id)
                      :transcript transcript}
              raw (openai-edn! (instructions) (pr-str packet))
              m (safe-edn raw)
              extracts (vec (or (:extract m) []))
              ctx {:guild-id (str guild-id)
                   :channel-id (str channel-id)
                   :message-id (str "backfill-chunk-" i)
                   :packet-id (str (java.util.UUID/randomUUID))
                   :model (str (:model (openai-cfg)))}
              ok (when (seq extracts)
                   (db/upsert-extracts! ctx extracts))]
          (when (pos? (long sleep-ms)) (Thread/sleep (long sleep-ms)))
          (recur (inc i)
                 (rest chunks-left)
                 (+ writes (if ok 1 0))))))))

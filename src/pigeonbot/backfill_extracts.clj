(ns pigeonbot.backfill-extracts
  (:require [clojure.edn :as edn]
            [clojure.string :as str]
            [org.httpkit.client :as http]
            [cheshire.core :as json]
            [pigeonbot.config :as config]
            [pigeonbot.db :as db]))

(defn- openai-cfg []
  (let [m (config/load-config)]
    {:base-url (or (:openai-base-url m) "https://api.openai.com")
     :api-key  (or (:openai-api-key m) (System/getenv "OPENAI_API_KEY"))
     :model    (or (:openai-model m) "gpt-5.5")
     :timeout  (long (or (:brain-timeout-ms m) 120000))
     :max-output-tokens (long (or (:openai-max-output-tokens m) 1200))}))

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
     (:output_text m)
     (let [out (or (:output m) [])
           texts (->> out
                      (mapcat
                       (fn [item]
                         (let [content (:content item)]
                           (cond
                             (sequential? content)
                             (keep #(or (:text %) (:output_text %)) content)

                             (string? content)
                             [content]

                             :else
                             []))))
                      (map str)
                      (remove str/blank?)
                      vec)]
       (when (seq texts)
         (str/join "" texts)))
     (get-in m [:choices 0 :message :content])
     (get-in m [:error :message])
     (str resp-body))))

(defn- openai-edn! [instructions user-text]
  (let [{:keys [base-url api-key model timeout max-output-tokens]} (openai-cfg)]
    (when-not (and (string? api-key) (seq api-key))
      (throw (ex-info "Missing OpenAI API key" {})))
    (let [ep (endpoint base-url "/v1/responses")
          payload {:model model
                   :instructions instructions
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
        (throw (ex-info "OpenAI request failed"
                        {:error (str error) :endpoint ep}))

        (or (nil? status) (not (<= 200 status 299)))
        (throw (ex-info "OpenAI non-2xx"
                        {:status status :body body :endpoint ep}))

        :else
        (-> (extract-output-text body) str str/trim)))))

(defn- strip-codefences [s]
  (-> (str (or s ""))
      (str/replace #"(?s)^\s*```[a-zA-Z0-9_-]*\s*" "")
      (str/replace #"(?s)\s*```\s*$" "")
      str/trim))

(defn- read-edn-or-nil [s]
  (try
    (edn/read-string {:readers {} :default (fn [_ v] v)} s)
    (catch Throwable _
      nil)))

(defn- balanced-map-snippets [s]
  (let [s (str s)]
    (loop [i 0
           start nil
           depth 0
           out []]
      (if (>= i (count s))
        out
        (let [ch (.charAt s i)]
          (cond
            (= ch \{)
            (recur (inc i)
                   (if (nil? start) i start)
                   (inc depth)
                   out)

            (= ch \})
            (let [depth' (dec depth)]
              (if (and start (zero? depth'))
                (recur (inc i)
                       nil
                       0
                       (conj out (subs s start (inc i))))
                (recur (inc i) start depth' out)))

            :else
            (recur (inc i) start depth out)))))))

(defn- safe-edn [raw]
  (let [s (strip-codefences raw)]
    (or
     (read-edn-or-nil s)
     (some read-edn-or-nil (reverse (balanced-map-snippets s)))
     {:extract [] :raw raw :parse-failed? true})))

(defn- normalize-tags [xs]
  (->> (or xs [])
       (map #(-> (str %) str/lower-case str/trim))
       (remove str/blank?)
       distinct
       (take 8)
       vec))

(defn- normalize-extract [x]
  (cond
    (string? x)
    (let [txt (str/trim x)]
      (when (seq txt)
        {:text txt
         :kind :note
         :confidence 0.65
         :tags []}))

    (map? x)
    (let [txt (some-> (or (:text x) (:content x) (:extract/text x)) str str/trim)]
      (when (seq txt)
        {:text txt
         :kind (or (:kind x) (:extract/kind x) :note)
         :confidence (double (or (:confidence x) (:extract/confidence x) 0.75))
         :tags (normalize-tags (or (:tags x) (:extract/tags x)))}))

    :else
    nil))

(defn- normalize-extracts [xs]
  (->> (or xs [])
       (map normalize-extract)
       (remove nil?)
       vec))

(defn- chunk-transcript [rows]
  (->> rows
       (map (fn [{:keys [message/id message/author-name message/content]}]
              (let [txt (-> (or content "")
                            (str/replace #"\s+" " ")
                            str/trim)]
                (str author-name " (" id "): " txt))))
       (remove str/blank?)
       (str/join "\n")))

(defn- instructions []
  (str/join
   "\n"
   ["You generate long-term memory extracts from Discord chat transcripts."
    ""
    "Return EDN only if you can."
    "Preferred shape: {:extract [...]}"
    ""
    "Each extract item should be:"
    "{:text \"one concise durable memory\" :kind :note :confidence 0.8 :tags [\"tag\" \"tag\"]}"
    ""
    "Rules:"
    "- Extract durable facts, preferences, decisions, project state, recurring ideas, and useful context."
    "- Ignore throwaway chatter unless it explains an ongoing project or preference."
    "- :text should be one sentence."
    "- :kind should be :note, :fact, :decision, :todo, :preference, or :project."
    "- :confidence should be 0.0 to 1.0."
    "- :tags should be lowercase strings, no spaces; use hyphen."
    "- If nothing useful exists, return {:extract []}."
    ""
    "No markdown. No prose outside the EDN map."]))


(defn backfill-channel-extracts!
  "Backfill extracts for a single channel by chunking messages and calling OpenAI.

  SQLite-safe:
  - reads messages via db/messages-for-channel
  - chunks locally
  - never recurs across try/catch"
  [guild-id channel-id & {:keys [chunk-size sleep-ms max-chunks debug?]
                          :or {chunk-size 30
                               sleep-ms 300
                               debug? false}}]
  (db/ensure-conn!)
  (let [messages (db/messages-for-channel channel-id)
        rows (->> messages
                  (map (fn [m]
                         [(:message/ts m)
                          (:message/author-name m)
                          (:message/content m)
                          (:message/id m)]))
                  vec)
        chunks0 (partition-all (long chunk-size) rows)
        chunks  (cond->> chunks0
                  max-chunks (take (long max-chunks)))]
    (loop [xs (seq chunks)
           i 0
           writes 0
           errors 0]
      (if-not xs
        {:channel-id (str channel-id)
         :chunks i
         :extract-writes writes
         :errors errors}
        (let [chunk (first xs)
              result
              (try
                (let [transcript (chunk-transcript chunk)
                      packet {:channel-id (str channel-id)
                              :guild-id (str guild-id)
                              :transcript transcript}
                      raw (openai-edn! (instructions) (pr-str packet))
                      m (try
                          (safe-edn raw)
                          (catch Throwable t
                            (when debug?
                              (println "\n--- backfill-channel-extracts! EDN PARSE FAILED ---")
                              (println (subs (str raw) 0 (min 2000 (count (str raw)))))
                              (println "--- /EDN PARSE FAILED ---\n"))
                            (throw t)))
                      extracts (vec (or (:extract m) []))
                      ctx {:guild-id (str guild-id)
                           :channel-id (str channel-id)
                           :message-id (str "backfill-chunk-" i)
                           :packet-id (str (java.util.UUID/randomUUID))
                           :model (str (:model (openai-cfg)))}
                      ok (when (seq extracts)
                           (db/upsert-extracts! ctx extracts))]
                  {:status (if ok :written :skipped)})
                (catch Throwable t
                  (println "extract backfill error:"
                           {:channel-id (str channel-id)
                            :chunk i
                            :error (.getMessage t)
                            :data (ex-data t)})
                  {:status :error}))]
          (when (pos? (long sleep-ms))
            (Thread/sleep (long sleep-ms)))
          (recur (next xs)
                 (inc i)
                 (if (= (:status result) :written) (inc writes) writes)
                 (if (= (:status result) :error) (inc errors) errors)))))))

(defn backfill-guild-extracts!
  "Build extract memories for every channel in a guild."
  [guild-id & {:keys [chunk-size sleep-ms max-chunks-per-channel debug?]
               :or {chunk-size 40
                    sleep-ms 150
                    debug? false}}]
  (db/ensure-conn!)
  (let [channels (db/guild-channel-ids (str guild-id))]
    (println "extract backfill channels:" (count channels))
    (reduce
     (fn [acc cid]
       (println "\n=== extracts for channel" cid "===")
       (let [res (backfill-channel-extracts!
                  guild-id
                  cid
                  :chunk-size chunk-size
                  :sleep-ms sleep-ms
                  :max-chunks max-chunks-per-channel
                  :debug? debug?)]
         (println res)
         (conj acc res)))
     []
     channels)))

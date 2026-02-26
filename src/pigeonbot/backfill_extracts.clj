(ns pigeonbot.backfill-extracts
  (:require [clojure.edn :as edn]
            [clojure.string :as str]
            [org.httpkit.client :as http]
            [cheshire.core :as json]
            [datalevin.core :as dlv]
            [pigeonbot.config :as config]
            [pigeonbot.db :as db]))

;; -----------------------------------------------------------------------------
;; OpenAI (Responses API)
;; -----------------------------------------------------------------------------

(defn- openai-cfg []
  (let [m (config/load-config)]
    {:base-url (or (:openai-base-url m) "https://api.openai.com")
     :api-key  (or (:openai-api-key m) (System/getenv "OPENAI_API_KEY"))
     :model    (or (:openai-model m) "gpt-4.1-mini")
     :timeout  (long (or (:brain-timeout-ms m) 60000))
     :max-output-tokens (or (:openai-max-output-tokens m) 900)
     :temperature (if (number? (:openai-temperature m)) (:openai-temperature m) 0)}))

(defn- endpoint [base path]
  (str (str/replace (or base "") #"/+$" "") path))

(defn- decode-body [body]
  (cond
    (nil? body) {}
    (map? body) body
    (string? body) (try (json/decode body true) (catch Throwable _ {}))
    :else {}))

(defn- extract-output-text
  "Extract assistant text from OpenAI Responses API result."
  [resp-body]
  (let [m (decode-body resp-body)]
    (or
      ;; common convenience field
      (when (string? (:output_text m)) (:output_text m))

      ;; general output traversal
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

      ;; error message fallback
      (get-in m [:error :message])

      ;; last resort
      (str resp-body))))

(defn- openai-edn!
  "Call OpenAI Responses API; expects EDN-only output (enforced by instructions)."
  [instructions user-text]
  (let [{:keys [base-url api-key model timeout max-output-tokens temperature]} (openai-cfg)
        _ (when-not (and (string? api-key) (seq api-key))
            (throw (ex-info "Missing OpenAI API key" {})))
        ep (endpoint base-url "/v1/responses")
        payload {:model model
                 :instructions (str instructions)
                 :input (str user-text)
                 :temperature (double temperature)
                 :max_output_tokens (long max-output-tokens)}
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

;; -----------------------------------------------------------------------------
;; EDN parsing (robust)
;; -----------------------------------------------------------------------------

(defn- strip-codefences
  "Remove ``` fences if the model wraps output."
  [s]
  (let [s (str (or s ""))]
    (-> s
        (str/replace #"(?s)^\s*```[a-zA-Z0-9_-]*\s*" "")
        (str/replace #"(?s)\s*```\s*$" "")
        (str/trim))))

(defn- try-read-edn
  "Try reading EDN. Returns value or nil."
  [s]
  (try
    (edn/read-string {:readers {} :default (fn [_ v] v)} s)
    (catch Throwable _ nil)))

(defn- best-edn-map-snippet
  "Heuristic: find a plausible EDN map from arbitrary text.
  Strategy:
  - collect all substrings that look like {...} (greedy-ish)
  - prefer the longest one (usually the full map)
  - fallback to last one"
  [s]
  (let [s (str (or s ""))]
    (when-let [ms (seq (re-seq #"(?s)\{.*\}" s))]
      (let [sorted (sort-by count ms)]
        (or (last sorted) (last ms))))))

(defn- safe-edn
  "Parse EDN map from model output. Throws with useful debug data on failure."
  [raw]
  (let [raw0 (str (or raw ""))
        s (strip-codefences raw0)]
    (or
      (try-read-edn s)
      (when-let [m (best-edn-map-snippet s)]
        (try-read-edn m))
      (throw (ex-info "Backfill extracts: model returned non-EDN"
                      {:raw (subs raw0 0 (min 1200 (count raw0)))
                       :hint "Expected a single EDN map like {:extract [...]}. Check instructions / model output."})))))

;; -----------------------------------------------------------------------------
;; Backfill logic
;; -----------------------------------------------------------------------------

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
  (str/join
   "\n"
   ["You are generating memory extracts from a Discord chat transcript."
    ""
    "HARD RULES:"
    "- Output EDN ONLY."
    "- Output EXACTLY ONE EDN MAP and nothing else."
    "- Do NOT wrap in markdown or code fences."
    ""
    "Required shape:"
    "{:extract [ ... ]}"
    ""
    "Each extract item MUST be a map with keys:"
    "  :text :kind :confidence :tags"
    ""
    "Constraints:"
    "- :text is ONE concise sentence."
    "- :kind is a keyword (e.g. :note :fact :decision :todo)."
    "- :confidence is a number 0.0 to 1.0."
    "- :tags is a vector of 3 to 8 strings."
    "- tags are lowercase, no spaces (use hyphen), prefer concrete nouns/models."
    ""
    "If you cannot produce any good extracts, return {:extract []}."
    ""
    "Now read the transcript and emit EDN."]))

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

  Args:
    guild-id, channel-id

  Options (keyword args):
    :chunk-size (default 30)
    :sleep-ms   (default 300)
    :max-chunks (default nil)   ;; for testing
    :debug?     (default false) ;; prints raw model output on parse failures

  Returns:
    {:channel-id .. :chunks .. :extract-writes ..}"
  [guild-id channel-id & {:keys [chunk-size sleep-ms max-chunks debug?]
                          :or {chunk-size 30 sleep-ms 300 debug? false}}]
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
          (when (pos? (long sleep-ms)) (Thread/sleep (long sleep-ms)))
          (recur (inc i)
                 (rest chunks-left)
                 (+ writes (if ok 1 0))))))))

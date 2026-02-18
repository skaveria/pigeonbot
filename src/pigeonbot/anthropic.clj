(ns pigeonbot.anthropic
  (:require [org.httpkit.client :as http]
            [cheshire.core :as json]
            [pigeonbot.config :as config]))

(def ^:private api-base "https://api.anthropic.com/v1")
(def ^:private api-version "2023-06-01")
(def ^:private default-model "claude-opus-4-6")
(def ^:private default-max-tokens 2048)

(defn- api-key []
  (or (System/getenv "ANTHROPIC_API_KEY")
      (:anthropic-api-key (config/load-config))
      (throw (ex-info "Missing ANTHROPIC_API_KEY env var or :anthropic-api-key in config.edn" {}))))

(defn- timeout-ms []
  (long (or (:brain-timeout-ms (config/load-config)) 90000)))

(defn call!
  "Call the Anthropic Messages API with structured JSON output.

  Arguments:
    system-prompt  — string (SLAP layers 1-4)
    user-message   — string (SLAP layers 5-6)
    json-schema    — Clojure map (string keys) for the response structure
    opts           — optional map: :model :max-tokens

  Returns: parsed Clojure map (string keys) matching json-schema."
  ([system-prompt user-message json-schema]
   (call! system-prompt user-message json-schema {}))
  ([system-prompt user-message json-schema
    {:keys [model max-tokens]
     :or   {model      default-model
            max-tokens default-max-tokens}}]
   (let [payload {:model        model
                  :max_tokens   max-tokens
                  :system       system-prompt
                  :messages     [{:role "user" :content user-message}]
                  :output_config {:format {:type   "json_schema"
                                           :schema json-schema}}}
         headers {"x-api-key"         (api-key)
                  "anthropic-version" api-version
                  "content-type"      "application/json"}
         {:keys [status body error]}
         @(http/post (str api-base "/messages")
                     {:headers headers
                      :body    (json/encode payload)
                      :timeout (timeout-ms)})]
     (cond
       error
       (throw (ex-info "Anthropic HTTP error" {:error (str error)}))

       (not (<= 200 status 299))
       (throw (ex-info "Anthropic non-2xx response"
                       {:status status :body (str body)}))

       :else
       (let [resp        (json/decode body)
             content-str (get-in resp ["content" 0 "text"])
             stop-reason (get resp "stop_reason")]
         (when (= stop-reason "refusal")
           (throw (ex-info "Anthropic refused the request" {:stop_reason stop-reason})))
         (when (= stop-reason "max_tokens")
           (println "[anthropic] WARNING: response truncated at max_tokens"))
         (json/decode content-str))))))

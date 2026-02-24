(ns pigeonbot.brains.openai
  (:require [clojure.string :as str]
            [org.httpkit.client :as http]
            [cheshire.core :as json]
            [pigeonbot.config :as config]))

;; Uses OpenAI's Responses API (POST /v1/responses).  [oai_citation:0‡OpenAI Platform](https://platform.openai.com/docs/api-reference/responses)

(defn- cfg []
  (let [m (config/load-config)]
    {:base-url (or (:openai-base-url m) "https://api.openai.com")
     ;; Prefer env var; allow config for local dev (but don't commit secrets).
     :api-key  (or (:openai-api-key m) (System/getenv "OPENAI_API_KEY"))
     ;; Let you override per your preference.
     :model    (or (:openai-model m) "gpt-4.1-mini")
     :timeout  (long (or (:brain-timeout-ms m) 60000))
     ;; Optional knobs
     :temperature (:openai-temperature m)         ;; number 0..2
     :max-output-tokens (:openai-max-output-tokens m) ;; integer
     :instructions (:openai-instructions m)}))    ;; system-ish string

(defn- endpoint [base path]
  (str (str/replace (or base "") #"/+$" "") path))

(defn- decode-body [body]
  (cond
    (nil? body) {}
    (map? body) body
    (string? body) (json/decode body true)
    :else {}))

(defn- extract-output-text
  "Extract concatenated assistant text from a Responses API REST response.

  Note: The docs mention `output_text` as an SDK convenience, so for raw REST
  we primarily parse the `output` array and `content` items.  [oai_citation:1‡OpenAI Platform](https://platform.openai.com/docs/api-reference/responses)"
  [resp-body]
  (let [m (decode-body resp-body)]
    (or
      ;; Sometimes proxies/SDKs include this field; accept if present.
      (when (string? (:output_text m)) (:output_text m))

      ;; Standard REST shape: output -> message items -> content -> output_text
      (let [out (or (:output m) [])
            texts
            (->> out
                 (mapcat (fn [item]
                           (let [content (:content item)]
                             (cond
                               ;; Most common: vector of content parts
                               (sequential? content)
                               (->> content
                                    (keep (fn [c]
                                            (cond
                                              ;; Typical: {:type "output_text" :text "..."}
                                              (string? (:text c)) (:text c)
                                              ;; Be tolerant
                                              (string? (:output_text c)) (:output_text c)
                                              :else nil))))
                               ;; Rare fallback
                               (string? content) [content]
                               :else []))))
                 (map str)
                 (remove str/blank?)
                 vec)]
        (when (seq texts) (str/join "" texts)))

      ;; Error fallbacks
      (get-in m [:error :message])
      (str resp-body))))

(defn ask-with-context
  "Call OpenAI Responses API with a simple text input.

  Matches pigeonbot.brain contract: returns assistant text."
  [context-text question]
  (let [{:keys [base-url api-key model timeout temperature max-output-tokens instructions]} (cfg)
        _ (when-not (and (string? api-key) (seq api-key))
            (throw (ex-info "Missing OpenAI API key (set OPENAI_API_KEY or :openai-api-key in config.edn)" {})))

        ctx (str/trim (or context-text ""))
        q   (str/trim (str question))
        user (if (seq ctx)
               (str "Recent chat:\n" ctx "\n\nNow respond to this message:\n" q)
               q)

        ep (endpoint base-url "/v1/responses")

        payload (cond-> {:model model
                         :input user}
                  (string? instructions) (assoc :instructions instructions)
                  (number? temperature) (assoc :temperature temperature)
                  (integer? max-output-tokens) (assoc :max_output_tokens max-output-tokens))

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
      (extract-output-text body))))

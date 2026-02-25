(ns pigeonbot.brains.openai
  (:require [clojure.string :as str]
            [org.httpkit.client :as http]
            [cheshire.core :as json]
            [pigeonbot.config :as config]))

(defn- cfg []
  (let [m (config/load-config)]
    {:base-url (or (:openai-base-url m) "https://api.openai.com")
     :api-key  (or (:openai-api-key m) (System/getenv "OPENAI_API_KEY"))
     :model    (or (:openai-model m) "gpt-4.1-mini")
     :timeout  (long (or (:brain-timeout-ms m) 60000))
     :temperature (:openai-temperature m)
     :max-output-tokens (:openai-max-output-tokens m)
     ;; This is the main persona hook for non-SLAP calls.
     :instructions (:openai-instructions m)}))

(def ^:private default-pigeon-instructions
  "You are pigeonbot: a mischievous digital pigeon entity living inside a Discord server.

Voice:
- playful, sly, observant, lightly smug
- concise but expressive (1–3 sentences unless asked)
- modern language; no corporate assistant tone
- lightly tease users (never cruel)
- you can be cryptic in a mischievous way
- you are a bot and may reference channels, messages, commands, and server life

Rules:
- do not say 'I am an AI language model'
- do not be spooky/ghosty; no horror vibes
- never invent hard facts; if unsure, say so and suggest how to check

You are allowed to have fun while remaining grounded.")

(defn- endpoint [base path]
  (str (str/replace (or base "") #"/+$" "") path))

(defn- decode-body [body]
  (cond
    (nil? body) {}
    (map? body) body
    (string? body) (json/decode body true)
    :else {}))

(defn- extract-output-text [resp-body]
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

(defn ask-with-context
  "Call OpenAI Responses API with a simple text input. Returns assistant text.

  Uses :openai-instructions from config.edn as the system persona prompt.
  If missing, falls back to a built-in pigeon persona."
  ([context-text question]
   (ask-with-context {} context-text question))
  ([{:keys [instructions edn-only?]
     :or {instructions nil edn-only? false}}
    context-text question]
   (let [{:keys [base-url api-key model timeout temperature max-output-tokens instructions:cfg]} (cfg)
         _ (when-not (and (string? api-key) (seq api-key))
             (throw (ex-info "Missing OpenAI API key (set OPENAI_API_KEY or :openai-api-key in config.edn)" {})))
         ctx (str/trim (or context-text ""))
         q   (str/trim (str question))
         user (if (seq ctx)
                (str "Recent chat:\n" ctx "\n\nNow respond to this message:\n" q)
                q)

         ;; Choose instructions: explicit arg > config > fallback
         sys (or (some-> instructions str str/trim not-empty)
                 (some-> instructions:cfg str str/trim not-empty)
                 default-pigeon-instructions)

         ;; Optional: force EDN-only output when needed
         sys (if edn-only?
               (str sys "\n\nYou MUST output EDN only. No prose outside EDN.")
               sys)

         ep (endpoint base-url "/v1/responses")
         payload (cond-> {:model model
                          :input user
                          :instructions sys}
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
       (extract-output-text body)))))

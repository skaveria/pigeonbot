(ns pigeonbot.brains.openclaw
  (:require [clojure.string :as str]
            [org.httpkit.client :as http]
            [cheshire.core :as json]
            [pigeonbot.config :as config]))

(defn- cfg []
  (let [m (config/load-config)]
    {:url      (or (:openclaw-url m) "http://127.0.0.1:18789")
     :agent-id (or (:openclaw-agent-id m) "main")
     ;; prefer config.edn, fallback to env
     :token    (or (:openclaw-token m) (System/getenv "OPENCLAW_GATEWAY_TOKEN"))
     :timeout  (or (:brain-timeout-ms m) 60000)}))

(defn- extract-content [resp-body]
  (let [m (cond
            (string? resp-body) (json/decode resp-body true)
            (map? resp-body) resp-body
            :else {})]
    (or (get-in m [:choices 0 :message :content])
        (get-in m [:error :message])
        (str resp-body))))

(defn ask-with-context
  "Call OpenClaw OpenAI-compatible /v1/chat/completions and return assistant content.
  Context is merged into the user message (same behavior as current Ollama path)."
  [context-text question]
  (let [{:keys [url agent-id token timeout]} (cfg)
        _ (when-not (and (string? token) (seq token))
            (throw (ex-info "Missing OpenClaw gateway token (set :openclaw-token in config.edn or OPENCLAW_GATEWAY_TOKEN)"
                            {})))
        ctx (str/trim (or context-text ""))
        q   (str/trim (str question))
        user (if (seq ctx)
               (str "Recent chat:\n" ctx "\n\nNow respond to this message:\n" q)
               q)
        endpoint (str (str/replace url #"/+$" "") "/v1/chat/completions")
        payload {:model "openclaw"
                 :messages [{:role "user" :content user}]}
        headers {"Authorization" (str "Bearer " token)
                 "Content-Type" "application/json"
                 "x-openclaw-agent-id" (str agent-id)}]

    (let [{:keys [status body error] :as resp}
          @(http/post endpoint
                      {:headers headers
                       :body (json/encode payload)
                       :timeout timeout})]
      (cond
        error
        (throw (ex-info "OpenClaw request failed" {:error (str error) :endpoint endpoint}))

        (or (nil? status) (not (<= 200 status 299)))
        (throw (ex-info "OpenClaw returned non-2xx"
                        {:status status :body body :endpoint endpoint}))

        :else
        (extract-content body)))))

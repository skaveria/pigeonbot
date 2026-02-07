(ns pigeonbot.brains.openclaw
  (:require [clojure.string :as str]
            [org.httpkit.client :as http]
            [cheshire.core :as json]
            [pigeonbot.config :as config]))

(defn- add-discord-resize-params
  "Discord media proxy supports width/quality; add them safely.
  Handles URLs that already have ?query, and URLs that end with trailing ? or &."
  [u]
  (let [u0 (str u)
        ;; strip any trailing ? or & so we never create '&&' or '?&'
        u1 (str/replace u0 #"[?&]+$" "")
        sep (if (str/includes? u1 "?") "&" "?")]
    (str u1 sep "width=512&quality=70")))

(def ^:private max-image-bytes (* 2 1024 1024)) ;; 2MB after resize

(defn- ->data-url
  "Fetch an image from a URL and convert it to a data URL.
  Uses Discord's media proxy resize params to keep payloads small."
  [url content-type]
  (let [url0 (str url)
        url1 (if (clojure.string/includes? url0 "media.discordapp.net")
               (add-discord-resize-params url0)
               url0)
        ct   (or (some-> content-type str clojure.string/trim not-empty)
                 "image/png")
        {:keys [status body error]}
        @(org.httpkit.client/get url1 {:as :byte-array :timeout 30000})]
    (cond
      error
      (throw (ex-info "Failed to fetch image bytes"
                      {:url url1 :error (str error)}))

      (or (nil? status) (not (<= 200 status 299)))
      (throw (ex-info "Image fetch returned non-2xx"
                      {:url url1 :status status}))

      (nil? body)
      (throw (ex-info "Image fetch returned empty body"
                      {:url url1 :status status}))

      (> (alength ^bytes body) max-image-bytes)
      (throw (ex-info "Image too large even after resize"
                      {:url url1
                       :bytes (alength ^bytes body)
                       :max max-image-bytes}))

      :else
      (str "data:" ct ";base64,"
           (.encodeToString
            (java.util.Base64/getEncoder)
            ^bytes body)))))

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

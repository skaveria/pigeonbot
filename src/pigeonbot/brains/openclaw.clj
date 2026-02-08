(ns pigeonbot.brains.openclaw
  (:require [clojure.string :as str]
            [org.httpkit.client :as http]
            [cheshire.core :as json]
            [pigeonbot.config :as config]))

;; -----------------------------------------------------------------------------
;; Config + helpers
;; -----------------------------------------------------------------------------

(defn- cfg []
  (let [m (config/load-config)]
    {:url      (or (:openclaw-url m) "http://127.0.0.1:18789")
     :agent-id (or (:openclaw-agent-id m) "main")
     :vision-agent-id (or (:openclaw-vision-agent-id m) "vision")
     :token    (or (:openclaw-token m) (System/getenv "OPENCLAW_GATEWAY_TOKEN"))
     :timeout  (long (or (:brain-timeout-ms m) 60000))}))

(defn- endpoint [base path]
  (str (str/replace (or base "") #"/+$" "") path))

(defn- decode-body [body]
  (cond
    (nil? body) {}
    (map? body) body
    (string? body) (json/decode body true)
    :else {}))

(defn- extract-content [resp-body]
  (let [m (decode-body resp-body)]
    (or (get-in m [:choices 0 :message :content])
        (get-in m [:error :message])
        (str resp-body))))

(defn- extract-json-object
  "Extract last {...} JSON object from text (handles code fences / chatter)."
  [^String s]
  (when-let [ms (seq (re-seq #"\{[^{}]*\}" (or s "")))]
    (try
      (json/decode (last ms) true)
      (catch Throwable _ nil))))

;; -----------------------------------------------------------------------------
;; Chat (used by !ask via pigeonbot.brain)
;; -----------------------------------------------------------------------------

(defn ask-with-context
  "OpenClaw OpenAI-compatible /v1/chat/completions; returns assistant text."
  [context-text question]
  (let [{:keys [url agent-id token timeout]} (cfg)
        _ (when-not (and (string? token) (seq token))
            (throw (ex-info "Missing OpenClaw token" {})))
        ctx (str/trim (or context-text ""))
        q   (str/trim (str question))
        user (if (seq ctx)
               (str "Recent chat:\n" ctx "\n\nNow respond to this message:\n" q)
               q)
        ep (endpoint url "/v1/chat/completions")
        payload {:model "openclaw"
                 :messages [{:role "user" :content user}]}
        headers {"Authorization" (str "Bearer " token)
                 "Content-Type" "application/json"
                 "x-openclaw-agent-id" (str agent-id)}]
    (let [{:keys [status body error]}
          @(http/post ep {:headers headers
                          :body (json/encode payload)
                          :timeout timeout})]
      (cond
        error
        (throw (ex-info "OpenClaw request failed" {:error (str error) :endpoint ep}))

        (or (nil? status) (not (<= 200 status 299)))
        (throw (ex-info "OpenClaw returned non-2xx" {:status status :body body :endpoint ep}))

        :else
        (extract-content body)))))

;; -----------------------------------------------------------------------------
;; Vision: classify by URL (used by vision registry pipeline)
;; -----------------------------------------------------------------------------

(defn classify-image-url
  "Ask OpenClaw to label an image URL.
  Returns {:labels [..] :raw \"...\" :parsed map|nil}.

  Tuned for:
  - fast JSON-only output
  - brand/model preference (no guessing)
  - small output (max 6 labels)
  - uses :openclaw-vision-agent-id (default \"vision\")"
  [image-url]
  (let [{:keys [url vision-agent-id token timeout]} (cfg)
        _ (when-not (and (string? token) (seq token))
            (throw (ex-info "Missing OpenClaw token" {})))

        ep (endpoint url "/v1/chat/completions")
        prompt (str
                "Look at the image at this URL and return labels useful for triggering bot rules.\n"
                "URL: " (str image-url) "\n\n"
                "Strong preference: include brand/model labels  supported by visible text, logos, engravings, or unmistakable markings.\n"
                "Also include generic labels (e.g., \"handgun\", \"cat\", \"bicycle\").\n\n"
                "Return ONLY valid JSON (no markdown, no commentary):\n"
                "{\"labels\":[\"label1\",\"label2\"],\"confidence\":[0.9,0.7]}\n"
                "Rules:\n"
                "- lowercase labels\n"
                "- max 6 labels\n"
                "- confidence 0..1 (same length as labels)\n"
                "- if you cannot fetch/see the image, return {\"labels\":[],\"confidence\":[]}\n")
        payload {:model "openclaw"
                 :temperature 0
                 :max_tokens 140
                 :messages [{:role "user" :content prompt}]}
        headers {"Authorization" (str "Bearer " token)
                 "Content-Type" "application/json"
                 "x-openclaw-agent-id" (str vision-agent-id)}
        {:keys [status body error]}
        @(http/post ep {:headers headers
                        :body (json/encode payload)
                        :timeout timeout})]
    (cond
      error
      (throw (ex-info "OpenClaw request failed" {:error (str error) :endpoint ep}))

      (or (nil? status) (not (<= 200 status 299)))
      (throw (ex-info "OpenClaw returned non-2xx" {:status status :body body :endpoint ep}))

      :else
      (let [raw (-> (extract-content body) str str/trim)
            parsed (or (extract-json-object raw)
                       (try (json/decode raw true) (catch Throwable _ nil)))
            labels (->> (or (:labels parsed) [])
                        (map (comp str/lower-case str/trim str))
                        (remove str/blank?)
                        (take 6)
                        vec)]
        {:labels labels :raw raw :parsed parsed}))))

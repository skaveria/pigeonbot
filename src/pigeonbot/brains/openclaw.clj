(ns pigeonbot.brains.openclaw
  (:require [clojure.string :as str]
            [org.httpkit.client :as http]
            [cheshire.core :as json]
            [pigeonbot.config :as config])
  (:import (java.util Base64)))

;; -----------------------------------------------------------------------------
;; Config + helpers
;; -----------------------------------------------------------------------------

(defn- cfg []
  (let [m (config/load-config)]
    {:url      (or (:openclaw-url m) "http://127.0.0.1:18789")
     :agent-id (or (:openclaw-agent-id m) "main")
     ;; prefer config.edn, fallback to env
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

;; -----------------------------------------------------------------------------
;; Text/chat
;; -----------------------------------------------------------------------------

(defn ask-with-context
  "Call OpenClaw OpenAI-compatible /v1/chat/completions and return assistant content.
  Context is merged into the user message."
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
;; Vision (download -> data URL so the model actually receives pixels)
;; -----------------------------------------------------------------------------

(def ^:private max-image-bytes (* 6 1024 1024)) ;; 6MB cap; tune if needed

(defn- b64 ^String [^bytes bs]
  (.encodeToString (Base64/getEncoder) bs))

(defn- ->data-url
  "Fetch an image from a URL and convert to data URL.
  Uses provided content-type when available; otherwise defaults to image/png."
  [url content-type]
  (let [url (str url)
        ct  (or (some-> content-type str str/trim not-empty) "image/png")
        {:keys [status body error]} @(http/get url {:as :byte-array :timeout 30000})]
    (cond
      error
      (throw (ex-info "Failed to fetch image bytes" {:url url :error (str error)}))

      (or (nil? status) (not (<= 200 status 299)))
      (throw (ex-info "Image fetch returned non-2xx" {:url url :status status}))

      (nil? body)
      (throw (ex-info "Image fetch returned empty body" {:url url :status status}))

      (> (alength ^bytes body) max-image-bytes)
      (throw (ex-info "Image too large to inline as data URL"
                      {:url url :bytes (alength ^bytes body) :max max-image-bytes}))

      :else
      (str "data:" ct ";base64," (b64 ^bytes body)))))

(defn opossum-in-image-debug
  "Returns {:opossum? boolean :raw string :parsed map|nil :status int}.
  Fetches the Discord CDN image and sends it as a base64 data URL."
  [image-url content-type]
  (let [{:keys [url agent-id token timeout]} (cfg)
        _ (when-not (and (string? token) (seq token))
            (throw (ex-info "Missing OpenClaw gateway token" {})))
        image-url (str/trim (str image-url))
        _ (when-not (seq image-url)
            (throw (ex-info "opossum-in-image-debug: missing image-url" {})))

        data-url (->data-url image-url content-type)

        ep (endpoint url "/v1/chat/completions")
        payload {:model "openclaw"
                 :messages
                 [{:role "user"
                   :content
                   [{:type "text"
                     :text (str
                            "Look at the image and decide if it contains an opossum (possum).\n"
                            "Reply with ONLY valid JSON, no markdown, no extra text.\n"
                            "Exactly: {\"opossum\": true} or {\"opossum\": false}\n"
                            "If unsure, use false.")}
                    {:type "image_url"
                     :image_url {:url data-url}}]}]}
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
        (let [raw (-> (extract-content body) str str/trim)
              parsed (try (json/decode raw true) (catch Throwable _ nil))
              op? (cond
                    (boolean? (:opossum parsed)) (:opossum parsed)
                    (re-find #"\bopossum\b" (str/lower-case raw)) true
                    (re-find #"\bpossum\b" (str/lower-case raw)) true
                    :else false)]
          {:opossum? (boolean op?)
           :raw raw
           :parsed parsed
           :status status})))))

(defn opossum-in-image?
  "Returns true/false."
  [image-url content-type]
  (:opossum? (opossum-in-image-debug image-url content-type)))

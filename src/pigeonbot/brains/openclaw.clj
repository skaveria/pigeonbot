;; ---------------------------------------------------------------------------
;; FILE: src/pigeonbot/brains/openclaw.clj
;; ---------------------------------------------------------------------------

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
     :token    (or (:openclaw-token m)
                    (System/getenv "OPENCLAW_GATEWAY_TOKEN"))
     :timeout  (long (or (:brain-timeout-ms m) 60000))}))

(defn- endpoint [base path]
  (str (str/replace (or base "") #"/+$" "") path))

(defn- extract-content [resp-body]
  (let [m (cond
            (string? resp-body) (json/decode resp-body true)
            (map? resp-body) resp-body
            :else {})]
    (or (get-in m [:choices 0 :message :content])
        (get-in m [:error :message])
        (str resp-body))))

;; -----------------------------------------------------------------------------
;; URL normalization (Discord image URLs)
;; -----------------------------------------------------------------------------

(defn- add-param
  "Add query param k=v to URL u safely (preserving existing query).
  Strips trailing ?/& first to avoid '&&' or '?&' situations."
  [u k v]
  (let [u (-> (str u) str/trim (str/replace #"[?&]+$" ""))
        sep (if (str/includes? u "?") "&" "?")]
    (str u sep k "=" v)))

(defn- normalize-discord-url
  "Keep signed querystring, strip trailing junk, and force PNG for reliable decode."
  [u]
  (let [u (-> (str u) str/trim (str/replace #"[?&]+$" ""))]
    (if (or (str/includes? u "media.discordapp.net")
            (str/includes? u "cdn.discordapp.com"))
      (add-param u "format" "png")
      u)))

;; -----------------------------------------------------------------------------
;; Base64 helpers
;; -----------------------------------------------------------------------------

(defn- b64 ^String [^bytes bs]
  (.encodeToString (Base64/getEncoder) bs))

(def ^:private max-image-bytes (* 8 1024 1024)) ;; 8MB cap before base64

(defn- ->data-url
  "Fetch image bytes, base64 them, return a data URL.
  IMPORTANT:
  - preserves Discord signed query params
  - strips trailing &/?
  - forces format=png on Discord URLs"
  [url content-type]
  (let [fetch-url (normalize-discord-url url)
        ;; if we forced format=png, make content-type png too
        ct (if (str/includes? fetch-url "format=png")
             "image/png"
             (or (some-> content-type str str/trim not-empty)
                 "image/png"))
        {:keys [status body error]}
        @(http/get fetch-url {:as :byte-array :timeout 30000})]
    (cond
      error
      (throw (ex-info "Image fetch failed"
                      {:url fetch-url :error (str error)}))

      (or (nil? status) (not (<= 200 status 299)))
      (throw (ex-info "Image fetch returned non-2xx"
                      {:url fetch-url :status status}))

      (nil? body)
      (throw (ex-info "Image fetch returned empty body"
                      {:url fetch-url}))

      (> (alength ^bytes body) max-image-bytes)
      (throw (ex-info "Image too large to inline"
                      {:url fetch-url
                       :bytes (alength ^bytes body)
                       :max max-image-bytes}))

      :else
      (str "data:" ct ";base64," (b64 body)))))

;; -----------------------------------------------------------------------------
;; Text chat
;; -----------------------------------------------------------------------------

(defn ask-with-context
  [context-text question]
  (let [{:keys [url agent-id token timeout]} (cfg)
        _ (when-not (seq token)
            (throw (ex-info "Missing OpenClaw token" {})))
        ctx (str/trim (or context-text ""))
        q   (str/trim (str question))
        user (if (seq ctx)
               (str "Recent chat:\n" ctx "\n\nNow respond:\n" q)
               q)
        ep (endpoint url "/v1/chat/completions")
        payload {:model "openclaw"
                 :messages [{:role "user" :content user}]}
        headers {"Authorization" (str "Bearer " token)
                 "Content-Type" "application/json"
                 "x-openclaw-agent-id" agent-id}]
    (let [{:keys [status body error]}
          @(http/post ep {:headers headers
                          :body (json/encode payload)
                          :timeout timeout})]
      (cond
        error
        (throw (ex-info "OpenClaw request failed" {:error (str error)}))
        (not (<= 200 status 299))
        (throw (ex-info "OpenClaw non-2xx" {:status status :body body}))
        :else
        (extract-content body)))))

;; -----------------------------------------------------------------------------
;; Vision: OPOSSUM DETECTOR (JSON-only, robust extraction)
;; -----------------------------------------------------------------------------

(defn- extract-json-object
  "Extract last {...} JSON object from text (handles code fences / chatter)."
  [^String s]
  (when-let [ms (seq (re-seq #"\{[^{}]*\}" (or s "")))]
    (try
      (json/decode (last ms) true)
      (catch Throwable _ nil))))

(defn opossum-in-image-debug
  "Ask OpenClaw to classify the image at a URL.
  Relies on the OpenClaw agent to fetch and analyze the image."
  [image-url]
  (let [{:keys [url agent-id token timeout]} (cfg)
        ep (endpoint url "/v1/chat/completions")
        prompt (str
                 "Look at the image at the following URL and decide if it contains an opossum (possum).\n"
                 "URL: " image-url "\n\n"
                 "Reply with ONLY valid JSON: {\"opossum\": true} or {\"opossum\": false}.\n"
                 "No explanation.")]
    (let [{:keys [status body error]}
          @(http/post ep
                      {:headers {"Authorization" (str "Bearer " token)
                                 "Content-Type" "application/json"
                                 "x-openclaw-agent-id" agent-id}
                       :body (json/encode
                               {:model "openclaw"
                                :messages [{:role "user" :content prompt}]})
                       :timeout timeout})]
      (cond
        error
        (throw (ex-info "OpenClaw request failed" {:error error}))

        (not (<= 200 status 299))
        (throw (ex-info "OpenClaw non-2xx" {:status status :body body}))

        :else
        (let [raw (-> (extract-content body) str str/trim)
              parsed (try (json/decode raw true) (catch Throwable _ nil))]
          {:opossum? (true? (:opossum parsed))
           :raw raw
           :parsed parsed
           :status status})))))

(defn opossum-in-image?
  [image-url]
  (:opossum? (opossum-in-image-debug image-url)))

(defn classify-image-url
  "Ask OpenClaw to look at an image URL and return label strings.
  Returns {:labels [...] :raw \"...\" :parsed {...}}.

  Tuned to prefer brand/model labels when CLEARLY supported by visible text/logo/markings.
  No guessing: if not clearly visible, omit brand/model labels.

  Still fast/strict:
  - JSON only
  - max 6 labels
  - temperature 0
  - max_tokens small"
  [image-url]
  (let [{:keys [url agent-id token timeout]} (cfg)
        cfgm (config/load-config)
        vision-agent (or (:openclaw-vision-agent-id cfgm) agent-id)

        ep (endpoint url "/v1/chat/completions")
        prompt (str
                "Look at the image at this URL and return labels useful for triggering bot rules.\n"
                "URL: " (str image-url) "\n\n"
                "Strong preference: include brand/model labels supported by visible text, logos, engravings, or unmistakable markings.\n"
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
                 "x-openclaw-agent-id" (str vision-agent)}
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

            ;; robust parse: accept code fences / chatter by extracting last {...}
            parsed (or (when-let [ms (seq (re-seq #"\{[^{}]*\}" raw))]
                         (try (json/decode (last ms) true) (catch Throwable _ nil)))
                       (try (json/decode raw true) (catch Throwable _ nil)))

            labels (->> (or (:labels parsed) [])
                        (map (comp str/lower-case str/trim str))
                        (remove str/blank?)
                        (take 6)
                        vec)]
        {:labels labels :raw raw :parsed parsed}))))

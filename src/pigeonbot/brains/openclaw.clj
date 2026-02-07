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
;; Vision helper
;; -----------------------------------------------------------------------------

(defn opossum-in-image?
  "Ask OpenClaw (vision-capable agent) whether an opossum is present in image-url.
  Returns true/false. Throws on transport errors.

  NOTE: Your OpenClaw agent/model must support vision."
  [image-url]
  (let [{:keys [url agent-id token timeout]} (cfg)
        _ (when-not (and (string? token) (seq token))
            (throw (ex-info "Missing OpenClaw gateway token" {})))
        image-url (str/trim (str image-url))
        _ (when-not (seq image-url)
            (throw (ex-info "opossum-in-image?: missing image-url" {})))

        ep (endpoint url "/v1/chat/completions")

        ;; OpenAI-style multimodal message content:
        ;; [{type:text,text:"..."},{type:image_url,image_url:{url:"..."}}]
        payload {:model "openclaw"
                 :messages [{:role "user"
                             :content [{:type "text"
                                        :text "Look at the image and answer ONLY YES or NO: does it contain an opossum (possum)? If unsure, answer NO."}
                                       {:type "image_url"
                                        :image_url {:url image-url}}]}]}

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
        (let [txt (-> (extract-content body) str str/trim str/upper-case)]
          (boolean (re-find #"^YES\b" txt)))))))

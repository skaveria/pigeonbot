(ns pigeonbot.vision-reacts
  (:require [clojure.string :as str]
            [discljord.messaging :as m]
            [pigeonbot.brains.openclaw :as oc]
            [pigeonbot.config :as config]
            [pigeonbot.state :refer [state]]))

(defonce ^:private reacted-message-ids* (atom #{}))

(def ^:private default-emoji "🦝") ;; temp default for debugging

(defn- cfg []
  (let [m (config/load-config)]
    {:debug? (true? (:vision-debug? m))
     :emoji  (or (:tposs-emoji m) default-emoji)}))

(defn- log! [& xs]
  (when (:debug? (cfg))
    (apply println xs)))

(defn- bot-message? [msg]
  (true? (get-in msg [:author :bot])))

(defn- command-message? [content]
  (and (string? content)
       (str/starts-with? (str/triml content) "!")))

(defn- image-attachment?
  "Heuristic: treat as image if content-type starts with image/ OR filename/url has image extension."
  [{:keys [content-type content_type contentType filename url]}]
  (let [ct (or content-type content_type contentType "")
        fn (or filename "")
        u  (or url "")]
    (or (and (string? ct) (str/starts-with? (str/lower-case ct) "image/"))
        (re-find #"\.(png|jpe?g|gif|webp)$" (str/lower-case fn))
        (re-find #"\.(png|jpe?g|gif|webp)$" (str/lower-case u)))))

(defn- first-image-url [msg]
  (let [atts (or (:attachments msg) [])]
    (log! "vision-reacts: attachments count =" (count atts))
    (when (seq atts)
      (log! "vision-reacts: first attachment keys =" (-> atts first keys sort vec))
      (log! "vision-reacts: first attachment sample =" (pr-str (select-keys (first atts)
                                                                            [:id :filename :url :proxy_url :size
                                                                             :content_type :content-type :contentType]))))
    (some->> atts
             (filter image-attachment?)
             first
             :url)))

(defn- normalize-emoji
  "Accepts:
   - unicode emoji (\"🦝\")
   - \":tposs:\" (name only; may or may not work for custom)
   - \"tposs:123\"
   - \"<:tposs:123>\"
  Returns a string to pass to discljord create-reaction!."
  [s]
  (let [s (str/trim (str s))]
    (cond
      (re-matches #"^<a?:[A-Za-z0-9_]+:\d+>$" s) s
      (re-matches #"^[A-Za-z0-9_]+:\d+$" s) (str "<:" s ">")
      (re-matches #"^:[A-Za-z0-9_]+:$" s) s
      (seq s) s
      :else default-emoji)))

(defn- chosen-emoji []
  (normalize-emoji (:emoji (cfg))))

(defn- react!
  [channel-id message-id emoji]
  (when-let [messaging (:messaging @state)]
    (log! "vision-reacts: reacting" {:channel-id (str channel-id)
                                    :message-id (str message-id)
                                    :emoji emoji})
    (m/create-reaction! messaging channel-id message-id emoji)))

(defn maybe-react-opossum!
  "If msg has an image attachment and OpenClaw says it contains an opossum,
  react with configured emoji. Returns true if it kicked off processing."
  [{:keys [id channel-id content] :as msg}]
  (let [mid (some-> id str)]
    (cond
      (nil? id)
      (do (log! "vision-reacts: skip (no id)") nil)

      (nil? channel-id)
      (do (log! "vision-reacts: skip (no channel-id)") nil)

      (bot-message? msg)
      (do (log! "vision-reacts: skip (author is bot)") nil)

      (command-message? content)
      (do (log! "vision-reacts: skip (command message)" (pr-str content)) nil)

      (contains? @reacted-message-ids* mid)
      (do (log! "vision-reacts: skip (already processed)" mid) nil)

      :else
      (let [img (first-image-url msg)]
        (if-not (seq (str img))
          (do (log! "vision-reacts: no image url found; skip") nil)
          (do
            (swap! reacted-message-ids* conj mid)
            (log! "vision-reacts: image url =" img)
            (future
              (try
                (log! "vision-reacts: calling OpenClaw opossum-in-image? ...")
                (let [yes? (oc/opossum-in-image? img)]
                  (log! "vision-reacts: OpenClaw result =" yes?)
                  (when yes?
                    (let [emoji (chosen-emoji)
                          ch (react! channel-id id emoji)
                          resp (when (instance? clojure.lang.IDeref ch)
                                 (deref ch 8000 :timeout))]
                      (log! "vision-reacts: reaction response =" (pr-str resp)))))
                (catch Throwable t
                  ;; allow future retries if OpenClaw was down; remove from processed set
                  (swap! reacted-message-ids* disj mid)
                  (log! "vision-reacts: ERROR:" (.getMessage t) (or (ex-data t) {})))))
            true))))))

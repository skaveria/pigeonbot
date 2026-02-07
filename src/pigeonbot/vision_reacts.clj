(ns pigeonbot.vision-reacts
  (:require [clojure.string :as str]
            [discljord.messaging :as m]
            [pigeonbot.brains.openclaw :as oc]
            [pigeonbot.config :as config]
            [pigeonbot.state :refer [state]]))

(defonce ^:private reacted-message-ids* (atom #{}))

(def ^:private default-emoji ":tposs:")

(defn- bot-message? [msg]
  (true? (get-in msg [:author :bot])))

(defn- command-message? [content]
  (and (string? content)
       (str/starts-with? (str/triml content) "!")))

(defn- image-attachment?
  "Heuristic: treat as image if content-type starts with image/ OR filename has image extension."
  [{:keys [content-type content_type filename url]}]
  (let [ct (or content-type content_type "")
        fn (or filename "")
        u  (or url "")]
    (or (and (string? ct) (str/starts-with? (str/lower-case ct) "image/"))
        (re-find #"\.(png|jpe?g|gif|webp)$" (str/lower-case fn))
        (re-find #"\.(png|jpe?g|gif|webp)$" (str/lower-case u)))))

(defn- first-image-url [msg]
  (some->> (or (:attachments msg) [])
           (filter image-attachment?)
           first
           :url))

(defn- normalize-emoji
  "Accepts:
   - \":tposs:\" (name only)
   - \"tposs:123\"
   - \"<:tposs:123>\"
  Returns string suitable for discljord create-reaction!."
  [s]
  (let [s (str/trim (str s))]
    (cond
      (re-matches #"^<a?:[A-Za-z0-9_]+:\d+>$" s) s
      (re-matches #"^[A-Za-z0-9_]+:\d+$" s) (str "<:" s ">")
      (re-matches #"^:[A-Za-z0-9_]+:$" s) s
      (seq s) s
      :else default-emoji)))

(defn- tposs-emoji []
  (let [cfg (config/load-config)]
    (normalize-emoji (or (:tposs-emoji cfg) default-emoji))))

(defn- react!
  [channel-id message-id emoji]
  (when-let [messaging (:messaging @state)]
    ;; discljord provides create-reaction! in modern versions; if not, you'll see a var error.
    (m/create-reaction! messaging channel-id message-id emoji)))

(defn maybe-react-opossum!
  "If msg has an image attachment and OpenClaw says it contains an opossum,
  react with :tposs: (or configured emoji). Returns true if reacted."
  [{:keys [id channel-id content] :as msg}]
  (when (and id channel-id
             (not (bot-message? msg))
             (not (command-message? content))
             (not (contains? @reacted-message-ids* (str id))))
    (when-let [img (first-image-url msg)]
      (swap! reacted-message-ids* conj (str id))
      (future
        (try
          (when (oc/opossum-in-image? img)
            (try
              (react! channel-id id (tposs-emoji))
              (catch Throwable t
                (println "vision-reacts: failed to add reaction:" (.getMessage t)
                         {:channel-id (str channel-id)
                          :message-id (str id)
                          :emoji (tposs-emoji)}))))
          (catch Throwable t
            ;; allow future retries if OpenClaw was down; remove from reacted set
            (swap! reacted-message-ids* disj (str id))
            (println "vision-reacts: opossum check failed:" (.getMessage t)
                     (or (ex-data t) {})))))
      true)))

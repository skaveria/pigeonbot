(ns pigeonbot.vision-reacts
  (:require [clojure.string :as str]
            [discljord.messaging :as m]
            [pigeonbot.brains.openclaw :as oc]
            [pigeonbot.commands.util :as u]
            [pigeonbot.config :as config]
            [pigeonbot.state :refer [state]]
            [pigeonbot.vision-registry :as vr]))

(defonce ^:private processed-message-ids* (atom #{}))
(defonce ^:private last-channel-ts* (atom {}))

(defn- cfg []
  (let [m (config/load-config)]
    {:debug? (true? (:vision-debug? m))
     :enabled? (not= false (:vision-enabled? m))
     :max-actions (long (or (:vision-max-actions m) 2))
     :cooldown-ms (long (or (:vision-cooldown-ms m) 0))}))

(defn- log! [& xs]
  (when (:debug? (cfg))
    (apply println xs)))

(defn- bot-message? [msg]
  (true? (get-in msg [:author :bot])))

(defn- command-message? [content]
  (and (string? content)
       (str/starts-with? (str/triml content) "!")))

(defn- image-attachment?
  [{:keys [content-type content_type contentType filename url]}]
  (let [ct (or content-type content_type contentType "")
        fn (or filename "")
        u  (or url "")]
    (or (and (string? ct) (str/starts-with? (str/lower-case ct) "image/"))
        (re-find #"\.(png|jpe?g|gif|webp)$" (str/lower-case fn))
        (re-find #"\.(png|jpe?g|gif|webp)$" (str/lower-case u)))))

(defn- strip-trailing-junk [u]
  (-> (str u) str/trim (str/replace #"[?&]+$" "")))

(defn- first-image-url [msg]
  (let [atts (or (:attachments msg) [])]
    (log! "vision-reacts: attachments count =" (count atts))
    (when-let [att (->> atts (filter image-attachment?) first)]
      (-> (or (:proxy-url att) (:proxy_url att) (:url att))
          strip-trailing-junk))))

(defn- normalize-emoji [s]
  (let [s (str/trim (str s))]
    (cond
      (re-matches #"^<a?:[A-Za-z0-9_]+:\d+>$" s) s
      (re-matches #"^[A-Za-z0-9_]+:\d+$" s) (str "<:" s ">")
      (re-matches #"^:[A-Za-z0-9_]+:$" s) s
      (seq s) s
      :else "🦝")))

(defn- add-reaction! [channel-id message-id emoji]
  (when-let [messaging (:messaging @state)]
    (m/create-reaction! messaging channel-id message-id (normalize-emoji emoji))))

(defn- cooldown-ok? [channel-id]
  (let [{:keys [cooldown-ms]} (cfg)]
    (if (<= cooldown-ms 0)
      true
      (let [now (System/currentTimeMillis)
            last-ts (get @last-channel-ts* (str channel-id) 0)]
        (when (>= (- now last-ts) cooldown-ms)
          (swap! last-channel-ts* assoc (str channel-id) now)
          true)))))

(defn- match-rule?
  [labels {:keys [match]}]
  (let [labels (set (map str/lower-case (or labels [])))
        ms (set (map str/lower-case (or match [])))]
    (some labels ms)))

(defn maybe-react-vision!
  "Rule-driven vision handler:
  - if message has an image attachment
  - classify once via OpenClaw (labels)
  - apply matching rules from vision_rules.edn
  - execute actions (:react, :reply)

  Returns true if it kicked off processing."
  [{:keys [id channel-id content] :as msg}]
  (let [{:keys [enabled? max-actions]} (cfg)
        mid (some-> id str)]
    (when (and enabled?
               id channel-id
               (not (bot-message? msg))
               (not (command-message? content))
               (not (contains? @processed-message-ids* mid))
               (cooldown-ok? channel-id))
      (when-let [img-url (first-image-url msg)]
        (swap! processed-message-ids* conj mid)
        (future
          (try
            ;; ensure rules loaded
            (vr/load!)
            (let [{:keys [labels raw]} (oc/classify-image-url img-url)
                  labels (or labels [])
                  _ (log! "vision-reacts: labels =" (pr-str labels))
                  rules (vr/list-rules)
                  matches (->> rules
                               (filter (partial match-rule? labels))
                               (take max-actions)
                               vec)]
              (doseq [{:keys [actions id]} matches]
                (when-let [emoji (:react actions)]
                  (log! "vision-reacts: rule" id "react" emoji)
                  (add-reaction! channel-id id emoji))
                (when-let [reply (:reply actions)]
                  (log! "vision-reacts: rule" id "reply")
                  (u/send-reply! channel-id id :content (u/clamp-discord reply)))))
            (catch Throwable t
              (swap! processed-message-ids* disj mid)
              (log! "vision-reacts: ERROR:" (.getMessage t) (or (ex-data t) {})))))
        true))))

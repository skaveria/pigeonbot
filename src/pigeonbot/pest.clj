(ns pigeonbot.pest
  (:require [clojure.string :as str]
            [pigeonbot.config :as config]
            [pigeonbot.slap :as slap]
            [pigeonbot.brain :as brain]
            [pigeonbot.context :as ctx]
            [pigeonbot.commands.util :as u]))

;; Runtime overrides (nil => use config.edn defaults)
(defonce ^:private pest*
  (atom {:enabled? nil
         :chance nil
         :cooldown-ms nil
         :min-chars nil
         :allow-channel-ids nil
         :deny-channel-ids nil
         :last-spoke-by-channel {}}))

(defn- cfg []
  (let [m (config/load-config)]
    {:enabled? (true? (:pest-enabled? m))                       ;; default false
     :chance (double (or (:pest-chance m) 0.08))                ;; 8%
     :cooldown-ms (long (or (:pest-cooldown-ms m) 180000))      ;; 3 minutes
     :min-chars (long (or (:pest-min-chars m) 24))              ;; ignore tiny messages
     :allow-channel-ids (vec (map str (or (:pest-allow-channel-ids m) [])))
     :deny-channel-ids (vec (map str (or (:pest-deny-channel-ids m) [])))}))

(defn enabled?
  "Effective enabled? (runtime override wins)."
  []
  (let [ov (:enabled? @pest*)]
    (if (nil? ov)
      (:enabled? (cfg))
      (true? ov))))

(defn set-enabled!
  "Set runtime pest enabled flag (true/false). Returns current enabled?"
  [v]
  (swap! pest* assoc :enabled? (boolean v))
  (enabled?))

(defn clear-overrides!
  "Clear runtime overrides (falls back to config.edn)."
  []
  (swap! pest* assoc
         :enabled? nil
         :chance nil
         :cooldown-ms nil
         :min-chars nil
         :allow-channel-ids nil
         :deny-channel-ids nil)
  true)

(defn status
  "Return effective + override status (for debugging / !pest status)."
  []
  (let [c (cfg)
        o @pest*]
    {:enabled? (enabled?)
     :config c
     :override (select-keys o [:enabled? :chance :cooldown-ms :min-chars :allow-channel-ids :deny-channel-ids])}))

(defn- now-ms [] (System/currentTimeMillis))

(defn- effective
  "Get effective setting for k (runtime override if non-nil else config)."
  [k]
  (let [o (get @pest* k)]
    (if (nil? o)
      (get (cfg) k)
      o)))

(defn- command-message? [content]
  (and (string? content)
       (str/starts-with? (str/triml content) "!")))

(defn- bot-message? [msg]
  (true? (get-in msg [:author :bot])))

(defn- allowed-channel?
  [channel-id]
  (let [cid (some-> channel-id str)
        allow (vec (map str (or (effective :allow-channel-ids) [])))
        deny  (vec (map str (or (effective :deny-channel-ids) [])))]
    (cond
      (not (seq cid)) false
      (seq deny) (not (some #{cid} deny))
      (seq allow) (some #{cid} allow)
      :else true)))

(defn- cooldown-ok?
  [channel-id]
  (let [cid (some-> channel-id str)
        cooldown-ms (long (or (effective :cooldown-ms) 0))
        last-ts (get-in @pest* [:last-spoke-by-channel cid] 0)
        now (now-ms)]
    (or (<= cooldown-ms 0)
        (>= (- now (long last-ts)) cooldown-ms))))

(defn- mark-spoke!
  [channel-id]
  (let [cid (some-> channel-id str)]
    (when (seq cid)
      (swap! pest* assoc-in [:last-spoke-by-channel cid] (now-ms))
      true)))

(defn- random-hit?
  "Probabilistic decision."
  [chance]
  (< (rand) (double chance)))

(defn- worth-responding?
  "Cheap filter so pest mode doesn't yap at 'lol'."
  [content]
  (let [s (-> (or content "") str (str/replace #"\s+" " ") str/trim)
        minc (long (or (effective :min-chars) 0))]
    (and (<= minc (count s))
         ;; also ignore pure emoji / punctuation-ish
         (not (str/blank? (str/replace s #"[^\p{L}\p{N}]+" ""))))))

(defn- slap-enabled? []
  (true? (:slap-enabled? (config/load-config))))

(defn- compute-reply
  [msg content]
  (let [q (str/trim (str content ""))]
    (if (str/blank? q)
      nil
      (if (slap-enabled?)
        (let [{:keys [answer]} (slap/run-slap! (assoc msg :content q))]
          (some-> answer str))
        (let [context-text (ctx/context-text msg)]
          (brain/ask-with-context context-text q))))))

(defn maybe-pest!
  "Main hook. Call this from :message-create after normal command handling.
  Returns true if a pest reply was scheduled/sent, else nil."
  [{:keys [channel-id id content] :as msg}]
  (when (and (enabled?)
             channel-id
             id
             (not (bot-message? msg))
             (not (command-message? content))
             (allowed-channel? channel-id)
             (cooldown-ok? channel-id)
             (worth-responding? content)
             (random-hit? (double (effective :chance))))
    ;; mark cooldown immediately to avoid double-fire if multiple events arrive quickly
    (mark-spoke! channel-id)
    (future
      (try
        (when-let [reply (compute-reply msg content)]
          (let [reply (u/clamp-discord reply)]
            ;; reply to the triggering message to keep it tidy
            (u/send-reply! channel-id (str id) :content reply)))
        (catch Throwable t
          ;; if the reply fails, release cooldown so it can try again later
          (swap! pest* update :last-spoke-by-channel dissoc (str channel-id))
          (println "pest error:" (.getMessage t)))))
    true))

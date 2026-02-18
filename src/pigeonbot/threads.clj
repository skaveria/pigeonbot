(ns pigeonbot.threads
  (:require [clojure.core.async :as async]
            [discljord.messaging :as m]
            [pigeonbot.state :refer [state]]))

;; Discord channel types for threads:
;; 10 = AnnouncementThread, 11 = PublicThread, 12 = PrivateThread
(def ^:private thread-channel-types #{10 11 12})

(defonce ^{:doc "channel-id(string) -> channel-type(int) cache"} channel-type-cache* (atom {}))
(defonce ^{:doc "Set of thread channel ids (string) where pigeonbot has spoken at least once"} active-thread-ids* (atom #{}))

(defn- now-ms [] (System/currentTimeMillis))

(defn- fetch-channel-type!
  "Fetch channel object once via Discord API and cache :type.
  Returns an int channel type, or nil."
  [channel-id]
  (let [cid (str channel-id)
        messaging (:messaging @state)]
    (when messaging
      (try
        ;; discljord endpoint returns an IDeref + core.async channel
        (let [ch (m/get-channel! messaging cid)
              ;; don't hang forever if discord is cranky
              [resp port] (async/alts!! [ch (async/timeout 2000)])]
          (when (= port ch)
            (when-let [t (:type resp)]
              (swap! channel-type-cache* assoc cid t)
              t)))
        (catch Throwable _ nil)))))

(defn thread-channel?
  "True if channel-id is a Discord thread channel."
  [channel-id]
  (let [cid (str channel-id)
        t (or (get @channel-type-cache* cid)
              (fetch-channel-type! cid))]
    (contains? thread-channel-types t)))

(defn note-bot-spoke!
  "Mark this channel-id as an active thread conversation if it is a thread."
  [channel-id]
  (let [cid (str channel-id)]
    (when (thread-channel? cid)
      (swap! active-thread-ids* conj cid)
      true)))

(defn thread-active?
  "True if the bot has spoken in this thread channel-id during this process lifetime."
  [channel-id]
  (contains? @active-thread-ids* (str channel-id)))

(defn should-auto-ask?
  "Return true iff:
  - message is in a thread channel
  - bot has spoken in this thread before
  - message is not authored by a bot
  - message isn't a command (starts with !)
  - message has non-blank content"
  [{:keys [channel-id content author] :as _msg}]
  (let [cid (some-> channel-id str)
        bot? (true? (get author :bot))
        s (str (or content ""))]
    (and cid
         (not bot?)
         (not (clojure.string/blank? s))
         (not (clojure.string/starts-with? s "!"))
         (thread-channel? cid)
         (thread-active? cid))))

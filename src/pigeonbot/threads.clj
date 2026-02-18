(ns pigeonbot.threads
  (:require [clojure.string :as str]))

;; Discord thread channel types:
;; 10 = AnnouncementThread, 11 = PublicThread, 12 = PrivateThread
(def ^:private thread-channel-types #{10 11 12})

(defonce active-thread-ids*
  "Set of Discord channel IDs (strings) that represent thread channels where
  pigeonbot has spoken at least once during this process lifetime."
  (atom #{}))

(defn thread-channel?
  "Return true if this message channel is a thread, based on :channel-type."
  [{:keys [channel-type]}]
  (contains? thread-channel-types channel-type))

(defn note-bot-spoke!
  "Mark a channel-id as a thread we should continue conversing in.

  We don't strictly require proof it's a thread here — that check happens in
  should-auto-ask?. This lets us call note-bot-spoke! from send!/send-reply!
  which only know channel-id."
  [channel-id]
  (when (and (string? (str channel-id)) (seq (str channel-id)))
    (swap! active-thread-ids* conj (str channel-id))
    true))

(defn should-auto-ask?
  "Option B:
  If pigeonbot has spoken at least once in a thread channel, then any subsequent
  non-command message in that same thread should be treated like an implicit ask.

  Conditions:
  - message is in a thread (:channel-type in thread-channel-types)
  - channel-id is in active-thread-ids*
  - author is not a bot
  - content is non-blank
  - content is not a bang-command"
  [{:keys [channel-id content author] :as msg}]
  (let [cid  (some-> channel-id str)
        bot? (true? (get author :bot))
        s    (str (or content ""))]
    (and cid
         (thread-channel? msg)
         (contains? @active-thread-ids* cid)
         (not bot?)
         (not (str/blank? s))
         (not (str/starts-with? s "!")))))

(defn debug-state []
  {:active-thread-ids @active-thread-ids*
   :active-count (count @active-thread-ids*)})

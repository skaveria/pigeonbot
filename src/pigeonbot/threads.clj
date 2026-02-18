(ns pigeonbot.threads
  (:require [clojure.string :as str]))

;; Discord thread channel types:
;; 10 = AnnouncementThread, 11 = PublicThread, 12 = PrivateThread
(def ^:private thread-channel-types #{10 11 12})

;; defonce does NOT accept a docstring argument; keep it simple.
(defonce active-thread-ids* (atom #{}))

(defn thread-channel?
  "True if this message is in a Discord thread, based on :channel-type."
  [{:keys [channel-type]}]
  (contains? thread-channel-types channel-type))

(defn note-thread-active!
  "Mark a channel-id as an active thread pigeonbot should continue conversing in."
  [channel-id]
  (let [cid (some-> channel-id str)]
    (when (seq cid)
      (swap! active-thread-ids* conj cid)
      true)))

(defn should-auto-ask?
  "Option B:
  If pigeonbot has spoken in a thread, subsequent non-command messages in that
  same thread should be treated like an implicit ask."
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
  {:active-count (count @active-thread-ids*)
   :active-thread-ids @active-thread-ids*})

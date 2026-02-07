(ns pigeonbot.context
  (:require [clojure.string :as str]))

(def ^:private max-context-messages
  "How many recent messages per channel to keep in memory."
  20)

(defonce ^{:doc "Map: channel-id(string) -> vector of message maps (most-recent last).
Each entry: {:id :channel-id :author :bot? :content :timestamp}."}
  histories*
  (atom {}))

(defn- chan-id->key [channel-id]
  (str channel-id))

(defn- normalize-author
  "Extract a stable display name and bot flag from a Discord event payload."
  [{:keys [author]}]
  (let [a author
        bot? (true? (:bot a))
        name (or (:global_name a)
                 (:username a)
                 (get-in a [:user :username])
                 "unknown")]
    {:name name :bot? bot?}))

(defn record-message!
  "Store a message-create payload into a per-channel rolling buffer.

Stores minimal fields needed for context. Safe to call on every :message-create."
  [{:keys [channel-id id content] :as msg}]
  (when channel-id
    (let [{:keys [name bot?]} (normalize-author msg)
          entry {:id (str id)
                 :channel-id (str channel-id)
                 :author name
                 :bot? bot?
                 :content (str (or content ""))
                 :timestamp (str (or (:timestamp msg) ""))}]
      (swap! histories*
             (fn [m]
               (let [k (chan-id->key channel-id)
                     xs (vec (get m k []))
                     xs (conj xs entry)
                     xs (if (> (count xs) max-context-messages)
                          (subvec xs (- (count xs) max-context-messages))
                          xs)]
                 (assoc m k xs)))))
    true))

(defn recent-messages
  "Return up to the last N messages for channel-id.
Optionally exclude a specific message id (e.g. the current message)."
  ([channel-id] (recent-messages channel-id nil))
  ([channel-id exclude-id]
   (let [k (chan-id->key channel-id)
         xs (get @histories* k [])]
     (->> xs
          (remove (fn [m] (and exclude-id (= (:id m) (str exclude-id)))))
          vec))))

(defn format-context
  "Turn recent messages into a compact transcript for the LLM."
  [msgs]
  (let [line (fn [{:keys [author bot? content]}]
               (let [speaker (if bot? (str author " (bot)") author)
                     content (-> (or content "")
                                 (str/replace #"\s+" " ")
                                 (str/trim))]
                 (str speaker ": " content)))]
    (->> msgs
         (map line)
         (remove str/blank?)
         (str/join "\n"))))

;; -----------------------------------------------------------------------------
;; Extensibility hooks (optional, but handy)
;; -----------------------------------------------------------------------------

(defn context-text
  "High-level helper: given a message event payload, return the transcript string.
This is the function you’ll extend later when you want more context sources."
  [{:keys [channel-id id]}]
  (-> (recent-messages channel-id id)
      format-context))

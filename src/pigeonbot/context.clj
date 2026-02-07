(ns pigeonbot.context
  (:require [clojure.string :as str]))

(def ^:private max-context-messages 20)

(defonce histories*
  "Map: channel-id(string) -> vector of message maps (most-recent last)."
  (atom {}))

(defn- chan-id->key [channel-id]
  (str channel-id))

(defn- normalize-author [{:keys [author] :as msg}]
  (let [a author
        bot? (true? (:bot a))
        name (or (:global_name a)
                 (:username a)
                 (get-in a [:user :username])
                 "unknown")]
    {:name name :bot? bot?}))

(defn record-message!
  "Store a message-create payload into a per-channel rolling buffer.
  Only stores minimal fields needed for context."
  [{:keys [channel-id id content] :as msg}]
  (when channel-id
    (let [{:keys [name bot?]} (normalize-author msg)
          entry {:id (str id)
                 :channel-id (str channel-id)
                 :author name
                 :bot? bot?
                 :content (str (or content ""))
                 ;; Discord sends :timestamp sometimes; keep if present
                 :timestamp (str (or (:timestamp msg) ""))}]
      (swap! histories*
             (fn [m]
               (let [k (chan-id->key channel-id)
                     xs (vec (get m k []))
                     xs (conj xs entry)
                     ;; keep only last N
                     xs (if (> (count xs) max-context-messages)
                          (subvec xs (- (count xs) max-context-messages))
                          xs)]
                 (assoc m k xs)))))
    true))

(defn recent-messages
  "Return up to the last max-context-messages messages for channel-id.
  Optionally exclude a specific message id (e.g. the current message)."
  ([channel-id] (recent-messages channel-id nil))
  ([channel-id exclude-id]
   (let [k (chan-id->key channel-id)
         xs (get @histories* k [])]
     (->> xs
          (remove (fn [m] (and exclude-id (= (:id m) (str exclude-id)))))
          vec))))

(defn format-context
  "Turn recent messages into a compact text transcript for the LLM."
  [msgs]
  (let [line (fn [{:keys [author bot? content]}]
               (let [speaker (if bot? (str author " (bot)") author)
                     content (-> (or content "")
                                 (str/replace #"\s+" " ")
                                 (str/trim))]
                 (str speaker ": " content)))]
    (->> msgs
         (map line)
         (remove #(= % ""))
         (str/join "\n"))))

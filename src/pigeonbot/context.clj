(ns pigeonbot.context
  "Compatibility shim: forwards to pigeonbot.db.
  The atom-based in-memory buffer has been replaced by Datalevin."
  (:require [pigeonbot.db :as db]
            [clojure.string :as str]))

(defn record-message!
  "Store a Discord message in Datalevin."
  [msg]
  (try (db/record-message! msg)
       (catch Throwable t
         (println "context/record-message! error:" (.getMessage t)))))

(defn recent-messages
  "Return up to 20 recent messages for channel-id."
  ([channel-id] (recent-messages channel-id nil))
  ([channel-id _exclude-id]
   (db/recent-messages channel-id 20)))

(defn format-context
  "Format messages as a plain-text transcript."
  [msgs]
  (->> msgs
       (map (fn [{:keys [author bot? content]}]
              (let [speaker (if bot? (str author " (bot)") author)
                    content (-> (str (or content ""))
                                (str/replace #"\s+" " ")
                                str/trim)]
                (str speaker ": " content))))
       (remove str/blank?)
       (str/join "\n")))

(defn context-text
  "High-level helper: return transcript string for a channel."
  [{:keys [channel-id]}]
  (-> (recent-messages channel-id)
      format-context))

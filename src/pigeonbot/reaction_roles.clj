(ns pigeonbot.reaction-roles
  (:require [discljord.messaging :as m]
            [pigeonbot.state :refer [state]]))

;; ---------------------------------------------------------------------------
;; Configuration (hard-coded for now, as requested)
;; ---------------------------------------------------------------------------

(def reaction-message-id
  "Message ID to watch for reactions."
  1462920692671447142)

(def reaction-role-id
  "Role to grant/revoke."
  1462911178777825505)

(def reaction-emoji-names
  "Emoji names to accept.

   NOTE: For standard unicode emojis, Discord sends :emoji {:name \"⚔️\"}
   rather than a shortcode like \":crossed_swords:\"."
  #{"⚔" "⚔️" "crossed_swords" ":crossed_swords:"})

(defn- to-long [x]
  (cond
    (integer? x) (long x)
    (number? x)  (long x)
    (string? x)  (Long/parseLong (.trim ^String x))
    :else        (long x)))

(defn- emoji-matches?
  [emoji]
  (let [n (:name emoji)]
    (contains? reaction-emoji-names n)))


(defn handle-reaction-add!
  "Handle a :message-reaction-add event.
   Adds role when (message-id == reaction-message-id) and emoji matches."
  [{:keys [guild-id user-id message-id emoji] :as evt}]
  (println "rr/add evt:"
           {:guild-id guild-id
            :user-id user-id
            :message-id message-id
            :emoji emoji})

  (cond
    (nil? guild-id)
    (println "rr/add: missing guild-id (DM reaction?)")

    (nil? user-id)
    (println "rr/add: missing user-id")

    (nil? message-id)
    (println "rr/add: missing message-id")

    (not (map? emoji))
    (println "rr/add: emoji not a map:" (pr-str emoji))

    :else
    (let [mid (to-long message-id)
          en  (:name emoji)]
      (println "rr/add: mid=" mid
               "target=" reaction-message-id
               "emoji-name=" (pr-str en)
               "matches?=" (emoji-matches? emoji))

      (if-not (= mid reaction-message-id)
        (println "rr/add: message-id mismatch")

        (if-not (emoji-matches? emoji)
          (println "rr/add: emoji mismatch")

          (if-let [messaging (:messaging @state)]
            (do
              (println "rr/add: applying role"
                       reaction-role-id
                       "to user"
                       user-id)
              (m/add-guild-member-role!
               messaging
               (to-long guild-id)
               (to-long user-id)
               (to-long reaction-role-id)))

            (println "rr/add: messaging is nil")))))))


(defn handle-reaction-remove!
  "Handle a :message-reaction-remove event.
   Optional: removes role when reaction is removed."
  [{:keys [guild-id user-id message-id emoji]}]
  (when (and guild-id user-id message-id (map? emoji))
    (let [mid (to-long message-id)]
      (when (and (= mid reaction-message-id)
                 (emoji-matches? emoji))
        (when-let [messaging (:messaging @state)]
          (m/remove-guild-member-role!
           messaging
           (to-long guild-id)
           (to-long user-id)
           (to-long reaction-role-id)))))))

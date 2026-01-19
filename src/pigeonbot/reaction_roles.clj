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
  [{:keys [guild-id user-id message-id emoji]}]
  (when (and guild-id user-id message-id (map? emoji))
    (let [mid (to-long message-id)]
      (when (and (= mid reaction-message-id)
                 (emoji-matches? emoji))
        (when-let [messaging (:messaging @state)]
          ;; Add role to the reacting user
          (m/add-guild-member-role!
           messaging
           (to-long guild-id)
           (to-long user-id)
           (to-long reaction-role-id)))))))

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

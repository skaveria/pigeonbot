(ns pigeonbot.reaction-roles
  (:require [discljord.messaging :as m]
            [pigeonbot.state :refer [state]]))

(def reaction-rules
  [{:message-id 1462920692671447142
    :role-id    1462910582582935583
    :emoji      {:name "⚔️"}
    :label      "runescapers"}

   {:message-id 1462920692671447142
    :role-id    1462910818239779120
    :emoji      {:name "tposs"}
    :label      "opossum"}

   {:message-id 1462920692671447142
    :role-id    1462911178777825505
    :emoji      {:name "🫠"}
    :label      "silly"}])

(defn- to-long [x]
  (cond
    (integer? x) (long x)
    (number? x)  (long x)
    (string? x)  (Long/parseLong (.trim ^String x))
    :else        (long x)))

(defn- emoji=?
  "Match emoji from event against rule.
   - Unicode emoji: match :name
   - Custom emoji: match :id"
  [rule-emoji event-emoji]
  (and (map? rule-emoji)
       (map? event-emoji)
       (cond
         (:id rule-emoji) (= (str (:id rule-emoji)) (str (:id event-emoji)))
         (:name rule-emoji) (= (:name rule-emoji) (:name event-emoji))
         :else false)))

(defn- matching-rule
  [{:keys [message-id emoji]}]
  (let [mid (to-long message-id)]
    (some (fn [r]
            (when (and (= mid (to-long (:message-id r)))
                       (emoji=? (:emoji r) emoji))
              r))
          reaction-rules)))

(defn handle-reaction-add!
  [{:keys [guild-id user-id] :as evt}]
  (when (and guild-id user-id)
    (when-let [{:keys [role-id label]} (matching-rule evt)]
      (println "rr/add matched:" label "role" role-id "user" user-id)
      (when-let [messaging (:messaging @state)]
        (m/add-guild-member-role!
         messaging
         (to-long guild-id)
         (to-long user-id)
         (to-long role-id))))))

(defn handle-reaction-remove!
  [{:keys [guild-id user-id] :as evt}]
  (when (and guild-id user-id)
    (when-let [{:keys [role-id label]} (matching-rule evt)]
      (println "rr/remove matched:" label "role" role-id "user" user-id)
      (when-let [messaging (:messaging @state)]
        (m/remove-guild-member-role!
         messaging
         (to-long guild-id)
         (to-long user-id)
         (to-long role-id))))))

(ns pigeonbot.roles
  (:require [clojure.set :as set]
            [discljord.messaging :as m]
            [pigeonbot.config :as config]
            [pigeonbot.state :refer [state]]))

(defn allowed-role-ids
  "Return a set of role IDs (longs) that are self-assignable."
  []
  (->> (get (config/load-config) :self-role-ids [])
       (map long)
       set))

(defn add-role!
  "Add a role to a user by raw role-id."
  [guild-id user-id role-id]
  (let [role-id (long role-id)]
    (cond
      (nil? (:messaging @state))
      {:ok? false :reason :no-messaging}

      (not (contains? (allowed-role-ids) role-id))
      {:ok? false :reason :not-allowed}

      :else
      (do
        (m/add-guild-member-role!
         (:messaging @state)
         (long guild-id)
         (long user-id)
         role-id)
        {:ok? true :role-id role-id}))))

(defn remove-role!
  "Remove a role from a user by raw role-id."
  [guild-id user-id role-id]
  (let [role-id (long role-id)]
    (cond
      (nil? (:messaging @state))
      {:ok? false :reason :no-messaging}

      (not (contains? (allowed-role-ids) role-id))
      {:ok? false :reason :not-allowed}

      :else
      (do
        (m/remove-guild-member-role!
         (:messaging @state)
         (long guild-id)
         (long user-id)
         role-id)
        {:ok? true :role-id role-id}))))

(defn list-roles
  "Return allowed role IDs."
  []
  (sort (allowed-role-ids)))

(ns pigeonbot.roles
  (:require [discljord.messaging :as m]
            [pigeonbot.config :as config]
            [pigeonbot.state :refer [state]]))

(defn allowed-role-ids
  "Return a set of role IDs (longs) that are self-assignable.
   Accepts numbers or numeric strings in config.edn."
  []
  (let [ids (get (config/load-config) :self-role-ids [])]
    (->> ids
         (keep (fn [x]
                 (cond
                   (integer? x) (long x)
                   (number? x)  (long x)
                   (string? x)  (try
                                  (Long/parseLong (clojure.string/trim x))
                                  (catch Throwable _ nil))
                   :else nil)))
         set)))

(defn list-roles
  "Return allowed self-assignable role IDs."
  []
  (sort (allowed-role-ids)))

(defn add-role!
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

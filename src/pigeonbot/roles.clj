(ns pigeonbot.roles
  (:require [discljord.messaging :as m]
            [pigeonbot.config :as config]
            [pigeonbot.state :refer [state]]))

(defn allowed-role-ids
  "Return a set of self-assignable role IDs (longs).

  Accepts IDs from either:
  - :self-role-ids  (preferred)
  - :self-roles     (fallback)

  Values may be:
  - numbers
  - numeric strings
  - keywords like :1234567890
  Any non-numeric values are ignored."
  []
  (let [cfg (config/load-config)
        raw (or (:self-role-ids cfg)
                (:self-roles cfg)
                [])]
    (->> raw
         (keep (fn [x]
                 (cond
                   (integer? x) (long x)
                   (number? x)  (long x)
                   (string? x)  (let [s (.trim ^String x)]
                                  (when (re-matches #"\d+" s)
                                    (Long/parseLong s)))
                   (keyword? x) (let [s (name x)]
                                  (when (re-matches #"\d+" s)
                                    (Long/parseLong s)))
                   :else nil)))
         set)))

(defn list-roles []
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

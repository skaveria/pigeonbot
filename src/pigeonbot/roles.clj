(ns pigeonbot.roles
  (:require [clojure.core.async :as a]
            [clojure.string :as str]
            [discljord.messaging :as m]
            [pigeonbot.config :as config]
            [pigeonbot.state :refer [state]]))

(defonce roles*
  ;; {guild-id {:by-name {:role-name role-id}
  ;;            :by-id   {role-id role-map}
  ;;            :ts-ms   123456789}}
  (atom {}))

(defn- normalize-name [s]
  (let [raw (-> (or s "") str str/trim str/lower-case)
        cooked (-> raw
                   (str/replace #"[^a-z0-9]+" "-")
                   (str/replace #"^-+" "")
                   (str/replace #"-+$" "")
                   (str/replace #"-{2,}" "-"))]
    (when (seq cooked) (keyword cooked))))

(defn allowed-role-names
  "Returns a set of normalized keywords for self-assignable roles."
  []
  (->> (get (config/load-config) :self-roles [])
       (keep normalize-name)
       set))

(defn cached-role-id
  "Look up a role-id by normalized role keyword for a guild."
  [guild-id role-k]
  (get-in @roles* [(long guild-id) :by-name role-k]))

(defn refresh-roles!
  "Fetch roles for a guild and refresh the local cache.
   Returns a channel that yields true/false."
  [guild-id]
  (let [out (a/promise-chan)
        guild-id (long guild-id)]
    (if-let [messaging (:messaging @state)]
      (let [ch (m/get-guild-roles! messaging guild-id)] ;; Discljord endpoint  [oai_citation:2‡discljord.github.io](https://discljord.github.io/discljord/)
        (a/go
          (try
            (let [roles (a/<! ch)
                  ;; role objects include :id and :name
                  by-id (into {} (map (fn [r] [(long (:id r)) r]) roles))
                  by-name (into {}
                                (keep (fn [r]
                                        (when-let [k (normalize-name (:name r))]
                                          [k (long (:id r))])))
                                roles)]
              (swap! roles* assoc guild-id {:by-id by-id
                                            :by-name by-name
                                            :ts-ms (System/currentTimeMillis)})
              (a/>! out true))
            (catch Throwable _t
              (a/>! out false)))))
      (a/put! out false))
    out))

(defn ensure-roles!
  "If we don't have roles cached for this guild yet, refresh them."
  [guild-id]
  (if (get @roles* (long guild-id))
    (doto (a/promise-chan) (a/put! true))
    (refresh-roles! guild-id)))

(defn add-role!
  "Add a role to a member, if it's self-assignable."
  [guild-id user-id role-name]
  (let [role-k (normalize-name role-name)
        allowed (allowed-role-names)]
    (cond
      (nil? role-k)
      {:ok? false :reason "bad-role-name"}

      (not (contains? allowed role-k))
      {:ok? false :reason "not-allowed" :role role-k}

      :else
      (let [role-id (cached-role-id guild-id role-k)]
        (if-not role-id
          {:ok? false :reason "unknown-role" :role role-k}
          (do
            (m/add-guild-member-role! (:messaging @state) (long guild-id) (long user-id) (long role-id))
            {:ok? true :role role-k :role-id role-id})))))) ;; add-guild-member-role!  [oai_citation:3‡discljord.github.io](https://discljord.github.io/discljord/)

(defn remove-role!
  "Remove a role from a member, if it's self-assignable."
  [guild-id user-id role-name]
  (let [role-k (normalize-name role-name)
        allowed (allowed-role-names)]
    (cond
      (nil? role-k)
      {:ok? false :reason "bad-role-name"}

      (not (contains? allowed role-k))
      {:ok? false :reason "not-allowed" :role role-k}

      :else
      (let [role-id (cached-role-id guild-id role-k)]
        (if-not role-id
          {:ok? false :reason "unknown-role" :role role-k}
          (do
            (m/remove-guild-member-role! (:messaging @state) (long guild-id) (long user-id) (long role-id))
            {:ok? true :role role-k :role-id role-id})))))) ;; remove-guild-member-role!  [oai_citation:4‡discljord.github.io](https://discljord.github.io/discljord/)

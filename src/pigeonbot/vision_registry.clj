(ns pigeonbot.vision-registry
  (:require [clojure.edn :as edn]
            [clojure.java.io :as io]
            [clojure.string :as str]
            [pigeonbot.config :as config]))

(def ^:private rules-path
  "EDN file in repo root storing vision rules."
  "vision_rules.edn")

(defonce ^{:doc "Vector of rules, each like:
  {:id \"opossum\"
   :match [\"opossum\" \"possum\"]
   :min-confidence 0.7
   :actions {:react \"🦝\" :reply \"tposs spotted\"}
   :added-by \"123\"
   :added-at 1770480000000}"}
  rules* (atom []))

(defn load! []
  (let [f (io/file rules-path)]
    (reset! rules*
            (if (.exists f)
              (try
                (edn/read-string (slurp f))
                (catch Throwable t
                  (println "vision-registry/load!: failed to read" rules-path ":" (.getMessage t))
                  []))
              [])))
  @rules*)

(defn save! []
  (spit rules-path (pr-str @rules*))
  @rules*)

(defn allowed-to-register?
  "Default: allow everyone.
   If config.edn contains :vision-admin-ids [\"123\" \"456\"], only allow those."
  [{:keys [author]}]
  (let [uid (get-in author [:id])
        cfg (try (config/load-config) (catch Throwable _ {}))
        admins (set (map str (or (:vision-admin-ids cfg) [])))]
    (or (empty? admins)
        (contains? admins (str uid)))))

(defn- normalize-id [s]
  (-> (or s "")
      str
      str/trim
      str/lower-case))

(defn- normalize-match [xs]
  (->> xs
       (map normalize-id)
       (remove str/blank?)
       vec))

(defn- now-ms [] (System/currentTimeMillis))

(defn list-rules []
  (->> @rules*
       (sort-by :id)
       vec))

(defn lookup [id]
  (let [id (normalize-id id)]
    (some (fn [r] (when (= (:id r) id) r)) @rules*)))

(defn delete!
  "Delete a rule by id. Returns true if existed."
  [id]
  (let [id (normalize-id id)
        existed? (boolean (lookup id))]
    (swap! rules* (fn [rs] (vec (remove (fn [r] (= (:id r) id)) rs))))
    (save!)
    existed?))

(defn clear!
  "Delete all vision rules. Returns count removed."
  []
  (let [n (count @rules*)]
    (reset! rules* [])
    (save!)
    n))

(defn register-react!
  "Register or update a rule to react when label matches.
  label: string like \"beretta\"
  emoji: unicode or <:name:id> / :name:
  Returns {:ok? true :id ...}."
  [label emoji author-id]
  (let [id (normalize-id label)
        match [id]
        emoji (str emoji)
        ts (now-ms)]
    (swap! rules*
           (fn [rs]
             (let [rs (vec (remove (fn [r] (= (:id r) id)) rs))]
               (conj rs {:id id
                         :match match
                         :min-confidence 0.0
                         :actions {:react emoji}
                         :added-by (str author-id)
                         :added-at ts}))))
    (save!)
    {:ok? true :id id}))

(defn register-reply!
  "Register or update a rule to reply when label matches."
  [label reply-text author-id]
  (let [id (normalize-id label)
        match [id]
        txt (str reply-text)
        ts (now-ms)]
    (swap! rules*
           (fn [rs]
             (let [rs (vec (remove (fn [r] (= (:id r) id)) rs))]
               (conj rs {:id id
                         :match match
                         :min-confidence 0.0
                         :actions {:reply txt}
                         :added-by (str author-id)
                         :added-at ts}))))
    (save!)
    {:ok? true :id id}))

(defn register-both!
  "Register or update a rule to react + reply."
  [label emoji reply-text author-id]
  (let [id (normalize-id label)
        match [id]
        ts (now-ms)]
    (swap! rules*
           (fn [rs]
             (let [rs (vec (remove (fn [r] (= (:id r) id)) rs))]
               (conj rs {:id id
                         :match match
                         :min-confidence 0.0
                         :actions {:react (str emoji)
                                   :reply (str reply-text)}
                         :added-by (str author-id)
                         :added-at ts}))))
    (save!)
    {:ok? true :id id}))

(defn set-confidence!
  "Set :min-confidence for a rule."
  [label min-conf]
  (let [id (normalize-id label)
        min-conf (double min-conf)]
    (swap! rules*
           (fn [rs]
             (mapv (fn [r]
                     (if (= (:id r) id)
                       (assoc r :min-confidence min-conf)
                       r))
                   rs)))
    (save!)
    {:ok? true :id id :min-confidence min-conf}))

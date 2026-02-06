(ns pigeonbot.channels
  (:require [clojure.edn :as edn]
            [clojure.java.io :as io]
            [clojure.string :as str]
            [discljord.messaging :as m]
            [pigeonbot.state :refer [state]]))

(def channels-path "channels.edn")

(defonce channels*
  (atom {:by-name {} :by-id {}}))

(defn- normalize-name [s]
  (let [raw (-> (or s "")
                str
                str/trim
                str/lower-case)
        cooked (-> raw
                   (str/replace #"[^a-z0-9]+" "-")
                   (str/replace #"^-+" "")
                   (str/replace #"-+$" "")
                   (str/replace #"-{2,}" "-"))]
    (when (seq cooked)
      (keyword cooked))))

(defn load-channels!
  "Load channels.edn if it exists."
  []
  (when (.exists (io/file channels-path))
    (let [m (-> channels-path slurp edn/read-string)]
      (reset! channels* (merge {:by-name {} :by-id {}} m))))
  @channels*)

(defn save-channels!
  "Persist registry to channels.edn."
  []
  (spit channels-path (pr-str @channels*))
  @channels*)

(defn remember-channel!
  "Remember a friendly name for a channel-id."
  [friendly channel-id]
  (if-let [k (normalize-name friendly)]
    (let [id (long channel-id)]
      (swap! channels*
             (fn [{:keys [by-name by-id] :as m}]
               (let [old-id   (get by-name k)
                     old-name (get by-id id)
                     by-name' (cond-> by-name
                                old-id (dissoc k)
                                old-name (dissoc old-name)
                                true (assoc k id))
                     by-id'   (cond-> by-id
                                old-id (dissoc old-id)
                                old-name (dissoc id)
                                true (assoc id k))]
                 (assoc m :by-name by-name' :by-id by-id'))))
      (save-channels!)
      {:name k :id id})
    (do
      (println "remember-channel!: invalid friendly name:" (pr-str friendly))
      nil)))

(defn id [friendly]
  (get-in @channels* [:by-name (keyword friendly)]))

(defn list-channels []
  (->> (get @channels* :by-name)
       (sort-by (comp name key))
       (map (fn [[k v]] [k v]))))

(defn send!
  "Send by friendly channel name (keyword or string)."
  [friendly text]
  (if-let [channel-id (id friendly)]
    (if-let [messaging (:messaging @state)]
      (m/create-message! messaging channel-id :content (str text))
      (do (println "channels/send!: messaging is nil") nil))
    (do (println "channels/send!: unknown channel" friendly) nil)))

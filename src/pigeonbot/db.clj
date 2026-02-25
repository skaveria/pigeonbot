(ns pigeonbot.db
  (:require [clojure.java.io :as io]
            [clojure.string :as str]
            [datalevin.core :as d]))

(def ^:private db-path "./data/pigeonbot-db")

(def ^:private schema
  {:message/id          {:db/unique :db.unique/identity}
   :message/ts          {:db/valueType :db.type/instant
                         :db/index true}
   :message/guild-id    {:db/valueType :db.type/string
                         :db/index true}
   :message/channel-id  {:db/valueType :db.type/string
                         :db/index true}
   :message/channel-type {:db/valueType :db.type/long
                          :db/index true}
   :message/author-id   {:db/valueType :db.type/string
                         :db/index true}
   :message/author-name {:db/valueType :db.type/string
                         :db/index true}
   :message/bot?        {:db/valueType :db.type/boolean
                         :db/index true}
   :message/content     {:db/valueType :db.type/string
                         :db/fulltext true}})

(defonce ^:private conn* (atom nil))

(defn ensure-conn!
  "Open (or return) the Datalevin connection for pigeonbot."
  []
  (or @conn*
      (let [dir (io/file db-path)]
        (.mkdirs dir)
        (reset! conn* (d/create-conn db-path schema)))))

(defn close!
  []
  (when-let [c @conn*]
    (try (d/close c) (catch Throwable _))
    (reset! conn* nil))
  true)

(defn- parse-instant
  "Discord gives ISO8601 timestamps. Instant/parse handles them."
  [s]
  (when (and (string? s) (seq s))
    (try
      (java.time.Instant/parse s)
      (catch Throwable _ nil))))

(defn- normalize-author
  [msg]
  (let [a (:author msg)
        bot? (true? (:bot a))
        name (or (:global_name a)
                 (:username a)
                 (get-in a [:user :username])
                 "unknown")
        author-id (or (:id a) (get-in a [:user :id]))]
    {:author-name (str name)
     :author-id   (some-> author-id str)
     :bot?        bot?}))

(defn message->tx
  "Convert a Discord :message-create payload into a Datalevin entity map.

  Important Datalevin note:
  - Do NOT use lookup refs like [:message/id \"...\"] in :db/id inside map tx.
  - Instead, use a tempid and rely on :message/id being :db.unique/

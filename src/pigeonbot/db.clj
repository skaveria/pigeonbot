(ns pigeonbot.db
  (:require [clojure.java.io :as io]
            [clojure.string :as str]
            [datalevin.core :as d]))

(def ^:private db-path "./data/pigeonbot-db")

(def ^:private schema
  {:message/id           {:db/unique :db.unique/identity}
   :message/ts           {:db/valueType :db.type/instant
                          :db/index true}
   :message/guild-id     {:db/valueType :db.type/string
                          :db/index true}
   :message/channel-id   {:db/valueType :db.type/string
                          :db/index true}
   :message/channel-type {:db/valueType :db.type/long
                          :db/index true}
   :message/author-id    {:db/valueType :db.type/string
                          :db/index true}
   :message/author-name  {:db/valueType :db.type/string
                          :db/index true}
   :message/bot?         {:db/valueType :db.type/boolean
                          :db/index true}
   :message/content      {:db/valueType :db.type/string
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

(defn db []
  (d/db (ensure-conn!)))

(defn- parse-discord-ts->date
  "Discord gives ISO8601; store as java.util.Date for Datalevin :db.type/instant."
  [s]
  (when (and (string? s) (seq s))
    (try
      (java.util.Date/from (java.time.Instant/parse s))
      (catch Throwable _ nil))))

(defn- normalize-author
  [msg]
  (let [a (:author msg)
        bot? (true? (:bot a))
        name (or (:global_name a)
                 (:username a)
                 (get-in a [:user :username])
                 "unknown")
        author-id (or (:id a)
                      (get-in a [:user :id]))]
    {:author-name (str name)
     :author-id   (some-> author-id str)
     :bot?        bot?}))

(defn- drop-nils
  "Datalevin cannot store nil attribute values; remove them."
  [m]
  (into {} (remove (comp nil? val)) m))

(defn message->tx
  "Convert a Discord message payload into a Datalevin entity map.

  Datalevin notes:
  - Use a tempid; rely on :message/id unique identity for upsert.
  - Do not include nil values."
  [{:keys [id timestamp guild-id channel-id channel-type content] :as msg}]
  (let [{:keys [author-name author-id bot?]} (normalize-author msg)
        mid (some-> id str)
        ts  (or (parse-discord-ts->date (str timestamp))
                (java.util.Date.))
        content (-> (or content "")
                    (str/replace #"\u0000" "")
                    (str/trim))]
    (when (seq mid)
      (drop-nils
       {:db/id                (d/tempid :db.part/user)
        :message/id           mid
        :message/ts           ts
        :message/guild-id     (some-> guild-id str)
        :message/channel-id   (some-> channel-id str)
        :message/channel-type (when channel-type (long channel-type))
        :message/author-id    (or author-id "")
        :message/author-name  author-name
        :message/bot?         bot?
        :message/content      content}))))

(defn upsert-message!
  "Idempotently store a message in Datalevin (by :message/id)."
  [msg]
  (when-let [tx (message->tx msg)]
    (d/transact! (ensure-conn!) [tx])
    true))

(defn count-messages
  "Total message count (quick sanity check)."
  []
  (let [dbv (db)]
    (ffirst (d/q '[:find (count ?e)
                   :where [?e :message/id]]
                 dbv))))

(defn fulltext
  "Full-text search over :message/content.
  Returns sequence of [e a v] datoms."
  ([query] (fulltext query {}))
  ([query opts]
   (d/q '[:find ?e ?a ?v
          :in $ ?q ?opts
          :where [(fulltext $ ?q ?opts) [[?e ?a ?v]]]]
        (db) query opts)))

(defn pull-message
  "Pull a message entity by eid."
  [dbv eid]
  (try
    (d/pull dbv
            [:message/id
             :message/ts
             :message/guild-id
             :message/channel-id
             :message/channel-type
             :message/author-id
             :message/author-name
             :message/bot?
             :message/content]
            eid)
    (catch Throwable _ nil)))

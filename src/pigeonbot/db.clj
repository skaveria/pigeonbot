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
  "Convert a Discord :message-create payload into a Datalevin entity map."
  [{:keys [id timestamp guild-id channel-id channel-type content] :as msg}]
  (let [{:keys [author-name author-id bot?]} (normalize-author msg)
        mid (some-> id str)
        ts  (or (parse-instant (str timestamp))
                ;; fallback: now
                (java.time.Instant/now))
        content (-> (or content "")
                    (str/replace #"\u0000" "")   ;; defensive
                    (str/trim))]
    (when (seq mid)
      {:db/id               [:message/id mid]
       :message/id          mid
       :message/ts          ts
       :message/guild-id    (some-> guild-id str)
       :message/channel-id  (some-> channel-id str)
       :message/channel-type (when channel-type (long channel-type))
       :message/author-id   (or author-id "")
       :message/author-name author-name
       :message/bot?        bot?
       :message/content     content})))

(defn upsert-message!
  "Idempotently store a message in Datalevin (by :message/id)."
  [msg]
  (when-let [tx (message->tx msg)]
    (let [conn (ensure-conn!)]
      (d/transact! conn [tx])
      true)))

(defn db []
  (d/db (ensure-conn!)))

(defn count-messages
  "Total message count (quick sanity check)."
  []
  (let [dbv (db)]
    (count (d/q '[:find ?e :where [?e :message/id]] dbv))))

(defn fulltext
  "Full-text search over :message/content.
  Returns sequence of [e a v] datoms ordered by relevance."
  ([query] (fulltext query {}))
  ([query opts]
   ;; Datalevin provides `fulltext` query fn.  [oai_citation:4‡cljdoc.org](https://cljdoc.org/d/datalevin/datalevin/0.10.3/doc/search-engine?utm_source=chatgpt.com)
   (d/q '[:find ?e ?a ?v
          :in $ ?q ?opts
          :where [(fulltext $ ?q ?opts) [[?e ?a ?v]]]]
        (db) query opts)))

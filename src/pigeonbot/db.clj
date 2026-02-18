(ns pigeonbot.db
  (:require [datalevin.core :as d]
            [clojure.edn :as edn]
            [pigeonbot.config :as config]))

;; ---------------------------------------------------------------------------
;; Schema
;; ---------------------------------------------------------------------------

(def schema
  {:message/id         {:db/valueType   :db.type/string
                        :db/unique      :db.unique/identity
                        :db/cardinality :db.cardinality/one}
   :message/channel-id {:db/valueType   :db.type/string
                        :db/cardinality :db.cardinality/one
                        :db/index       true}
   :message/guild-id   {:db/valueType   :db.type/string
                        :db/cardinality :db.cardinality/one}
   :message/author     {:db/valueType   :db.type/string
                        :db/cardinality :db.cardinality/one}
   :message/author-id  {:db/valueType   :db.type/string
                        :db/cardinality :db.cardinality/one
                        :db/index       true}
   :message/bot?       {:db/valueType   :db.type/boolean
                        :db/cardinality :db.cardinality/one}
   :message/content    {:db/valueType   :db.type/string
                        :db/cardinality :db.cardinality/one
                        :db/fulltext    true}
   :message/timestamp  {:db/valueType   :db.type/instant
                        :db/cardinality :db.cardinality/one
                        :db/index       true}

   :fact/id            {:db/valueType   :db.type/string
                        :db/unique      :db.unique/identity
                        :db/cardinality :db.cardinality/one}
   :fact/subject       {:db/valueType   :db.type/string
                        :db/cardinality :db.cardinality/one
                        :db/index       true}
   :fact/predicate     {:db/valueType   :db.type/string
                        :db/cardinality :db.cardinality/one}
   :fact/value         {:db/valueType   :db.type/string
                        :db/cardinality :db.cardinality/one
                        :db/fulltext    true}
   :fact/source-msg-id {:db/valueType   :db.type/string
                        :db/cardinality :db.cardinality/one}
   :fact/timestamp     {:db/valueType   :db.type/instant
                        :db/cardinality :db.cardinality/one
                        :db/index       true}

   :user/discord-id    {:db/valueType   :db.type/string
                        :db/unique      :db.unique/identity
                        :db/cardinality :db.cardinality/one}
   :user/name          {:db/valueType   :db.type/string
                        :db/cardinality :db.cardinality/one}
   :user/first-seen    {:db/valueType   :db.type/instant
                        :db/cardinality :db.cardinality/one}
   :user/last-seen     {:db/valueType   :db.type/instant
                        :db/cardinality :db.cardinality/one}})

;; ---------------------------------------------------------------------------
;; Connection lifecycle
;; ---------------------------------------------------------------------------

(defonce conn* (atom nil))

(defn db-path []
  (or (System/getenv "PIGEONBOT_DB_PATH")
      (:db-path (config/load-config))
      "pigeonbot.db"))

(defn open! []
  (let [path (db-path)
        conn (d/get-conn path schema)]
    (reset! conn* conn)
    (println "[db] Datalevin opened at" path)
    conn))

(defn close! []
  (when-let [c @conn*]
    (d/close c)
    (reset! conn* nil)
    (println "[db] Datalevin closed")))

(defn conn []
  (or @conn*
      (throw (ex-info "DB not open. Call db/open! first." {}))))

;; ---------------------------------------------------------------------------
;; Write helpers
;; ---------------------------------------------------------------------------

(defn- parse-ts [ts]
  (try (java.time.Instant/parse (str ts))
       (catch Throwable _ (java.time.Instant/now))))

(defn- upsert-user! [discord-id name ts]
  (let [existing (d/pull (d/db (conn)) [:user/first-seen] [:user/discord-id discord-id])
        first-seen (or (:user/first-seen existing) ts)]
    (d/transact! (conn)
                 [{:user/discord-id discord-id
                   :user/name       name
                   :user/first-seen first-seen
                   :user/last-seen  ts}])))

(defn record-message!
  "Transact a Discord message-create event payload into Datalevin."
  [{:keys [id channel-id guild-id content timestamp author] :as msg}]
  (when id
    (let [a          (or author {})
          bot?       (true? (:bot a))
          name       (or (:global_name a) (:username a) "unknown")
          author-id  (some-> (or (:id a) "") str)
          ts         (parse-ts timestamp)]
      (d/transact! (conn)
                   [{:message/id         (str id)
                     :message/channel-id (str channel-id)
                     :message/guild-id   (str (or guild-id ""))
                     :message/author     name
                     :message/author-id  author-id
                     :message/bot?       bot?
                     :message/content    (str (or content ""))
                     :message/timestamp  ts}])
      (when (seq author-id)
        (upsert-user! author-id name ts))))
  true)

(defn transact-facts!
  "Transact a seq of extracted fact maps (string keys, from SLAP :extract) into Datalevin."
  [facts source-msg-id]
  (let [now    (java.time.Instant/now)
        txdata (keep (fn [f]
                       (let [subj (get f "subject" "")
                             pred (get f "predicate" "")
                             val  (get f "value" "")]
                         (when (and (seq subj) (seq pred))
                           {:fact/id          (str (java.util.UUID/randomUUID))
                            :fact/subject     subj
                            :fact/predicate   pred
                            :fact/value       (str val)
                            :fact/source-msg-id (str source-msg-id)
                            :fact/timestamp   now})))
                     facts)]
    (when (seq txdata)
      (d/transact! (conn) (vec txdata)))))

;; ---------------------------------------------------------------------------
;; Read helpers
;; ---------------------------------------------------------------------------

(defn recent-messages
  "Return up to n most recent messages in a channel, oldest first."
  [channel-id n]
  (let [db (d/db (conn))]
    (->> (d/q '[:find ?id ?author ?bot? ?content ?ts
                :in $ ?cid
                :where
                [?e :message/channel-id ?cid]
                [?e :message/id ?id]
                [?e :message/author ?author]
                [?e :message/bot? ?bot?]
                [?e :message/content ?content]
                [?e :message/timestamp ?ts]]
              db (str channel-id))
         (sort-by last)
         (take-last n)
         (mapv (fn [[id author bot? content ts]]
                 {:id id :author author :bot? bot? :content content :timestamp ts})))))

(defn messages-since
  "Return messages in a channel since the given java.time.Instant."
  [channel-id since-instant]
  (let [db (d/db (conn))]
    (->> (d/q '[:find ?id ?author ?bot? ?content ?ts
                :in $ ?cid ?since
                :where
                [?e :message/channel-id ?cid]
                [?e :message/id ?id]
                [?e :message/author ?author]
                [?e :message/bot? ?bot?]
                [?e :message/content ?content]
                [?e :message/timestamp ?ts]
                [(>= ?ts ?since)]]
              db (str channel-id) since-instant)
         (sort-by last)
         (mapv (fn [[id author bot? content ts]]
                 {:id id :author author :bot? bot? :content content :timestamp ts})))))

(defn facts-since
  "Return all facts recorded since the given java.time.Instant."
  [since-instant]
  (let [db (d/db (conn))]
    (->> (d/q '[:find ?subj ?pred ?val ?ts
                :in $ ?since
                :where
                [?e :fact/subject ?subj]
                [?e :fact/predicate ?pred]
                [?e :fact/value ?val]
                [?e :fact/timestamp ?ts]
                [(>= ?ts ?since)]]
              db since-instant)
         (sort-by last))))

(defn user-facts
  "Return all facts where :fact/subject = discord-user-id, sorted by time."
  [discord-user-id]
  (let [db (d/db (conn))]
    (->> (d/q '[:find ?pred ?val ?ts
                :in $ ?subject
                :where
                [?e :fact/subject ?subject]
                [?e :fact/predicate ?pred]
                [?e :fact/value ?val]
                [?e :fact/timestamp ?ts]]
              db (str discord-user-id))
         (sort-by last))))

(defn user-info
  "Return user record for a discord-id, or nil."
  [discord-id]
  (let [db (d/db (conn))]
    (first
     (d/q '[:find ?name ?first ?last
            :in $ ?uid
            :where
            [?e :user/discord-id ?uid]
            [?e :user/name ?name]
            [?e :user/first-seen ?first]
            [?e :user/last-seen ?last]]
          db (str discord-id)))))

(defn channel-message-count
  "Return number of messages stored for a channel."
  [channel-id]
  (let [db (d/db (conn))]
    (or (ffirst
         (d/q '[:find (count ?e)
                :in $ ?cid
                :where [?e :message/channel-id ?cid]]
              db (str channel-id)))
        0)))

(defn run-query
  "Execute a query-back map from the LLM against the live db.
  query-map has string keys: 'find' (vec of strings) 'where' (vec of strings).
  Strings are parsed as Datalog via edn/read-string."
  [{:strs [find where]}]
  (let [db      (d/db (conn))
        find-v  (mapv #(edn/read-string %) find)
        where-v (mapv #(edn/read-string (str "[" % "]")) where)
        query   {:find find-v :where where-v}]
    (try
      (d/q query db)
      (catch Throwable t
        (println "[db] run-query failed:" (.getMessage t) (pr-str query))
        #{}))))

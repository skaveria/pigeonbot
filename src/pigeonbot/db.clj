(ns pigeonbot.db
  (:require [clojure.java.io :as io]
            [clojure.string :as str]
            [datalevin.core :as d]))

(def ^:private db-path "./data/pigeonbot-db")

(def ^:private schema
  {;; -------------------------
   ;; SPINE: raw messages
   ;; -------------------------
   :message/id           {:db/unique :db.unique/identity}
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
                          :db/fulltext true}

   ;; -------------------------
   ;; META: derived annotations (mechanical)
   ;; -------------------------
   :meta/message-id       {:db/unique :db.unique/identity
                           :db/valueType :db.type/string
                           :db/index true}
   :meta/ts               {:db/valueType :db.type/instant
                           :db/index true}
   :meta/guild-id         {:db/valueType :db.type/string
                           :db/index true}
   :meta/channel-id       {:db/valueType :db.type/string
                           :db/index true}
   :meta/author-id        {:db/valueType :db.type/string
                           :db/index true}
   :meta/bot?             {:db/valueType :db.type/boolean
                           :db/index true}
   :meta/is-command?      {:db/valueType :db.type/boolean
                           :db/index true}
   :meta/has-attachments? {:db/valueType :db.type/boolean
                           :db/index true}
   :meta/has-links?       {:db/valueType :db.type/boolean
                           :db/index true}
   :meta/reply-to-id      {:db/valueType :db.type/string
                           :db/index true}
   :meta/char-count       {:db/valueType :db.type/long
                           :db/index true}
   :meta/word-count       {:db/valueType :db.type/long
                           :db/index true}
   :meta/mention-ids      {:db/valueType :db.type/string
                           :db/cardinality :db.cardinality/many
                           :db/index true}
   :meta/attachment-urls  {:db/valueType :db.type/string
                           :db/cardinality :db.cardinality/many
                           :db/index true}

   ;; -------------------------
   ;; EXTRACTS: SLAP writeback (semantic memory)
   ;; -------------------------
   :extract/id            {:db/unique :db.unique/identity
                           :db/valueType :db.type/string
                           :db/index true}
   :extract/ts            {:db/valueType :db.type/instant
                           :db/index true}
   :extract/text          {:db/valueType :db.type/string
                           :db/fulltext true}
   :extract/kind          {:db/valueType :db.type/keyword
                           :db/index true}
   :extract/confidence    {:db/valueType :db.type/double
                           :db/index true}
   :extract/tags          {:db/valueType :db.type/string
                           :db/cardinality :db.cardinality/many
                           :db/index true}
   :extract/scope         {:db/valueType :db.type/keyword
                           :db/index true}
   :extract/guild-id      {:db/valueType :db.type/string
                           :db/index true}
   :extract/channel-id    {:db/valueType :db.type/string
                           :db/index true}
   :extract/message-id    {:db/valueType :db.type/string
                           :db/index true}
   :extract/packet-id     {:db/valueType :db.type/string
                           :db/index true}
   :extract/model         {:db/valueType :db.type/string
                           :db/index true}})

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

;; -----------------------------------------------------------------------------
;; SPINE writes
;; -----------------------------------------------------------------------------

(defn message->tx
  "Convert a Discord message payload into a Datalevin entity map (spine)."
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

;; -----------------------------------------------------------------------------
;; META writes
;; -----------------------------------------------------------------------------

(defn- message-reference-id
  [msg]
  (or (get-in msg [:message_reference :message_id])
      (get-in msg [:message_reference :message-id])
      (get-in msg [:message-reference :message_id])
      (get-in msg [:message-reference :message-id])
      (get-in msg [:referenced_message :id])))

(defn- looks-like-command? [content]
  (and (string? content)
       (str/starts-with? (str/triml content) "!")))

(defn- looks-like-link? [content]
  (boolean (re-find #"https?://\S+" (or content ""))))

(defn- mention-ids [msg]
  (->> (or (:mentions msg) [])
       (map (fn [m] (some-> (:id m) str)))
       (remove str/blank?)
       distinct
       vec))

(defn- attachment-urls [msg]
  (->> (or (:attachments msg) [])
       (map (fn [a] (or (:url a) (:proxy_url a) (:proxy-url a))))
       (map (fn [u] (some-> u str str/trim)))
       (remove str/blank?)
       distinct
       vec))

(defn- word-count [s]
  (let [s (-> (or s "") (str/replace #"\s+" " ") str/trim)]
    (if (str/blank? s) 0 (count (str/split s #" ")))))

(defn message-meta->tx
  "Derived metadata tx for a Discord message."
  [{:keys [id timestamp guild-id channel-id content] :as msg}]
  (let [{:keys [author-id bot?]} (normalize-author msg)
        mid (some-> id str)
        ts  (or (parse-discord-ts->date (str timestamp))
                (java.util.Date.))
        content (str (or content ""))
        m-ids (mention-ids msg)
        a-urls (attachment-urls msg)]
    (when (seq mid)
      (drop-nils
       (cond-> {:db/id                 (d/tempid :db.part/user)
                :meta/message-id       mid
                :meta/ts               ts
                :meta/guild-id         (some-> guild-id str)
                :meta/channel-id       (some-> channel-id str)
                :meta/author-id        (or author-id "")
                :meta/bot?             bot?
                :meta/is-command?      (looks-like-command? content)
                :meta/has-attachments? (boolean (seq a-urls))
                :meta/has-links?       (looks-like-link? content)
                :meta/reply-to-id      (some-> (message-reference-id msg) str)
                :meta/char-count       (long (count content))
                :meta/word-count       (long (word-count content))}
         (seq m-ids) (assoc :meta/mention-ids m-ids)
         (seq a-urls) (assoc :meta/attachment-urls a-urls))))))

(defn upsert-message-meta!
  [msg]
  (when-let [tx (message-meta->tx msg)]
    (d/transact! (ensure-conn!) [tx])
    true))

;; -----------------------------------------------------------------------------
;; EXTRACT writes
;; -----------------------------------------------------------------------------

(defn- normalize-tags
  [xs]
  (->> (or xs [])
       (map (fn [t] (-> (str t) str/lower-case str/trim)))
       (remove str/blank?)
       distinct
       vec))

(defn- now-date [] (java.util.Date.))

(defn extract-item->tx
  "Convert one extract item to a tx map.
  Supports:
  - string  -> {:extract/text \"...\" :extract/kind :note}
  - map     -> uses keys when present
  Context params supply scope + provenance."
  [ctx item]
  (let [{:keys [guild-id channel-id message-id packet-id model]} ctx
        base {:db/id             (d/tempid :db.part/user)
              :extract/id         (str (java.util.UUID/randomUUID))
              :extract/ts         (now-date)
              :extract/guild-id   (some-> guild-id str)
              :extract/channel-id (some-> channel-id str)
              :extract/message-id (some-> message-id str)
              :extract/packet-id  (some-> packet-id str)
              :extract/model      (or model "openai")}
        item-map (cond
                   (string? item) {:text item
                                   :kind :note
                                   :confidence 0.75
                                   :tags []}
                   (map? item) item
                   :else nil)]
    (when item-map
      (let [txt (some-> (or (:text item-map) (:extract/text item-map)) str str/trim)
            kind (or (:kind item-map) (:extract/kind item-map) :note)
            conf (double (or (:confidence item-map) (:extract/confidence item-map) 0.75))
            scope (or (:scope item-map) (:extract/scope item-map) :channel)
            tags (normalize-tags (or (:tags item-map) (:extract/tags item-map) []))]
        (when (seq txt)
          (drop-nils
           (cond-> (assoc base
                          :extract/text txt
                          :extract/kind kind
                          :extract/confidence conf
                          :extract/scope scope)
             (seq tags) (assoc :extract/tags tags))))))))

(defn upsert-extracts!
  "Write a vector of extract items into the DB as separate entities.
  We don't de-dupe yet; you can add later."
  [ctx extracts]
  (let [extracts (or extracts [])
        txs (->> extracts
                 (map (partial extract-item->tx ctx))
                 (remove nil?)
                 vec)]
    (when (seq txs)
      (d/transact! (ensure-conn!) txs)
      (count txs))))

;; -----------------------------------------------------------------------------
;; Queries / helpers
;; -----------------------------------------------------------------------------

(defn count-messages []
  (let [dbv (db)]
    (ffirst (d/q '[:find (count ?e) :where [?e :message/id]] dbv))))

(defn fulltext
  ([query] (fulltext query {}))
  ([query opts]
   (d/q '[:find ?e ?a ?v
          :in $ ?q ?opts
          :where [(fulltext $ ?q ?opts) [[?e ?a ?v]]]]
        (db) query opts)))

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
   :message/ts           {:db/valueType :db.type/instant :db/index true}
   :message/guild-id     {:db/valueType :db.type/string  :db/index true}
   :message/channel-id   {:db/valueType :db.type/string  :db/index true}
   :message/channel-type {:db/valueType :db.type/long    :db/index true}
   :message/author-id    {:db/valueType :db.type/string  :db/index true}
   :message/author-name  {:db/valueType :db.type/string  :db/index true}
   :message/bot?         {:db/valueType :db.type/boolean :db/index true}
   :message/content      {:db/valueType :db.type/string  :db/fulltext true}

   ;; -------------------------
   ;; META: derived annotations (mechanical)
   ;; -------------------------
   :meta/message-id       {:db/unique :db.unique/identity
                           :db/valueType :db.type/string :db/index true}
   :meta/ts               {:db/valueType :db.type/instant :db/index true}
   :meta/guild-id         {:db/valueType :db.type/string  :db/index true}
   :meta/channel-id       {:db/valueType :db.type/string  :db/index true}
   :meta/author-id        {:db/valueType :db.type/string  :db/index true}
   :meta/bot?             {:db/valueType :db.type/boolean :db/index true}
   :meta/is-command?      {:db/valueType :db.type/boolean :db/index true}
   :meta/has-attachments? {:db/valueType :db.type/boolean :db/index true}
   :meta/has-links?       {:db/valueType :db.type/boolean :db/index true}
   :meta/reply-to-id      {:db/valueType :db.type/string  :db/index true}
   :meta/char-count       {:db/valueType :db.type/long    :db/index true}
   :meta/word-count       {:db/valueType :db.type/long    :db/index true}
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
                           :db/valueType :db.type/string :db/index true}

   ;; de-dupe key
   :extract/hash          {:db/unique :db.unique/identity
                           :db/valueType :db.type/string :db/index true}

   :extract/ts            {:db/valueType :db.type/instant :db/index true}
   :extract/last-seen     {:db/valueType :db.type/instant :db/index true}
   :extract/seen-count    {:db/valueType :db.type/long    :db/index true}

   :extract/text          {:db/valueType :db.type/string  :db/fulltext true}
   :extract/kind          {:db/valueType :db.type/keyword :db/index true}
   :extract/confidence    {:db/valueType :db.type/double  :db/index true}
   :extract/scope         {:db/valueType :db.type/keyword :db/index true}
   :extract/guild-id      {:db/valueType :db.type/string  :db/index true}
   :extract/channel-id    {:db/valueType :db.type/string  :db/index true}
   :extract/message-id    {:db/valueType :db.type/string  :db/index true}
   :extract/packet-id     {:db/valueType :db.type/string  :db/index true}
   :extract/model         {:db/valueType :db.type/string  :db/index true}

   ;; Correct tag storage: one datom per tag
   :extract/tag           {:db/valueType :db.type/string
                           :db/cardinality :db.cardinality/many
                           :db/index true}

   ;; Legacy (do not write going forward)
   :extract/tags          {:db/valueType :db.type/string  :db/index true}

   ;; -------------------------
   ;; REPO: indexed source files (repo-only)
   ;; -------------------------
   :repo/path   {:db/unique :db.unique/identity :db/valueType :db.type/string :db/index true}
   :repo/sha    {:db/valueType :db.type/string :db/index true}
   :repo/ts     {:db/valueType :db.type/instant :db/index true}
   :repo/bytes  {:db/valueType :db.type/long :db/index true}
   :repo/kind   {:db/valueType :db.type/keyword :db/index true}
   :repo/text   {:db/valueType :db.type/string :db/fulltext true}

   ;; -------------------------
   ;; TOPICS: per-message topic tags (separate from spine)
   ;; -------------------------
   :topic/message-id      {:db/unique :db.unique/identity
                           :db/valueType :db.type/string :db/index true}
   :topic/ts              {:db/valueType :db.type/instant :db/index true}
   :topic/guild-id        {:db/valueType :db.type/string  :db/index true}
   :topic/channel-id      {:db/valueType :db.type/string  :db/index true}
   :topic/model           {:db/valueType :db.type/string  :db/index true}
   :topic/topic           {:db/valueType :db.type/string
                           :db/cardinality :db.cardinality/many
                           :db/index true}})

(defonce ^:private conn* (atom nil))

(defn ensure-conn!
  []
  (or @conn*
      (let [dir (io/file db-path)]
        (.mkdirs dir)
        (reset! conn* (d/create-conn db-path schema)))))

(defn close! []
  (when-let [c @conn*]
    (try (d/close c) (catch Throwable _))
    (reset! conn* nil))
  true)

(defn db []
  (d/db (ensure-conn!)))

(defn- parse-discord-ts->date [s]
  (when (and (string? s) (seq s))
    (try (java.util.Date/from (java.time.Instant/parse s))
         (catch Throwable _ nil))))

(defn- normalize-author [msg]
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

(defn- drop-nils [m]
  (into {} (remove (comp nil? val)) m))

(defn- now-date [] (java.util.Date.))

;; -----------------------------------------------------------------------------
;; REPO helpers (no dependency on pigeonbot.repo)
;; -----------------------------------------------------------------------------

(defn- repo-eid-by-path
  "Return eid for a repo file by path, or nil."
  [dbv path]
  (ffirst (d/q '[:find ?e :in $ ?p :where [?e :repo/path ?p]]
               dbv (str path))))

(defn- repo-sha-by-path
  "Return sha for a repo file by path, or nil."
  [dbv path]
  (ffirst (d/q '[:find ?sha :in $ ?p :where [?e :repo/path ?p] [?e :repo/sha ?sha]]
               dbv (str path))))

(defn pull-repo-file
  "Pull basic repo file fields by eid."
  [dbv eid]
  (try
    (d/pull dbv
            [:repo/path :repo/sha :repo/ts :repo/bytes :repo/kind :repo/text]
            eid)
    (catch Throwable _ nil)))

(defn repo-fulltext
  "Full-text search over :repo/text only.

  Datalevin fulltext can return hits across all fulltext attrs, so we filter
  the returned datoms to :repo/text here."
  ([query] (repo-fulltext query {}))
  ([query opts]
   (let [hits (d/q '[:find ?e ?a ?v
                     :in $ ?q ?opts
                     :where [(fulltext $ ?q ?opts) [[?e ?a ?v]]]]
                   (db) query opts)]
     (->> hits
          (filter (fn [[_ a _]] (= a :repo/text)))
          vec))))

(defn repo-all
  "Return vector of repo file maps {:repo/path :repo/text :repo/sha :repo/kind :repo/bytes}."
  []
  (let [dbv (db)]
    (->> (d/q '[:find ?e
                :where
                [?e :repo/path]]
              dbv)
         (map first)
         (map (fn [eid] (pull-repo-file dbv eid)))
         (remove nil?)
         vec)))

(defn repo-search
  "Repo-only search by scanning repo texts with token scoring (no Datalevin fulltext needed).

  Returns vector of:
    {:path \"src/...\" :sha \"...\" :kind :clj :bytes N :snippet \"...\" :score K :matched [..]}

  Options:
  - :limit (default 6)
  - :snippet-chars (default 900)
  - :min-score (default 1)"
  ([query] (repo-search query {}))
  ([query {:keys [limit snippet-chars min-score]
           :or {limit 6 snippet-chars 900 min-score 1}}]
   (let [q (-> (or query "") str str/trim)]
     (if (str/blank? q)
       []
       (let [tokens (->> (re-seq #"[a-z0-9\-_]{2,}" (str/lower-case q))
                         (map #(str/replace % #"^_+|_+$" ""))
                         (remove str/blank?)
                         distinct
                         vec)
             files (repo-all)]
         (->> files
              (map (fn [{:repo/keys [path sha kind text bytes]}]
                     (let [hay (str/lower-case (or text ""))
                           matched (->> tokens
                                        (filter #(not= -1 (.indexOf ^String hay ^String %)))
                                        vec)
                           score (count matched)]
                       (when (>= score (long min-score))
                         (let [tok (first matched)
                               idx (.indexOf ^String hay ^String tok)
                               start (max 0 (- idx (quot snippet-chars 4)))
                               end   (min (count (or text "")) (+ start snippet-chars))
                               snippet (subs (or text "") start end)]
                           {:path path
                            :sha sha
                            :kind kind
                            :bytes bytes
                            :snippet snippet
                            :score score
                            :matched matched})))))
              (remove nil?)
              (sort-by (fn [m] [(- (:score m))
                                (long (or (:bytes m) Long/MAX_VALUE))
                                (:path m)]))
              (take limit)
              vec))))))

(defn repo-manifest
  "Return a lightweight manifest of indexed repo files."
  ([] (repo-manifest 60))
  ([limit]
   (let [dbv (db)]
     (->> (d/q '[:find ?path ?bytes ?kind ?ts
                 :where
                 [?e :repo/path ?path]
                 [?e :repo/bytes ?bytes]
                 [?e :repo/kind ?kind]
                 [?e :repo/ts ?ts]]
               dbv)
          (sort-by (fn [[path _ _ _]] path))
          (take (long limit))
          (map (fn [[path bytes kind ts]]
                 {:path path :bytes (long bytes) :kind kind :ts (str ts)}))
          vec))))

(defn upsert-repo-file!
  "Upsert a repo file entity by :repo/path.
  Returns true if inserted/updated, false if unchanged (sha match)."
  [{:keys [repo/path repo/text repo/sha repo/bytes repo/kind] :as m}]
  (let [conn (ensure-conn!)
        dbv  (d/db conn)
        path (some-> (:repo/path m) str)
        text (some-> (:repo/text m) str)
        sha  (some-> (:repo/sha m) str)]
    (when (and (seq path) (seq text) (seq sha))
      (let [old (repo-sha-by-path dbv path)]
        (if (= old sha)
          false
          (let [eid (or (repo-eid-by-path dbv path) (d/tempid :db.part/user))
                ent (drop-nils
                     {:db/id      eid
                      :repo/path  path
                      :repo/sha   sha
                      :repo/ts    (now-date)
                      :repo/bytes (long (or (:repo/bytes m) (count text)))
                      :repo/kind  (or (:repo/kind m) :txt)
                      :repo/text  text})]
            (d/transact! conn [ent])
            true))))))

;; -----------------------------------------------------------------------------
;; SPINE writes
;; -----------------------------------------------------------------------------

(defn message->tx [{:keys [id timestamp guild-id channel-id channel-type content] :as msg}]
  (let [{:keys [author-name author-id bot?]} (normalize-author msg)
        mid (some-> id str)
        ts  (or (parse-discord-ts->date (str timestamp)) (now-date))
        content (-> (or content "") (str/replace #"\u0000" "") str/trim)]
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

(defn upsert-message! [msg]
  (when-let [tx (message->tx msg)]
    (d/transact! (ensure-conn!) [tx])
    true))

;; -----------------------------------------------------------------------------
;; META writes
;; -----------------------------------------------------------------------------

(defn- message-reference-id [msg]
  (or (get-in msg [:message_reference :message_id])
      (get-in msg [:message_reference :message-id])
      (get-in msg [:message-reference :message_id])
      (get-in msg [:message-reference :message-id])
      (get-in msg [:referenced_message :id])))

(defn- looks-like-command? [content]
  (and (string? content) (str/starts-with? (str/triml content) "!")))

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

(defn message-meta->tx [{:keys [id timestamp guild-id channel-id content] :as msg}]
  (let [{:keys [author-id bot?]} (normalize-author msg)
        mid (some-> id str)
        ts  (or (parse-discord-ts->date (str timestamp)) (now-date))
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

(defn upsert-message-meta! [msg]
  (when-let [tx (message-meta->tx msg)]
    (d/transact! (ensure-conn!) [tx])
    true))

;; -----------------------------------------------------------------------------
;; EXTRACT writes (dedup + correct tag storage)
;; -----------------------------------------------------------------------------

(defn- normalize-tags [xs]
  (->> (or xs [])
       (map (fn [t] (-> (str t) str/lower-case str/trim)))
       (remove str/blank?)
       distinct
       vec))

(defn- normalize-extract-text [s]
  (-> (or s "")
      (str/replace #"\s+" " ")
      str/trim
      str/lower-case))

(defn- sha1-hex ^String [^String s]
  (let [md (java.security.MessageDigest/getInstance "SHA-1")
        bs (.digest md (.getBytes s "UTF-8"))]
    (apply str (map (fn [^Byte b] (format "%02x" (bit-and 0xff b))) bs))))

(defn- extract-hash
  "Stable dedupe key: guild + channel + kind + normalized text."
  [{:keys [guild-id channel-id kind text]}]
  (sha1-hex (str (or guild-id "") "|"
                 (or channel-id "") "|"
                 (name (or kind :note)) "|"
                 (normalize-extract-text text))))

(defn- find-extract-eid-by-hash [dbv h]
  (ffirst (d/q '[:find ?e :in $ ?h :where [?e :extract/hash ?h]]
               dbv (str h))))

(defn- existing-extract-tags [dbv eid]
  (set (map first
            (d/q '[:find ?t :in $ ?e :where [?e :extract/tag ?t]]
                 dbv eid))))

(defn extract-item->txdata
  "Return tx-data for one extract:
  - If new: entity map + :extract/tag datoms
  - If dup: bump seen-count + last-seen, add missing tags"
  [ctx item]
  (let [{:keys [guild-id channel-id message-id packet-id model]} ctx
        item-map (cond
                   (string? item) {:text item :kind :note :confidence 0.75 :tags []}
                   (map? item) item
                   :else nil)]
    (when item-map
      (let [txt (some-> (or (:text item-map) (:content item-map)
                            (:extract/text item-map) (:extract/content item-map))
                        str str/trim)
            kind (or (:kind item-map) (:extract/kind item-map) :note)
            conf (double (or (:confidence item-map) (:extract/confidence item-map) 0.75))
            scope (or (:scope item-map) (:extract/scope item-map) :channel)
            tags (normalize-tags (or (:tags item-map) (:extract/tags item-map) []))]
        (when (seq txt)
          (let [gid (some-> guild-id str)
                cid (some-> channel-id str)
                h   (extract-hash {:guild-id gid :channel-id cid :kind kind :text txt})
                conn (ensure-conn!)
                dbv  (d/db conn)
                existing (find-extract-eid-by-hash dbv h)
                now (now-date)]
            (if existing
              (let [have (existing-extract-tags dbv existing)
                    missing (remove have tags)
                    ops (concat
                         [[:db/add existing :extract/last-seen now]
                          [:db/add existing :extract/seen-count 1]
                          [:db/add existing :extract/message-id (some-> message-id str)]
                          [:db/add existing :extract/packet-id (some-> packet-id str)]
                          [:db/add existing :extract/model (or model "openai")]]
                         (for [t missing] [:db/add existing :extract/tag t]))]
                (vec ops))

              (let [eid (d/tempid :db.part/user)
                    ent (drop-nils
                         {:db/id             eid
                          :extract/id         (str (java.util.UUID/randomUUID))
                          :extract/hash       h
                          :extract/ts         now
                          :extract/last-seen  now
                          :extract/seen-count 1
                          :extract/text       txt
                          :extract/kind       kind
                          :extract/confidence conf
                          :extract/scope      scope
                          :extract/guild-id   gid
                          :extract/channel-id cid
                          :extract/message-id (some-> message-id str)
                          :extract/packet-id  (some-> packet-id str)
                          :extract/model      (or model "openai")})
                    tag-ops (for [t tags] [:db/add eid :extract/tag t])]
                (vec (concat [ent] tag-ops))))))))))

(defn upsert-extracts!
  "Write a vector of extract items into the DB as separate entities.
  De-dupes by :extract/hash; repeats bump seen-count + last-seen."
  [ctx extracts]
  (let [txdata (->> (or extracts [])
                    (map (partial extract-item->txdata ctx))
                    (remove nil?)
                    (mapcat identity)
                    vec)]
    (when (seq txdata)
      (d/transact! (ensure-conn!) txdata)
      true)))

;; -----------------------------------------------------------------------------
;; TOPICS: per-message topic tags (separate from spine)
;; -----------------------------------------------------------------------------

(defn- normalize-topics [xs]
  (->> (or xs [])
       (map (fn [t] (-> (str t) str/lower-case str/trim)))
       (remove str/blank?)
       distinct
       (take 12)
       vec))

(defn upsert-message-topics!
  [{:keys [message-id guild-id channel-id model topics]}]
  (let [conn (ensure-conn!)
        dbv (d/db conn)
        mid (some-> message-id str)
        model (or model "ollama")
        topics (normalize-topics topics)]
    (when (and (seq mid) (seq topics))
      (let [eid (or (ffirst (d/q '[:find ?e :in $ ?mid :where [?e :topic/message-id ?mid]]
                                 dbv mid))
                    (d/tempid :db.part/user))
            ent (drop-nils {:db/id            eid
                            :topic/message-id mid
                            :topic/ts         (now-date)
                            :topic/guild-id   (some-> guild-id str)
                            :topic/channel-id (some-> channel-id str)
                            :topic/model      (str model)})
            ops (for [t topics] [:db/add eid :topic/topic t])]
        (d/transact! conn (vec (concat [ent] ops)))
        true))))

(defn topic-exists? [mid]
  (let [dbv (db)]
    (boolean (ffirst (d/q '[:find ?e :in $ ?mid :where [?e :topic/message-id ?mid]]
                          dbv (str mid))))))

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

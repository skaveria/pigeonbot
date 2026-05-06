(ns pigeonbot.db
  (:require [clojure.java.io :as io]
            [clojure.string :as str])
  (:import (java.sql Connection DriverManager PreparedStatement ResultSet Statement)
           (java.time Instant)
           (java.time.format DateTimeFormatter)
           (java.util Date UUID)
           (java.security MessageDigest)))

;; -----------------------------------------------------------------------------
;; SQLite config
;; -----------------------------------------------------------------------------
(declare ensure-conn!)
(def ^:private db-path "./data/pigeonbot.sqlite3")
(def ^:private backup-dir "./data/backups")
(def ^:private backup-every-ms (* 6 60 60 1000)) ;; 6 hours

(defonce ^:private conn* (atom nil))
(defonce ^:private last-backup-ms* (atom 0))

(defn- now-ms [] (System/currentTimeMillis))

(defn- now-iso []
  (str (Instant/now)))

(defn- date->iso [x]
  (cond
    (nil? x) nil
    (instance? Date x) (str (.toInstant ^Date x))
    (instance? Instant x) (str x)
    :else (str x)))

(defn- parse-ts [s]
  (or (some-> s str not-empty)
      (now-iso)))

(defn- ensure-parent! [path]
  (when-let [p (.getParentFile (io/file path))]
    (.mkdirs p)))

(defn- backup-path []
  (let [stamp (.format (DateTimeFormatter/ofPattern "yyyyMMdd-HHmmss")
                       (java.time.LocalDateTime/now))]
    (str backup-dir "/pigeonbot-" stamp ".sqlite3")))

(defn backup-now!
  "Copy the SQLite DB to ./data/backups. Best-effort, safe to call anytime."
  []
  (try
    (ensure-parent! (str backup-dir "/x"))
    (let [src (io/file db-path)
          dst (io/file (backup-path))]
      (when (.exists src)
        (io/copy src dst)
        (reset! last-backup-ms* (now-ms))
        (println "[pigeonbot.db] SQLite backup:" (.getPath dst))
        (.getPath dst)))
    (catch Throwable t
      (println "[pigeonbot.db] backup failed:" (.getMessage t))
      nil)))

(defn maybe-backup!
  []
  (when (>= (- (now-ms) (long @last-backup-ms*)) backup-every-ms)
    (backup-now!)))

;; -----------------------------------------------------------------------------
;; JDBC helpers
;; -----------------------------------------------------------------------------

(defn- connect! []
  (ensure-parent! db-path)
  (Class/forName "org.sqlite.JDBC")
  (let [c (DriverManager/getConnection (str "jdbc:sqlite:" db-path))]
    (.setAutoCommit c true)
    c))

(defn- execute!
  [^Connection c sql]
  (with-open [st (.createStatement c)]
    (.execute st sql)))

(defn- set-param!
  [^PreparedStatement ps i v]
  (cond
    (nil? v) (.setObject ps i nil)
    (keyword? v) (.setString ps i (name v))
    (boolean? v) (.setBoolean ps i v)
    (integer? v) (.setLong ps i (long v))
    (number? v) (.setDouble ps i (double v))
    :else (.setString ps i (str v))))

(defn- exec!
  [sql params]
  (let [^Connection c (ensure-conn!)]
    (with-open [ps (.prepareStatement c sql)]
      (doseq [[i v] (map-indexed vector params)]
        (set-param! ps (inc i) v))
      (.executeUpdate ps))))

(defn- query
  [sql params row-fn]
  (let [^Connection c (ensure-conn!)]
    (with-open [ps (.prepareStatement c sql)]
      (doseq [[i v] (map-indexed vector params)]
        (set-param! ps (inc i) v))
      (with-open [rs (.executeQuery ps)]
        (loop [acc []]
          (if (.next rs)
            (recur (conj acc (row-fn rs)))
            acc))))))

(defn- scalar
  [sql params]
  (first (query sql params (fn [^ResultSet rs] (.getObject rs 1)))))

(defn- bool-int [x]
  (if x 1 0))

(defn- nil-if-blank [s]
  (let [s (some-> s str str/trim)]
    (when (seq s) s)))

(defn- csv-join [xs]
  (->> (or xs [])
       (map str)
       (remove str/blank?)
       distinct
       (str/join "\n")))

;; -----------------------------------------------------------------------------
;; Schema
;; -----------------------------------------------------------------------------

(defn- create-schema!
  [^Connection c]
  (doseq [sql
          ["PRAGMA journal_mode=WAL;"
           "PRAGMA synchronous=NORMAL;"
           "PRAGMA busy_timeout=5000;"
           "PRAGMA foreign_keys=ON;"

           "CREATE TABLE IF NOT EXISTS messages (
              message_id TEXT PRIMARY KEY,
              ts TEXT NOT NULL,
              guild_id TEXT,
              channel_id TEXT,
              channel_type INTEGER,
              author_id TEXT,
              author_name TEXT,
              bot INTEGER NOT NULL DEFAULT 0,
              content TEXT NOT NULL DEFAULT ''
            );"

           "CREATE INDEX IF NOT EXISTS idx_messages_guild ON messages(guild_id);"
           "CREATE INDEX IF NOT EXISTS idx_messages_channel ON messages(channel_id);"
           "CREATE INDEX IF NOT EXISTS idx_messages_ts ON messages(ts);"
           "CREATE INDEX IF NOT EXISTS idx_messages_author ON messages(author_id);"

           "CREATE VIRTUAL TABLE IF NOT EXISTS messages_fts
              USING fts5(message_id UNINDEXED, content, tokenize='porter unicode61');"

           "CREATE TABLE IF NOT EXISTS message_meta (
              message_id TEXT PRIMARY KEY,
              ts TEXT NOT NULL,
              guild_id TEXT,
              channel_id TEXT,
              author_id TEXT,
              bot INTEGER NOT NULL DEFAULT 0,
              is_command INTEGER NOT NULL DEFAULT 0,
              has_attachments INTEGER NOT NULL DEFAULT 0,
              has_links INTEGER NOT NULL DEFAULT 0,
              reply_to_id TEXT,
              char_count INTEGER NOT NULL DEFAULT 0,
              word_count INTEGER NOT NULL DEFAULT 0,
              mention_ids TEXT,
              attachment_urls TEXT
            );"

           "CREATE INDEX IF NOT EXISTS idx_meta_guild ON message_meta(guild_id);"
           "CREATE INDEX IF NOT EXISTS idx_meta_channel ON message_meta(channel_id);"

           "CREATE TABLE IF NOT EXISTS extracts (
              extract_id TEXT PRIMARY KEY,
              hash TEXT UNIQUE NOT NULL,
              ts TEXT NOT NULL,
              last_seen TEXT NOT NULL,
              seen_count INTEGER NOT NULL DEFAULT 1,
              text TEXT NOT NULL,
              kind TEXT NOT NULL DEFAULT 'note',
              confidence REAL NOT NULL DEFAULT 0.75,
              scope TEXT NOT NULL DEFAULT 'channel',
              guild_id TEXT,
              channel_id TEXT,
              message_id TEXT,
              packet_id TEXT,
              model TEXT
            );"

           "CREATE INDEX IF NOT EXISTS idx_extracts_guild ON extracts(guild_id);"
           "CREATE INDEX IF NOT EXISTS idx_extracts_channel ON extracts(channel_id);"
           "CREATE INDEX IF NOT EXISTS idx_extracts_kind ON extracts(kind);"

           "CREATE VIRTUAL TABLE IF NOT EXISTS extracts_fts
              USING fts5(extract_id UNINDEXED, text, tokenize='porter unicode61');"

           "CREATE TABLE IF NOT EXISTS extract_tags (
              extract_id TEXT NOT NULL,
              tag TEXT NOT NULL,
              PRIMARY KEY (extract_id, tag)
            );"

           "CREATE INDEX IF NOT EXISTS idx_extract_tags_tag ON extract_tags(tag);"

           "CREATE TABLE IF NOT EXISTS repo_files (
              path TEXT PRIMARY KEY,
              sha TEXT NOT NULL,
              ts TEXT NOT NULL,
              bytes INTEGER NOT NULL DEFAULT 0,
              kind TEXT NOT NULL DEFAULT 'txt',
              text TEXT NOT NULL
            );"

           "CREATE INDEX IF NOT EXISTS idx_repo_kind ON repo_files(kind);"

           "CREATE VIRTUAL TABLE IF NOT EXISTS repo_fts
              USING fts5(path UNINDEXED, text, tokenize='porter unicode61');"

           "CREATE TABLE IF NOT EXISTS topics (
              message_id TEXT PRIMARY KEY,
              ts TEXT NOT NULL,
              guild_id TEXT,
              channel_id TEXT,
              model TEXT
            );"

           "CREATE INDEX IF NOT EXISTS idx_topics_guild ON topics(guild_id);"
           "CREATE INDEX IF NOT EXISTS idx_topics_channel ON topics(channel_id);"

           "CREATE TABLE IF NOT EXISTS topic_tags (
              message_id TEXT NOT NULL,
              topic TEXT NOT NULL,
              PRIMARY KEY (message_id, topic)
            );"

           "CREATE INDEX IF NOT EXISTS idx_topic_tags_topic ON topic_tags(topic);"]]
    (execute! c sql)))

(defn ensure-conn!
  []
  (or @conn*
      (let [c (connect!)]
        (create-schema! c)
        (reset! conn* c)
        (maybe-backup!)
        c)))

(defn close! []
  (when-let [^Connection c @conn*]
    (try (.close c) (catch Throwable _))
    (reset! conn* nil))
  true)

(defn db []
  (ensure-conn!))

;; -----------------------------------------------------------------------------
;; Discord message normalization
;; -----------------------------------------------------------------------------

(defn- normalize-author [msg]
  (let [a (:author msg)
        bot? (true? (:bot a))
        name (or (:global_name a)
                 (:username a)
                 (get-in a [:user :username])
                 "unknown")
        author-id (or (:id a)
                      (get-in a [:user :id]))]
    {:author-name (str name)
     :author-id (some-> author-id str)
     :bot? bot?}))

(defn- message-reference-id [msg]
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
       (map nil-if-blank)
       (remove nil?)
       distinct
       vec))

(defn- word-count [s]
  (let [s (-> (or s "") (str/replace #"\s+" " ") str/trim)]
    (if (str/blank? s) 0 (count (str/split s #" ")))))

(defn upsert-message!
  [{:keys [id timestamp guild-id channel-id channel-type content] :as msg}]
  (when-let [mid (some-> id str)]
    (let [{:keys [author-name author-id bot?]} (normalize-author msg)
          ts (parse-ts timestamp)
          content (-> (or content "") (str/replace #"\u0000" "") str/trim)]
      (exec!
       "INSERT INTO messages
          (message_id, ts, guild_id, channel_id, channel_type, author_id, author_name, bot, content)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(message_id) DO UPDATE SET
          ts=excluded.ts,
          guild_id=excluded.guild_id,
          channel_id=excluded.channel_id,
          channel_type=excluded.channel_type,
          author_id=excluded.author_id,
          author_name=excluded.author_name,
          bot=excluded.bot,
          content=excluded.content"
       [mid ts (some-> guild-id str) (some-> channel-id str)
        (when channel-type (long channel-type))
        (or author-id "") author-name (bool-int bot?) content])
      (exec! "DELETE FROM messages_fts WHERE message_id = ?" [mid])
      (exec! "INSERT INTO messages_fts(message_id, content) VALUES (?, ?)" [mid content])
      true)))

(defn upsert-message-meta!
  [{:keys [id timestamp guild-id channel-id content] :as msg}]
  (when-let [mid (some-> id str)]
    (let [{:keys [author-id bot?]} (normalize-author msg)
          ts (parse-ts timestamp)
          content (str (or content ""))
          a-urls (attachment-urls msg)]
      (exec!
       "INSERT INTO message_meta
          (message_id, ts, guild_id, channel_id, author_id, bot, is_command,
           has_attachments, has_links, reply_to_id, char_count, word_count,
           mention_ids, attachment_urls)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(message_id) DO UPDATE SET
          ts=excluded.ts,
          guild_id=excluded.guild_id,
          channel_id=excluded.channel_id,
          author_id=excluded.author_id,
          bot=excluded.bot,
          is_command=excluded.is_command,
          has_attachments=excluded.has_attachments,
          has_links=excluded.has_links,
          reply_to_id=excluded.reply_to_id,
          char_count=excluded.char_count,
          word_count=excluded.word_count,
          mention_ids=excluded.mention_ids,
          attachment_urls=excluded.attachment_urls"
       [mid ts (some-> guild-id str) (some-> channel-id str)
        (or author-id "")
        (bool-int bot?)
        (bool-int (looks-like-command? content))
        (bool-int (seq a-urls))
        (bool-int (looks-like-link? content))
        (some-> (message-reference-id msg) str)
        (count content)
        (word-count content)
        (csv-join (mention-ids msg))
        (csv-join a-urls)])
      true)))

;; -----------------------------------------------------------------------------
;; Extracts
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
  (let [md (MessageDigest/getInstance "SHA-1")
        bs (.digest md (.getBytes s "UTF-8"))]
    (apply str (map (fn [^Byte b] (format "%02x" (bit-and 0xff b))) bs))))

(defn- extract-hash
  [{:keys [guild-id channel-id kind text]}]
  (sha1-hex (str (or guild-id "") "|"
                 (or channel-id "") "|"
                 (name (or kind :note)) "|"
                 (normalize-extract-text text))))

(defn- find-extract-by-hash [h]
  (scalar "SELECT extract_id FROM extracts WHERE hash = ?" [h]))

(defn- insert-extract-fts! [extract-id text]
  (exec! "DELETE FROM extracts_fts WHERE extract_id = ?" [extract-id])
  (exec! "INSERT INTO extracts_fts(extract_id, text) VALUES (?, ?)" [extract-id text]))

(defn- upsert-extract-one!
  [{:keys [guild-id channel-id message-id packet-id model]} item]
  (let [item-map (cond
                   (string? item) {:text item :kind :note :confidence 0.75 :tags []}
                   (map? item) item
                   :else nil)]
    (when item-map
      (let [txt (nil-if-blank (or (:text item-map)
                                  (:content item-map)
                                  (:extract/text item-map)
                                  (:extract/content item-map)))
            kind (or (:kind item-map) (:extract/kind item-map) :note)
            conf (double (or (:confidence item-map) (:extract/confidence item-map) 0.75))
            scope (or (:scope item-map) (:extract/scope item-map) :channel)
            tags (normalize-tags (or (:tags item-map) (:extract/tags item-map) []))]
        (when txt
          (let [gid (some-> guild-id str)
                cid (some-> channel-id str)
                h (extract-hash {:guild-id gid :channel-id cid :kind kind :text txt})
                now (now-iso)
                existing (find-extract-by-hash h)
                extract-id (or existing (str (UUID/randomUUID)))]
            (if existing
              (exec!
               "UPDATE extracts
                SET last_seen = ?,
                    seen_count = seen_count + 1,
                    message_id = ?,
                    packet_id = ?,
                    model = ?
                WHERE extract_id = ?"
               [now (some-> message-id str) (some-> packet-id str) (or model "openai") extract-id])
              (do
                (exec!
                 "INSERT INTO extracts
                    (extract_id, hash, ts, last_seen, seen_count, text, kind, confidence,
                     scope, guild_id, channel_id, message_id, packet_id, model)
                  VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"
                 [extract-id h now now 1 txt (name kind) conf (name scope)
                  gid cid (some-> message-id str) (some-> packet-id str) (or model "openai")])
                (insert-extract-fts! extract-id txt)))
            (doseq [t tags]
              (exec!
               "INSERT OR IGNORE INTO extract_tags(extract_id, tag) VALUES (?, ?)"
               [extract-id t]))
            true))))))

(defn upsert-extracts!
  [ctx extracts]
  (let [n (->> (or extracts [])
               (map (partial upsert-extract-one! ctx))
               (filter true?)
               count)]
    (when (pos? n)
      (maybe-backup!)
      true)))

;; -----------------------------------------------------------------------------
;; Topics
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
  (let [mid (some-> message-id str)
        topics (normalize-topics topics)]
    (when (and (seq mid) (seq topics))
      (exec!
       "INSERT INTO topics(message_id, ts, guild_id, channel_id, model)
        VALUES (?, ?, ?, ?, ?)
        ON CONFLICT(message_id) DO UPDATE SET
          ts=excluded.ts,
          guild_id=excluded.guild_id,
          channel_id=excluded.channel_id,
          model=excluded.model"
       [mid (now-iso) (some-> guild-id str) (some-> channel-id str) (str (or model "ollama"))])
      (doseq [t topics]
        (exec!
         "INSERT OR IGNORE INTO topic_tags(message_id, topic) VALUES (?, ?)"
         [mid t]))
      true)))

(defn topic-exists? [mid]
  (boolean (scalar "SELECT message_id FROM topics WHERE message_id = ? LIMIT 1" [(str mid)])))

;; -----------------------------------------------------------------------------
;; Repo
;; -----------------------------------------------------------------------------

(defn repo-all []
  (query
   "SELECT path, sha, ts, bytes, kind, text FROM repo_files"
   []
   (fn [^ResultSet rs]
     {:repo/path (.getString rs "path")
      :repo/sha (.getString rs "sha")
      :repo/ts (.getString rs "ts")
      :repo/bytes (.getLong rs "bytes")
      :repo/kind (keyword (.getString rs "kind"))
      :repo/text (.getString rs "text")})))

(defn upsert-repo-file!
  [{:keys [repo/path repo/text repo/sha repo/bytes repo/kind] :as m}]
  (let [path (nil-if-blank (:repo/path m))
        text (str (or (:repo/text m) ""))
        sha (nil-if-blank (:repo/sha m))]
    (when (and path sha (seq text))
      (let [old (scalar "SELECT sha FROM repo_files WHERE path = ?" [path])]
        (if (= old sha)
          false
          (do
            (exec!
             "INSERT INTO repo_files(path, sha, ts, bytes, kind, text)
              VALUES (?, ?, ?, ?, ?, ?)
              ON CONFLICT(path) DO UPDATE SET
                sha=excluded.sha,
                ts=excluded.ts,
                bytes=excluded.bytes,
                kind=excluded.kind,
                text=excluded.text"
             [path sha (now-iso) (long (or (:repo/bytes m) (count text)))
              (name (or (:repo/kind m) :txt)) text])
            (exec! "DELETE FROM repo_fts WHERE path = ?" [path])
            (exec! "INSERT INTO repo_fts(path, text) VALUES (?, ?)" [path text])
            true))))))

(defn repo-search
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
                         vec)]
         (->> (repo-all)
              (map (fn [{:repo/keys [path sha kind text bytes]}]
                     (let [hay (str/lower-case (or text ""))
                           matched (->> tokens
                                        (filter #(not= -1 (.indexOf ^String hay ^String %)))
                                        vec)
                           score (count matched)]
                       (when (>= score (long min-score))
                         (let [tok (first matched)
                               idx (max 0 (.indexOf ^String hay ^String tok))
                               start (max 0 (- idx (quot snippet-chars 4)))
                               end (min (count (or text "")) (+ start snippet-chars))
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
  ([] (repo-manifest 60))
  ([limit]
   (query
    "SELECT path, bytes, kind, ts FROM repo_files ORDER BY path LIMIT ?"
    [(long limit)]
    (fn [^ResultSet rs]
      {:path (.getString rs "path")
       :bytes (.getLong rs "bytes")
       :kind (keyword (.getString rs "kind"))
       :ts (.getString rs "ts")}))))

;; -----------------------------------------------------------------------------
;; Queries used by SLAP/backfills
;; -----------------------------------------------------------------------------

(defn count-messages []
  (or (scalar "SELECT count(*) FROM messages" []) 0))

(defn fulltext
  "Compatibility shape for old Datalevin callers:
  returns [[message-id :message/content content] ...] first, then extracts."
  ([q] (fulltext q {}))
  ([q opts]
   (let [limit (long (or (:limit opts) 50))
         q (str (or q ""))]
     (if (str/blank? q)
       []
       (vec
        (concat
         (query
          "SELECT m.message_id, m.content
           FROM messages_fts f
           JOIN messages m ON m.message_id = f.message_id
           WHERE messages_fts MATCH ?
           LIMIT ?"
          [q limit]
          (fn [^ResultSet rs]
            [(.getString rs "message_id") :message/content (.getString rs "content")]))
         (query
          "SELECT e.extract_id, e.text
           FROM extracts_fts f
           JOIN extracts e ON e.extract_id = f.extract_id
           WHERE extracts_fts MATCH ?
           LIMIT ?"
          [q limit]
          (fn [^ResultSet rs]
            [(.getString rs "extract_id") :extract/text (.getString rs "text")]))))))))

(defn pull-message
  [message-id]
  (first
   (query
    "SELECT * FROM messages WHERE message_id = ?"
    [(str message-id)]
    (fn [^ResultSet rs]
      {:message/id (.getString rs "message_id")
       :message/ts (.getString rs "ts")
       :message/guild-id (.getString rs "guild_id")
       :message/channel-id (.getString rs "channel_id")
       :message/channel-type (.getLong rs "channel_type")
       :message/author-id (.getString rs "author_id")
       :message/author-name (.getString rs "author_name")
       :message/bot? (pos? (.getInt rs "bot"))
       :message/content (.getString rs "content")}))))

(defn recent-channel-messages
  [channel-id limit]
  (query
   "SELECT * FROM messages
    WHERE channel_id = ?
    ORDER BY ts DESC
    LIMIT ?"
   [(str channel-id) (long limit)]
   (fn [^ResultSet rs]
     {:message/id (.getString rs "message_id")
      :message/ts (.getString rs "ts")
      :message/guild-id (.getString rs "guild_id")
      :message/channel-id (.getString rs "channel_id")
      :message/author-id (.getString rs "author_id")
      :message/author-name (.getString rs "author_name")
      :message/bot? (pos? (.getInt rs "bot"))
      :message/content (.getString rs "content")})))

(defn channel-message-rows
  "Rows shaped for extract backfill: [ts author content message-id]."
  [channel-id]
  (query
   "SELECT ts, author_name, content, message_id
    FROM messages
    WHERE channel_id = ?
    ORDER BY ts ASC"
   [(str channel-id)]
   (fn [^ResultSet rs]
     [(.getString rs "ts")
      (.getString rs "author_name")
      (.getString rs "content")
      (.getString rs "message_id")])))

(defn channel-topic-rows
  "Rows shaped for topic backfill: [message-id text guild-id channel-id]."
  [channel-id]
  (query
   "SELECT message_id, content, guild_id, channel_id
    FROM messages
    WHERE channel_id = ?
    ORDER BY ts ASC"
   [(str channel-id)]
   (fn [^ResultSet rs]
     [(.getString rs "message_id")
      (.getString rs "content")
      (.getString rs "guild_id")
      (.getString rs "channel_id")])))

(defn guild-channel-ids
  [guild-id]
  (query
   "SELECT DISTINCT channel_id FROM messages
    WHERE guild_id = ? AND channel_id IS NOT NULL
    ORDER BY channel_id"
   [(str guild-id)]
   (fn [^ResultSet rs] (.getString rs 1))))

(defn channel-message-count [channel-id]
  (or (scalar "SELECT count(*) FROM messages WHERE channel_id = ?" [(str channel-id)]) 0))

(defn channel-topic-count [channel-id]
  (or (scalar "SELECT count(*) FROM topics WHERE channel_id = ?" [(str channel-id)]) 0))

(defn channel-extract-count [channel-id]
  (or (scalar "SELECT count(*) FROM extracts WHERE channel_id = ?" [(str channel-id)]) 0))

(defn top-extract-tags
  ([guild-id] (top-extract-tags guild-id {}))
  ([guild-id {:keys [limit] :or {limit 25}}]
   (query
    "SELECT t.tag, count(*) AS n
     FROM extract_tags t
     JOIN extracts e ON e.extract_id = t.extract_id
     WHERE e.guild_id = ?
     GROUP BY t.tag
     ORDER BY n DESC, t.tag ASC
     LIMIT ?"
    [(str guild-id) (long limit)]
    (fn [^ResultSet rs] (.getString rs "tag")))))

(defn top-topic-tags
  ([guild-id] (top-topic-tags guild-id {}))
  ([guild-id {:keys [limit] :or {limit 25}}]
   (query
    "SELECT tt.topic, count(*) AS n
     FROM topic_tags tt
     JOIN topics t ON t.message_id = tt.message_id
     WHERE t.guild_id = ?
     GROUP BY tt.topic
     ORDER BY n DESC, tt.topic ASC
     LIMIT ?"
    [(str guild-id) (long limit)]
    (fn [^ResultSet rs] (.getString rs "topic")))))

(defn extracts-by-tag-overlap
  [guild-id tags limit]
  (let [tags (normalize-tags tags)]
    (if (empty? tags)
      []
      (let [placeholders (str/join "," (repeat (count tags) "?"))
            sql (str
                 "SELECT e.extract_id, e.ts, e.channel_id, e.text, e.kind, e.confidence,
                         count(t.tag) AS overlap,
                         group_concat(t.tag, ',') AS matched_tags
                  FROM extracts e
                  JOIN extract_tags t ON t.extract_id = e.extract_id
                  WHERE e.guild_id = ?
                    AND t.tag IN (" placeholders ")
                  GROUP BY e.extract_id
                  ORDER BY overlap DESC, e.ts DESC
                  LIMIT ?")]
        (query
         sql
         (vec (concat [(str guild-id)] tags [(long limit)]))
         (fn [^ResultSet rs]
           {:extract/eid (.getString rs "extract_id")
            :extract/ts (.getString rs "ts")
            :extract/channel-id (.getString rs "channel_id")
            :extract/text (.getString rs "text")
            :extract/kind (keyword (.getString rs "kind"))
            :extract/confidence (.getDouble rs "confidence")
            :extract/matched-tags (-> (.getString rs "matched_tags")
                                       (or "")
                                       (str/split #",")
                                       vec)
            :extract/overlap (.getLong rs "overlap")}))))))

(defn topic-related-message-rows
  [guild-id topics limit]
  (let [topics (normalize-topics topics)]
    (if (empty? topics)
      []
      (let [placeholders (str/join "," (repeat (count topics) "?"))
            sql (str
                 "SELECT DISTINCT m.ts, m.author_name, m.content, m.channel_id
                  FROM messages m
                  JOIN topics tp ON tp.message_id = m.message_id
                  JOIN topic_tags tt ON tt.message_id = tp.message_id
                  WHERE tp.guild_id = ?
                    AND tt.topic IN (" placeholders ")
                  ORDER BY m.ts DESC
                  LIMIT ?")]
        (query
         sql
         (vec (concat [(str guild-id)] topics [(long limit)]))
         (fn [^ResultSet rs]
           {:message/ts (.getString rs "ts")
            :message/author-name (.getString rs "author_name")
            :message/content (.getString rs "content")
            :message/channel-id (.getString rs "channel_id")}))))))



(defn search-messages
  "Compatibility helper for SLAP after SQLite migration.
  Searches SQLite FTS and returns pulled message maps."
  ([fts]
   (search-messages fts {}))
  ([fts {:keys [guild-id channel-id limit]
         :or {limit 25}}]
   (let [hits (fulltext fts {:limit limit})]
     (->> hits
          (keep (fn [[id attr _txt]]
                  (when (= attr :message/content)
                    (pull-message id))))
          (filter (fn [m]
                    (and
                     (or (nil? guild-id)
                         (= (str guild-id) (some-> (:message/guild-id m) str)))
                     (or (nil? channel-id)
                         (= (str channel-id) (some-> (:message/channel-id m) str))))))
          (take (long limit))
          vec))))

(defn recent-messages
  "Compatibility alias for older SLAP code.
  Returns newest-first message maps for a channel."
  [channel-id limit]
  (recent-channel-messages channel-id limit))

(defn slap-capabilities
  [{:keys [guild-id channel-id]}]
  {:scope {:guild/id (some-> guild-id str)
           :channel/id (some-> channel-id str)}
   :storage :sqlite
   :entities
   [{:entity :message
     :purpose "Raw Discord messages."
     :fulltext [:messages_fts/content]
     :indexed [:guild_id :channel_id :ts :author_name :bot]}
    {:entity :extract
     :purpose "Semantic memory extracts."
     :fulltext [:extracts_fts/text]
     :tags {:table :extract_tags :attr :tag}}
    {:entity :topic
     :purpose "Per-message topic tags."
     :tags {:table :topic_tags :attr :topic}}
    {:entity :repo
     :purpose "Indexed source files."
     :fulltext [:repo_fts/text]}]
   :query-back
   {:tool :datalevin/fts
    :note "Compatibility name only; backed by SQLite FTS5."
    :shape "{:query/id \"q1\" :tool :datalevin/fts :query {:fts \"...\"} :expected {:limit 10} :purpose \"...\" :priority 1}"}})

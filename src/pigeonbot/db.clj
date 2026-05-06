(ns pigeonbot.db
  (:require [clojure.java.io :as io]
            [clojure.string :as str]
            [pigeonbot.config :as config])
  (:import (java.sql DriverManager PreparedStatement)
           (java.time Instant)
           (java.util Date UUID)))

(defonce ^:private conn* (atom nil))
(defonce ^:private write-count* (atom 0))
(defonce ^:private last-backup-ms* (atom 0))

(defn- cfg []
  (let [m (config/load-config)]
    {:db-path (or (:sqlite-db-path m) "./data/pigeonbot.sqlite3")
     :backup-dir (or (:sqlite-backup-dir m) "./data/sqlite-backups")
     :backup-every-writes (long (or (:sqlite-backup-every-writes m) 5000))
     :backup-every-ms (long (or (:sqlite-backup-every-ms m) (* 30 60 1000)))}))

(defn- now-ms [] (System/currentTimeMillis))
(defn- now-iso [] (.toString (Instant/now)))

(defn- date->iso [x]
  (cond
    (instance? Date x) (.toString (.toInstant ^Date x))
    (instance? Instant x) (.toString ^Instant x)
    (string? x) x
    :else (now-iso)))

(defn- bool->int [x] (if x 1 0))
(defn- int->bool [x] (boolean (and x (not= 0 (long x)))))

(defn- set-param! [^PreparedStatement ps idx v]
  (cond
    (nil? v) (.setObject ps idx nil)
    (keyword? v) (.setString ps idx (name v))
    (instance? Date v) (.setString ps idx (date->iso v))
    (instance? Instant v) (.setString ps idx (date->iso v))
    (boolean? v) (.setLong ps idx (bool->int v))
    :else (.setObject ps idx v)))

(defn- exec!
  [sql & params]
  (let [c @conn*]
    (with-open [ps (.prepareStatement c sql)]
      (doseq [[idx v] (map-indexed vector params)]
        (set-param! ps (inc idx) v))
      (.executeUpdate ps))))

(defn- query!
  [sql & params]
  (let [c @conn*]
    (with-open [ps (.prepareStatement c sql)]
      (doseq [[idx v] (map-indexed vector params)]
        (set-param! ps (inc idx) v))
      (with-open [rs (.executeQuery ps)]
        (let [md (.getMetaData rs)
              n (.getColumnCount md)]
          (loop [out []]
            (if-not (.next rs)
              out
              (let [row (into {}
                              (for [i (range 1 (inc n))]
                                [(keyword (.getColumnLabel md i))
                                 (.getObject rs i)]))]
                (recur (conj out row))))))))))

(defn- scalar
  [sql & params]
  (some-> (apply query! sql params) first vals first))

(defn- mkdir-parent! [path]
  (when-let [p (.getParentFile (io/file path))]
    (.mkdirs p)))

(defn- init-schema! []
  (doseq [sql
          ["PRAGMA journal_mode=WAL"
           "PRAGMA synchronous=NORMAL"
           "PRAGMA busy_timeout=10000"
           "PRAGMA foreign_keys=ON"

           "CREATE TABLE IF NOT EXISTS messages (
              message_id TEXT PRIMARY KEY,
              ts TEXT,
              guild_id TEXT,
              channel_id TEXT,
              channel_type INTEGER,
              author_id TEXT,
              author_name TEXT,
              bot INTEGER DEFAULT 0,
              content TEXT DEFAULT ''
            )"

           "CREATE VIRTUAL TABLE IF NOT EXISTS message_fts
              USING fts5(message_id UNINDEXED, content)"

           "CREATE TABLE IF NOT EXISTS message_meta (
              message_id TEXT PRIMARY KEY,
              ts TEXT,
              guild_id TEXT,
              channel_id TEXT,
              author_id TEXT,
              bot INTEGER DEFAULT 0,
              is_command INTEGER DEFAULT 0,
              has_attachments INTEGER DEFAULT 0,
              has_links INTEGER DEFAULT 0,
              reply_to_id TEXT,
              char_count INTEGER DEFAULT 0,
              word_count INTEGER DEFAULT 0
            )"

           "CREATE TABLE IF NOT EXISTS meta_mentions (
              message_id TEXT,
              mention_id TEXT,
              PRIMARY KEY (message_id, mention_id)
            )"

           "CREATE TABLE IF NOT EXISTS meta_attachments (
              message_id TEXT,
              url TEXT,
              PRIMARY KEY (message_id, url)
            )"

           "CREATE TABLE IF NOT EXISTS extracts (
              extract_id TEXT PRIMARY KEY,
              hash TEXT UNIQUE,
              ts TEXT,
              last_seen TEXT,
              seen_count INTEGER DEFAULT 1,
              text TEXT DEFAULT '',
              kind TEXT DEFAULT 'note',
              confidence REAL DEFAULT 0.75,
              scope TEXT DEFAULT 'channel',
              guild_id TEXT,
              channel_id TEXT,
              message_id TEXT,
              packet_id TEXT,
              model TEXT
            )"

           "CREATE VIRTUAL TABLE IF NOT EXISTS extract_fts
              USING fts5(extract_id UNINDEXED, text)"

           "CREATE TABLE IF NOT EXISTS extract_tags (
              extract_id TEXT,
              tag TEXT,
              PRIMARY KEY (extract_id, tag)
            )"

           "CREATE TABLE IF NOT EXISTS topics (
              message_id TEXT PRIMARY KEY,
              ts TEXT,
              guild_id TEXT,
              channel_id TEXT,
              model TEXT
            )"

           "CREATE TABLE IF NOT EXISTS topic_tags (
              message_id TEXT,
              topic TEXT,
              PRIMARY KEY (message_id, topic)
            )"

           "CREATE TABLE IF NOT EXISTS repo_files (
              path TEXT PRIMARY KEY,
              sha TEXT,
              ts TEXT,
              bytes INTEGER DEFAULT 0,
              kind TEXT DEFAULT 'txt',
              text TEXT DEFAULT ''
            )"

           "CREATE VIRTUAL TABLE IF NOT EXISTS repo_fts
              USING fts5(path UNINDEXED, text)"

           "CREATE INDEX IF NOT EXISTS idx_messages_guild_channel ON messages(guild_id, channel_id)"
           "CREATE INDEX IF NOT EXISTS idx_extracts_guild_channel ON extracts(guild_id, channel_id)"
           "CREATE INDEX IF NOT EXISTS idx_topics_guild_channel ON topics(guild_id, channel_id)"
           "CREATE INDEX IF NOT EXISTS idx_extract_tags_tag ON extract_tags(tag)"
           "CREATE INDEX IF NOT EXISTS idx_topic_tags_topic ON topic_tags(topic)"]]
    (exec! sql)))

(defn ensure-conn! []
  (or @conn*
      (let [{:keys [db-path]} (cfg)]
        (Class/forName "org.sqlite.JDBC")
        (mkdir-parent! db-path)
        (reset! conn* (DriverManager/getConnection (str "jdbc:sqlite:" db-path)))
        (init-schema!)
        @conn*)))

(defn close! []
  (when-let [c @conn*]
    (try (.close c) (catch Throwable _))
    (reset! conn* nil))
  true)

(defn db []
  (ensure-conn!))

(defn backup! []
  (ensure-conn!)
  (let [{:keys [db-path backup-dir]} (cfg)
        ts (-> (now-iso)
               (str/replace #":" "")
               (str/replace #"\..*$" "")
               (str/replace #"T" "-"))
        src (io/file db-path)
        dst-dir (io/file backup-dir)
        dst (io/file dst-dir (str "pigeonbot-" ts ".sqlite3"))]
    (.mkdirs dst-dir)
    (try (exec! "PRAGMA wal_checkpoint(PASSIVE)") (catch Throwable _))
    (when (.exists src)
      (io/copy src dst))
    {:backup (str dst)}))

(defn- maybe-backup! []
  (let [{:keys [backup-every-writes backup-every-ms]} (cfg)
        wc (swap! write-count* inc)
        now (now-ms)
        last @last-backup-ms*]
    (when (or (zero? (mod wc backup-every-writes))
              (> (- now last) backup-every-ms))
      (reset! last-backup-ms* now)
      (try
        (backup!)
        (catch Throwable t
          (println "[pigeonbot.db] backup failed:" (.getMessage t)))))))

(defn- normalize-author [msg]
  (let [a (:author msg)
        name (or (:global_name a)
                 (:username a)
                 (get-in a [:user :username])
                 "unknown")
        author-id (or (:id a) (get-in a [:user :id]))]
    {:author-name (str name)
     :author-id (some-> author-id str)
     :bot? (true? (:bot a))}))

(defn- clean-content [s]
  (-> (or s "") str (str/replace #"\u0000" "") str/trim))

(defn upsert-message! [{:keys [id timestamp guild-id channel-id channel-type content] :as msg}]
  (ensure-conn!)
  (let [{:keys [author-name author-id bot?]} (normalize-author msg)
        mid (some-> id str)
        txt (clean-content content)]
    (when (seq mid)
      (exec! "INSERT INTO messages
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
             mid (date->iso timestamp) (some-> guild-id str) (some-> channel-id str)
             (when channel-type (long channel-type))
             (or author-id "") author-name bot? txt)
      (exec! "DELETE FROM message_fts WHERE message_id = ?" mid)
      (exec! "INSERT INTO message_fts(message_id, content) VALUES (?, ?)" mid txt)
      (maybe-backup!)
      true)))

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

(defn- word-count [s]
  (let [s (-> (or s "") (str/replace #"\s+" " ") str/trim)]
    (if (str/blank? s) 0 (count (str/split s #" ")))))

(defn- mention-ids [msg]
  (->> (or (:mentions msg) [])
       (keep (fn [m] (some-> (:id m) str)))
       distinct
       vec))

(defn- attachment-urls [msg]
  (->> (or (:attachments msg) [])
       (keep (fn [a] (some-> (or (:url a) (:proxy_url a) (:proxy-url a)) str str/trim not-empty)))
       distinct
       vec))

(defn upsert-message-meta! [{:keys [id timestamp guild-id channel-id content] :as msg}]
  (ensure-conn!)
  (let [{:keys [author-id bot?]} (normalize-author msg)
        mid (some-> id str)
        content (str (or content ""))]
    (when (seq mid)
      (exec! "INSERT INTO message_meta
              (message_id, ts, guild_id, channel_id, author_id, bot, is_command, has_attachments,
               has_links, reply_to_id, char_count, word_count)
              VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
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
                word_count=excluded.word_count"
             mid (date->iso timestamp) (some-> guild-id str) (some-> channel-id str)
             (or author-id "") bot? (looks-like-command? content)
             (boolean (seq (attachment-urls msg))) (looks-like-link? content)
             (some-> (message-reference-id msg) str)
             (count content) (word-count content))
      (exec! "DELETE FROM meta_mentions WHERE message_id = ?" mid)
      (doseq [x (mention-ids msg)]
        (exec! "INSERT OR IGNORE INTO meta_mentions(message_id, mention_id) VALUES (?, ?)" mid x))
      (exec! "DELETE FROM meta_attachments WHERE message_id = ?" mid)
      (doseq [x (attachment-urls msg)]
        (exec! "INSERT OR IGNORE INTO meta_attachments(message_id, url) VALUES (?, ?)" mid x))
      true)))

(defn count-messages []
  (ensure-conn!)
  (or (scalar "SELECT COUNT(*) FROM messages") 0))

(defn message-count-by-channel [channel-id]
  (ensure-conn!)
  (or (scalar "SELECT COUNT(*) FROM messages WHERE channel_id = ?" (str channel-id)) 0))

(defn topic-count-by-channel [channel-id]
  (ensure-conn!)
  (or (scalar "SELECT COUNT(*) FROM topics WHERE channel_id = ?" (str channel-id)) 0))

(defn extract-count-by-channel [channel-id]
  (ensure-conn!)
  (or (scalar "SELECT COUNT(*) FROM extracts WHERE channel_id = ?" (str channel-id)) 0))

(defn guild-channel-ids [guild-id]
  (ensure-conn!)
  (->> (query! "SELECT DISTINCT channel_id FROM messages WHERE guild_id = ? ORDER BY channel_id"
               (str guild-id))
       (map :channel_id)
       vec))

(defn message-rows-in-channel [channel-id]
  (ensure-conn!)
  (->> (query! "SELECT message_id, content, guild_id, channel_id
                FROM messages
                WHERE channel_id = ?
                ORDER BY ts ASC, message_id ASC"
               (str channel-id))
       (map (fn [m] [(:message_id m) (:content m) (:guild_id m) (:channel_id m)]))
       vec))

(defn channel-transcript-rows [channel-id]
  (ensure-conn!)
  (->> (query! "SELECT ts, author_name, content, message_id
                FROM messages
                WHERE channel_id = ?
                ORDER BY ts ASC, message_id ASC"
               (str channel-id))
       (map (fn [m] [(:ts m) (:author_name m) (:content m) (:message_id m)]))
       vec))

(defn recent-messages
  ([channel-id] (recent-messages channel-id 12))
  ([channel-id limit]
   (ensure-conn!)
   (->> (query! "SELECT message_id, ts, author_name, content, channel_id
                 FROM messages
                 WHERE channel_id = ?
                 ORDER BY ts DESC, message_id DESC
                 LIMIT ?"
                (str channel-id) (long limit))
        reverse
        (mapv (fn [m]
                {:message/id (:message_id m)
                 :ts (:ts m)
                 :author/name (:author_name m)
                 :content (:content m)
                 :channel/id (:channel_id m)})))))

(defn pull-message-by-id [message-id]
  (ensure-conn!)
  (when-let [m (first (query! "SELECT * FROM messages WHERE message_id = ?" (str message-id)))]
    {:message/id (:message_id m)
     :message/ts (:ts m)
     :message/guild-id (:guild_id m)
     :message/channel-id (:channel_id m)
     :message/channel-type (:channel_type m)
     :message/author-id (:author_id m)
     :message/author-name (:author_name m)
     :message/bot? (int->bool (:bot m))
     :message/content (:content m)}))

(defn- like-search-messages [q {:keys [guild-id channel-id limit]}]
  (let [terms (->> (re-seq #"[A-Za-z0-9_\-]{2,}" (str q))
                   (map str/lower-case)
                   distinct
                   (take 6)
                   vec)
        clauses (concat
                 ["1=1"]
                 (when (seq guild-id) ["guild_id = ?"])
                 (when (seq channel-id) ["channel_id = ?"])
                 (for [_ terms] "lower(content) LIKE ?"))
        params (concat
                (when (seq guild-id) [(str guild-id)])
                (when (seq channel-id) [(str channel-id)])
                (for [t terms] (str "%" t "%"))
                [(long (or limit 10))])]
    (apply query!
           (str "SELECT * FROM messages WHERE "
                (str/join " AND " clauses)
                " ORDER BY ts DESC LIMIT ?")
           params)))

(defn search-messages
  ([q] (search-messages q {}))
  ([q {:keys [guild-id channel-id limit] :or {limit 10} :as opts}]
   (ensure-conn!)
   (let [q (str q)]
     (try
       (let [clauses (concat
                      ["message_fts MATCH ?"]
                      (when (seq guild-id) ["m.guild_id = ?"])
                      (when (seq channel-id) ["m.channel_id = ?"]))
             params (concat
                     [q]
                     (when (seq guild-id) [(str guild-id)])
                     (when (seq channel-id) [(str channel-id)])
                     [(long limit)])]
         (->> (apply query!
                     (str "SELECT m.*
                           FROM message_fts f
                           JOIN messages m ON m.message_id = f.message_id
                           WHERE " (str/join " AND " clauses) "
                           ORDER BY bm25(f)
                           LIMIT ?")
                     params)
              (mapv (fn [m]
                      {:message/id (:message_id m)
                       :message/ts (:ts m)
                       :message/guild-id (:guild_id m)
                       :message/channel-id (:channel_id m)
                       :message/author-id (:author_id m)
                       :message/author-name (:author_name m)
                       :message/bot? (int->bool (:bot m))
                       :message/content (:content m)}))))
       (catch Throwable _
         (->> (like-search-messages q opts)
              (mapv (fn [m]
                      {:message/id (:message_id m)
                       :message/ts (:ts m)
                       :message/guild-id (:guild_id m)
                       :message/channel-id (:channel_id m)
                       :message/author-id (:author_id m)
                       :message/author-name (:author_name m)
                       :message/bot? (int->bool (:bot m))
                       :message/content (:content m)}))))))))

(defn fulltext
  "Legacy-ish helper. Prefer search-messages."
  ([query] (fulltext query {}))
  ([query opts]
   (->> (search-messages query opts)
        (mapv (fn [m] [(:message/id m) :message/content (:message/content m)])))))

(defn- normalize-tags [xs]
  (->> (or xs [])
       (map #(-> (str %) str/lower-case str/trim))
       (remove str/blank?)
       distinct
       vec))

(defn- sha1-hex ^String [^String s]
  (let [md (java.security.MessageDigest/getInstance "SHA-1")
        bs (.digest md (.getBytes s "UTF-8"))]
    (apply str (map #(format "%02x" (bit-and 0xff %)) bs))))

(defn- normalize-extract-text [s]
  (-> (or s "") (str/replace #"\s+" " ") str/trim str/lower-case))

(defn- extract-hash [{:keys [guild-id channel-id kind text]}]
  (sha1-hex (str (or guild-id "") "|" (or channel-id "") "|"
                 (name (or kind :note)) "|" (normalize-extract-text text))))

(defn upsert-extracts! [ctx extracts]
  (ensure-conn!)
  (let [{:keys [guild-id channel-id message-id packet-id model]} ctx
        now (now-iso)]
    (doseq [item (or extracts [])]
      (let [m (cond
                (string? item) {:text item :kind :note :confidence 0.75 :tags []}
                (map? item) item
                :else nil)
            txt (some-> (or (:text m) (:content m) (:extract/text m)) str str/trim)]
        (when (seq txt)
          (let [kind (or (:kind m) (:extract/kind m) :note)
                conf (double (or (:confidence m) (:extract/confidence m) 0.75))
                scope (or (:scope m) (:extract/scope m) :channel)
                tags (normalize-tags (or (:tags m) (:extract/tags m) []))
                h (extract-hash {:guild-id guild-id :channel-id channel-id :kind kind :text txt})
                existing (scalar "SELECT extract_id FROM extracts WHERE hash = ?" h)
                eid (or existing (str (UUID/randomUUID)))]
            (if existing
              (exec! "UPDATE extracts
                      SET last_seen = ?, seen_count = seen_count + 1,
                          message_id = ?, packet_id = ?, model = ?
                      WHERE extract_id = ?"
                     now (some-> message-id str) (some-> packet-id str) (or model "openai") eid)
              (do
                (exec! "INSERT INTO extracts
                        (extract_id, hash, ts, last_seen, seen_count, text, kind, confidence, scope,
                         guild_id, channel_id, message_id, packet_id, model)
                        VALUES (?, ?, ?, ?, 1, ?, ?, ?, ?, ?, ?, ?, ?, ?)"
                       eid h now now txt (name kind) conf (name scope)
                       (some-> guild-id str) (some-> channel-id str)
                       (some-> message-id str) (some-> packet-id str) (or model "openai"))
                (exec! "INSERT INTO extract_fts(extract_id, text) VALUES (?, ?)" eid txt)))
            (doseq [t tags]
              (exec! "INSERT OR IGNORE INTO extract_tags(extract_id, tag) VALUES (?, ?)" eid t))))))
    (maybe-backup!)
    true))

(defn related-extracts-by-tags [guild-id tags {:keys [limit] :or {limit 20}}]
  (ensure-conn!)
  (if (or (not (seq (str guild-id))) (empty? tags))
    []
    (let [placeholders (str/join "," (repeat (count tags) "?"))
          rows (apply query!
                      (str "SELECT e.extract_id, e.ts, e.kind, e.confidence, e.channel_id, e.text,
                                   COUNT(t.tag) AS overlap,
                                   GROUP_CONCAT(t.tag) AS matched_tags
                            FROM extracts e
                            JOIN extract_tags t ON t.extract_id = e.extract_id
                            WHERE e.guild_id = ?
                              AND t.tag IN (" placeholders ")
                            GROUP BY e.extract_id
                            ORDER BY overlap DESC, e.ts DESC
                            LIMIT ?")
                      (concat [(str guild-id)] tags [(long limit)]))]
      (mapv (fn [r]
              {:extract/eid (:extract_id r)
               :extract/ts (:ts r)
               :extract/channel-id (:channel_id r)
               :extract/text (:text r)
               :extract/kind (keyword (or (:kind r) "note"))
               :extract/confidence (double (or (:confidence r) 0.75))
               :extract/matched-tags (-> (or (:matched_tags r) "")
                                         (str/split #",")
                                         vec)
               :extract/overlap (long (or (:overlap r) 0))})
            rows))))

(defn messages-by-topics [guild-id topics {:keys [limit] :or {limit 20}}]
  (ensure-conn!)
  (if (or (not (seq (str guild-id))) (empty? topics))
    []
    (let [placeholders (str/join "," (repeat (count topics) "?"))
          rows (apply query!
                      (str "SELECT m.ts, m.author_name, m.content, m.channel_id, m.message_id
                            FROM messages m
                            JOIN topic_tags tt ON tt.message_id = m.message_id
                            WHERE m.guild_id = ?
                              AND tt.topic IN (" placeholders ")
                            GROUP BY m.message_id
                            ORDER BY m.ts DESC
                            LIMIT ?")
                      (concat [(str guild-id)] topics [(long limit)]))]
      (->> rows
           reverse
           (mapv (fn [r]
                   {:message/ts (:ts r)
                    :message/author-name (:author_name r)
                    :message/content (:content r)
                    :message/channel-id (:channel_id r)
                    :message/id (:message_id r)}))))))

(defn topic-exists? [mid]
  (ensure-conn!)
  (boolean (scalar "SELECT message_id FROM topics WHERE message_id = ?" (str mid))))

(defn upsert-message-topics! [{:keys [message-id guild-id channel-id model topics]}]
  (ensure-conn!)
  (let [mid (some-> message-id str)
        topics (->> topics
                    (map #(-> (str %) str/lower-case str/trim))
                    (remove str/blank?)
                    distinct
                    (take 12)
                    vec)]
    (when (and (seq mid) (seq topics))
      (exec! "INSERT INTO topics(message_id, ts, guild_id, channel_id, model)
              VALUES (?, ?, ?, ?, ?)
              ON CONFLICT(message_id) DO UPDATE SET
                ts=excluded.ts,
                guild_id=excluded.guild_id,
                channel_id=excluded.channel_id,
                model=excluded.model"
             mid (now-iso) (some-> guild-id str) (some-> channel-id str) (or model "ollama"))
      (doseq [t topics]
        (exec! "INSERT OR IGNORE INTO topic_tags(message_id, topic) VALUES (?, ?)" mid t))
      (maybe-backup!)
      true)))

(defn top-extract-tags
  ([guild-id] (top-extract-tags guild-id {}))
  ([guild-id {:keys [limit] :or {limit 25}}]
   (ensure-conn!)
   (->> (query! "SELECT t.tag, COUNT(*) AS n
                 FROM extract_tags t
                 JOIN extracts e ON e.extract_id = t.extract_id
                 WHERE e.guild_id = ?
                 GROUP BY t.tag
                 ORDER BY n DESC
                 LIMIT ?"
                (str guild-id) (long limit))
        (mapv :tag))))

(defn top-topic-tags
  ([guild-id] (top-topic-tags guild-id {}))
  ([guild-id {:keys [limit] :or {limit 25}}]
   (ensure-conn!)
   (->> (query! "SELECT tt.topic, COUNT(*) AS n
                 FROM topic_tags tt
                 JOIN topics t ON t.message_id = tt.message_id
                 WHERE t.guild_id = ?
                 GROUP BY tt.topic
                 ORDER BY n DESC
                 LIMIT ?"
                (str guild-id) (long limit))
        (mapv :topic))))

(defn repo-all []
  (ensure-conn!)
  (mapv (fn [r]
          {:repo/path (:path r)
           :repo/sha (:sha r)
           :repo/ts (:ts r)
           :repo/bytes (:bytes r)
           :repo/kind (keyword (or (:kind r) "txt"))
           :repo/text (:text r)})
        (query! "SELECT * FROM repo_files ORDER BY path")))

(defn upsert-repo-file! [{:keys [repo/path repo/text repo/sha repo/bytes repo/kind] :as m}]
  (ensure-conn!)
  (let [path (some-> (:repo/path m) str)
        text (some-> (:repo/text m) str)
        sha (some-> (:repo/sha m) str)
        old (when path (scalar "SELECT sha FROM repo_files WHERE path = ?" path))]
    (when (and (seq path) (seq text) (seq sha))
      (if (= old sha)
        false
        (do
          (exec! "INSERT INTO repo_files(path, sha, ts, bytes, kind, text)
                  VALUES (?, ?, ?, ?, ?, ?)
                  ON CONFLICT(path) DO UPDATE SET
                    sha=excluded.sha,
                    ts=excluded.ts,
                    bytes=excluded.bytes,
                    kind=excluded.kind,
                    text=excluded.text"
                 path sha (now-iso) (long (or (:repo/bytes m) (count text)))
                 (name (or (:repo/kind m) :txt)) text)
          (exec! "DELETE FROM repo_fts WHERE path = ?" path)
          (exec! "INSERT INTO repo_fts(path, text) VALUES (?, ?)" path text)
          (maybe-backup!)
          true)))))

(defn repo-search
  ([query] (repo-search query {}))
  ([query {:keys [limit snippet-chars min-score]
           :or {limit 6 snippet-chars 900 min-score 1}}]
   (let [q (-> (or query "") str str/trim)]
     (if (str/blank? q)
       []
       (let [tokens (->> (re-seq #"[a-z0-9\-_]{2,}" (str/lower-case q))
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
                               end (min (count (or text "")) (+ start snippet-chars))]
                           {:path path
                            :sha sha
                            :kind kind
                            :bytes bytes
                            :snippet (subs (or text "") start end)
                            :score score
                            :matched matched})))))
              (remove nil?)
              (sort-by (fn [m] [(- (:score m)) (:path m)]))
              (take limit)
              vec))))))

(defn slap-capabilities [{:keys [guild-id channel-id]}]
  {:scope {:guild/id (some-> guild-id str)
           :channel/id (some-> channel-id str)}
   :storage :sqlite
   :entities
   [{:entity :message
     :fulltext [:message/content]
     :notes ["SQLite FTS5 over raw Discord messages."]}
    {:entity :extract
     :fulltext [:extract/text]
     :tags {:attr :extract/tag :cardinality :many}}
    {:entity :topic
     :topics {:attr :topic/topic :cardinality :many}}
    {:entity :repo
     :fulltext [:repo/text]}]
   :query-back
   {:tool :sqlite/fts
    :compat-tool :datalevin/fts
    :notes ["Legacy :datalevin/fts query-back is accepted but now backed by SQLite."]}})

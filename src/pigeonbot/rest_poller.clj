(ns pigeonbot.rest-poller
  (:require [clojure.string :as str]
            [org.httpkit.client :as http]
            [cheshire.core :as json]
            [pigeonbot.config :as config]
            [pigeonbot.context :as ctx]
            [pigeonbot.db :as db]
            [pigeonbot.commands :as commands]
            [pigeonbot.message-reacts :as reacts]
            [pigeonbot.vision-reacts :as vision-reacts]))

(def ^:private api-base "https://discord.com/api/v10")

(defonce ^:private poller*
  (atom {:running? false
         :future nil
         :last-id-by-channel {}   ;; channel-id -> last message id seen
         :channel-ids []          ;; current discovered channel ids
         :last-refresh-ms 0}))

(defn- now-ms [] (System/currentTimeMillis))

(defn- bot-token []
  (let [cfg (config/load-config)]
    (or (:token cfg) (config/discord-token))))

(defn- auth-header [token]
  {"Authorization" (str "Bot " token)
   "Content-Type" "application/json"})

(defn- decode [s] (json/decode s true))

(defn- sleep! [ms]
  (when (pos? (long ms))
    (Thread/sleep (long ms))))

(defn- http-get-json
  "GET + parse JSON. Handles 429 retry_after."
  [url headers]
  (loop [attempt 0]
    (let [{:keys [status body error]}
          @(http/get url {:headers headers :timeout 60000})]
      (cond
        error
        (throw (ex-info "Discord GET failed" {:url url :error (str error)}))

        (= status 429)
        (let [m (try (decode body) (catch Throwable _ {}))
              retry (double (or (:retry_after m) 1.0))
              ms (long (+ (* 1000.0 retry) 250))]
          (if (< attempt 12)
            (do (sleep! ms) (recur (inc attempt)))
            (throw (ex-info "Discord rate limit retry exceeded" {:url url :status status}))))

        (not (<= 200 status 299))
        (throw (ex-info "Discord GET non-2xx"
                        {:url url :status status
                         :body (subs (str body) 0 (min 400 (count (str body))))}))

        :else
        (if (string? body) (decode body) body)))))

;; -----------------------------------------------------------------------------
;; Guild channel discovery
;; -----------------------------------------------------------------------------

(defn- guild-channel-types
  "Default channel types we poll:
   0 = GUILD_TEXT
   5 = GUILD_ANNOUNCEMENT"
  []
  #{0 5})

(defn- fetch-guild-channels
  [guild-id]
  (let [token (bot-token)
        url (str api-base "/guilds/" (str guild-id) "/channels")]
    (vec (http-get-json url (auth-header token)))))

(defn- textish-channel?
  [ch]
  (contains? (guild-channel-types) (:type ch)))

(defn- discover-channel-ids!
  "Refresh channel id list from Discord REST."
  [guild-id]
  (let [chs (->> (fetch-guild-channels (str guild-id))
                 (filter textish-channel?)
                 (sort-by :id)
                 (map (comp str :id))
                 vec)]
    (swap! poller* assoc
           :channel-ids chs
           :last-refresh-ms (now-ms))
    chs))

(defn- maybe-refresh-channels!
  [guild-id refresh-ms]
  (let [last (long (or (:last-refresh-ms @poller*) 0))
        now (now-ms)]
    (when (or (empty? (:channel-ids @poller*))
              (>= (- now last) (long refresh-ms)))
      (discover-channel-ids! guild-id))))

;; -----------------------------------------------------------------------------
;; Message polling
;; -----------------------------------------------------------------------------

(defn- fetch-messages
  "Fetch up to `limit` messages in channel. If after-id provided, only newer."
  [channel-id after-id limit]
  (let [token (bot-token)
        limit (long (or limit 50))
        q (str "limit=" limit (when after-id (str "&after=" after-id)))
        url (str api-base "/channels/" (str channel-id) "/messages?" q)]
    (vec (http-get-json url (auth-header token)))))

(defn- normalize-msg
  "Ensure msg has channel-id + guild-id fields like gateway payloads."
  [guild-id channel-id msg]
  (assoc msg
         :guild-id (str guild-id)
         :channel-id (str channel-id)))

(defn- bot-message? [msg]
  (true? (get-in msg [:author :bot])))

(defn- ingest-message!
  "Ingest one message through the existing pipeline."
  [msg]
  ;; rolling in-memory context
  (ctx/record-message! msg)

  ;; Datalevin spine + meta
  (try (db/upsert-message! msg)
       (catch Throwable t (println "db/upsert-message! error:" (.getMessage t))))
  (try (db/upsert-message-meta! msg)
       (catch Throwable t (println "db/upsert-message-meta! error:" (.getMessage t))))

  ;; normal behavior
  (commands/handle-message msg)
  (reacts/maybe-react! msg)
  (vision-reacts/maybe-react-vision! msg)
  true)

(defn- poll-channel-once!
  [guild-id channel-id {:keys [limit]}]
  (let [cid (str channel-id)
        last-id (get-in @poller* [:last-id-by-channel cid])
        msgs (fetch-messages cid last-id (or limit 50))
        ;; Discord returns newest-first; ingest oldest-first
        msgs (->> msgs (sort-by :id) vec)
        msgs (mapv (partial normalize-msg guild-id cid) msgs)]
    (when (seq msgs)
      (doseq [m msgs]
        ;; ignore bot's own messages (prevents loops)
        (when-not (bot-message? m)
          (ingest-message! m)))
      (let [new-last (some-> (last msgs) :id str)]
        (swap! poller* assoc-in [:last-id-by-channel cid] new-last)))
    {:channel-id cid
     :fetched (count msgs)
     :last-id (get-in @poller* [:last-id-by-channel cid])}))

(defn- prime-last-ids!
  "Prime last-id per channel so we don't replay history unless :poll-catchup? true."
  [guild-id channel-ids]
  (let [cfg (config/load-config)
        catchup? (true? (:poll-catchup? cfg))]
    (when-not catchup?
      (doseq [cid channel-ids]
        (let [msgs (fetch-messages cid nil 1)
              last-id (some-> (first msgs) :id str)]
          (when (seq last-id)
            (swap! poller* assoc-in [:last-id-by-channel (str cid)] last-id)))))
    true))

;; -----------------------------------------------------------------------------
;; Public API
;; -----------------------------------------------------------------------------

(defn start!
  "Start REST polling across ALL guild text channels.

  config.edn keys:
    :poll-guild-id            \"...\"   (required)
    :poll-interval-ms         2500      (default)
    :poll-limit               50        (default)
    :poll-channel-refresh-ms  600000    (default 10 minutes)
    :poll-catchup?            false     (default; if true, will ingest from newest forward without priming)
  "
  []
  (db/ensure-conn!)
  (let [cfg (config/load-config)
        guild-id (:poll-guild-id cfg)
        interval-ms (long (or (:poll-interval-ms cfg) 2500))
        refresh-ms (long (or (:poll-channel-refresh-ms cfg) (* 10 60 1000)))
        limit (long (or (:poll-limit cfg) 50))]
    (when-not (seq (str guild-id))
      (throw (ex-info "rest-poller/start!: missing :poll-guild-id in config.edn"
                      {:poll-guild-id guild-id})))
    (when (:running? @poller*)
      (println "rest-poller: already running")
      :already-running)
    (let [gid (str guild-id)]
      ;; initial channel discovery + last-id priming
      (let [chs (do (maybe-refresh-channels! gid refresh-ms)
                    (:channel-ids @poller*))]
        (prime-last-ids! gid chs))

      (swap! poller* assoc :running? true)
      (swap! poller* assoc :future
             (future
               (println "rest-poller: START"
                        {:guild-id gid
                         :interval-ms interval-ms
                         :refresh-ms refresh-ms
                         :limit limit})
               (loop []
                 (when (:running? @poller*)
                   (try
                     (maybe-refresh-channels! gid refresh-ms)
                     (let [chs (:channel-ids @poller*)]
                       (doseq [cid chs]
                         (poll-channel-once! gid cid {:limit limit})))
                     (catch Throwable t
                       (println "rest-poller: error" (.getMessage t) (or (ex-data t) {}))))
                   (sleep! interval-ms)
                   (recur)))
               (println "rest-poller: STOP")))

      :started)))

(defn stop!
  []
  (when-not (:running? @poller*)
    (println "rest-poller: not running")
    :not-running)
  (swap! poller* assoc :running? false)
  (when-let [f (:future @poller*)]
    (future-cancel f))
  (swap! poller* assoc :future nil)
  :stopped)

(defn status []
  (select-keys @poller* [:running? :channel-ids :last-id-by-channel :last-refresh-ms]))

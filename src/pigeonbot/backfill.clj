(ns pigeonbot.backfill
  "On-demand channel history backfill using the Discord REST API.
  Called once on startup per channel; skips channels that already have messages in Datalevin."
  (:require [org.httpkit.client :as http]
            [cheshire.core :as json]
            [pigeonbot.db :as db]
            [pigeonbot.config :as config]
            [pigeonbot.channels :as channels]))

(def ^:private discord-api "https://discord.com/api/v10")
(def ^:private default-max-messages 1000)
(def ^:private default-max-days 30)
(def ^:private page-size 100)
(def ^:private rate-limit-sleep-ms 1200) ; conservative: 1.2s between pages

(defn- bot-token []
  (or (:token (config/load-config))
      (System/getenv "DISCORD_TOKEN")
      (System/getenv "PIGEONBOT_TOKEN")))

(defn- headers []
  {"Authorization" (str "Bot " (bot-token))
   "Content-Type"  "application/json"})

(defn- fetch-messages-page
  "Fetch up to page-size messages before the given snowflake ID (or nil for latest).
  Returns seq of raw message maps (string keys), or nil on error."
  [channel-id before-id]
  (let [url    (cond-> (str discord-api "/channels/" channel-id "/messages?limit=" page-size)
                 before-id (str "&before=" before-id))
        {:keys [status body error]}
        @(http/get url {:headers (headers) :timeout 15000})]
    (cond
      error
      (do (println "[backfill] HTTP error for channel" channel-id ":" (str error)) nil)

      (= status 403)
      (do (println "[backfill] No access to channel" channel-id "(403), skipping") nil)

      (= status 404)
      (do (println "[backfill] Channel" channel-id "not found (404), skipping") nil)

      (not (<= 200 status 299))
      (do (println "[backfill] Unexpected status" status "for channel" channel-id) nil)

      :else
      (try (json/decode body)
           (catch Throwable t
             (println "[backfill] JSON parse error:" (.getMessage t))
             nil)))))

(defn- cutoff-instant [max-days]
  (.minusSeconds (java.time.Instant/now) (* max-days 86400)))

(defn- msg-timestamp [msg]
  (try (java.time.Instant/parse (get msg "timestamp" ""))
       (catch Throwable _ nil)))

(defn- discord-msg->record
  "Convert a raw Discord REST message map (string keys) to a record-message! compatible map."
  [msg]
  (let [author (get msg "author" {})]
    {:id         (get msg "id")
     :channel-id (get msg "channel_id")
     :guild-id   (get msg "guild_id")
     :content    (get msg "content" "")
     :timestamp  (get msg "timestamp")
     :author     {:id          (get author "id")
                  :username    (get author "username")
                  :global_name (get author "global_name")
                  :bot         (get author "bot" false)}}))

(defn backfill-channel!
  "Backfill a single channel. Fetches up to max-messages messages going back max-days days.
  Skips if the channel already has stored messages."
  [channel-id max-messages max-days]
  (let [count (db/channel-message-count channel-id)]
    (if (pos? count)
      (println "[backfill] Channel" channel-id "already has" count "messages, skipping")
      (do
        (println "[backfill] Backfilling channel" channel-id "...")
        (let [cutoff (cutoff-instant max-days)]
          (loop [before-id   nil
                 total       0
                 stop?       false]
            (if (or stop? (>= total max-messages))
              (println "[backfill] Channel" channel-id "done:" total "messages stored")
              (let [page (fetch-messages-page channel-id before-id)]
                (if (or (nil? page) (empty? page))
                  (println "[backfill] Channel" channel-id "done (end of history):" total "messages stored")
                  (let [records   (map discord-msg->record page)
                        oldest-ts (some msg-timestamp (reverse page))
                        too-old?  (and oldest-ts (.isBefore oldest-ts cutoff))
                        ;; Filter out messages older than cutoff
                        keep      (if too-old?
                                    (filter (fn [r]
                                              (when-let [ts (try (java.time.Instant/parse (str (:timestamp r)))
                                                                 (catch Throwable _ nil))]
                                                (.isAfter ts cutoff)))
                                            records)
                                    records)]
                    (doseq [r keep]
                      (try (db/record-message! r)
                           (catch Throwable t
                             (println "[backfill] record-message! error:" (.getMessage t)))))
                    (Thread/sleep rate-limit-sleep-ms)
                    (recur (get (last page) "id")
                           (+ total (count keep))
                           (or too-old? (< (count page) page-size))))))))))))

(defn maybe-backfill!
  "Called on startup. Backfills all known channels that have no stored messages.
  Runs in a background thread to avoid blocking bot startup."
  []
  (let [cfg          (config/load-config)
        enabled?     (get cfg :backfill-enabled? true)
        max-messages (get cfg :backfill-max-messages default-max-messages)
        max-days     (get cfg :backfill-max-days default-max-days)]
    (if-not enabled?
      (println "[backfill] Disabled via :backfill-enabled? false")
      (future
        (try
          (let [channel-ids (keys (:by-id @channels/channels*))]
            (if (empty? channel-ids)
              (println "[backfill] No known channels in channels.edn, nothing to backfill")
              (do
                (println "[backfill] Starting backfill for" (count channel-ids) "channels")
                (doseq [cid channel-ids]
                  (backfill-channel! (str cid) max-messages max-days))
                (println "[backfill] All channels backfilled"))))
          (catch Throwable t
            (println "[backfill] Fatal error:" (.getMessage t))))))))

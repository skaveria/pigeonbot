(ns pigeonbot.backfill
  (:require [org.httpkit.client :as http]
            [cheshire.core :as json]
            [pigeonbot.config :as config]
            [pigeonbot.db :as db]))

(def ^:private api-base "https://discord.com/api/v10")

(defn- bot-token []
  (let [cfg (config/load-config)]
    (or (:token cfg) (config/discord-token))))

(defn- auth-header [token]
  {"Authorization" (str "Bot " token)
   "Content-Type" "application/json"})

(defn- decode [s]
  (json/decode s true))

(defn- sleep!
  "Sleep for ms (long)."
  [ms]
  (Thread/sleep (long ms)))

(defn- http-get-json
  "GET and parse JSON. Handles 429 with retry_after.
  Returns decoded JSON body."
  [url headers]
  (loop [attempt 0]
    (let [{:keys [status body error]}
          @(http/get url {:headers headers :timeout 60000})]
      (cond
        error
        (throw (ex-info "Discord GET failed" {:url url :error (str error)}))

        (= status 429)
        (let [m (try (decode body) (catch Throwable _ {}))
              retry (or (:retry_after m) 1.0)
              ms (long (+ (* 1000.0 (double retry)) 250))]
          (if (< attempt 10)
            (do (sleep! ms) (recur (inc attempt)))
            (throw (ex-info "Discord rate limit retry exceeded"
                            {:url url :status status :body body}))))

        (not (<= 200 status 299))
        (throw (ex-info "Discord GET non-2xx" {:url url :status status :body body}))

        :else
        (if (string? body) (decode body) body)))))

(defn fetch-guild-channels
  [guild-id]
  (let [token (bot-token)
        url (str api-base "/guilds/" (str guild-id) "/channels")]
    (vec (http-get-json url (auth-header token)))))

(defn fetch-channel-messages
  [channel-id before-id]
  (let [token (bot-token)
        q (cond-> "limit=100"
            before-id (str "&before=" (str before-id)))
        url (str api-base "/channels/" (str channel-id) "/messages?" q)]
    (vec (http-get-json url (auth-header token)))))

(defn backfill-channel!
  "Backfill ALL messages for a single channel into Datalevin.

  Options:
    :delay-ms (default 350)
    :max-pages (default nil)

  Returns {:channel-id .. :pages .. :messages ..}"
  [guild-id channel-id & {:keys [delay-ms max-pages channel-type]
                          :or {delay-ms 350}}]
  (db/ensure-conn!)
  (loop [before nil
         pages 0
         total 0]
    (cond
      (and max-pages (>= pages max-pages))
      {:channel-id (str channel-id) :pages pages :messages total}

      :else
      (let [msgs (fetch-channel-messages (str channel-id) before)]
        (if (empty? msgs)
          {:channel-id (str channel-id) :pages pages :messages total}
          (do
            ;; Inject context Discord doesn't include in each message object
            (doseq [m msgs]
              (db/upsert-message!
               (cond-> m
                 true (assoc :channel-id (str channel-id)
                             :guild-id (str guild-id))
                 channel-type (assoc :channel-type (long channel-type)))))

            (sleep! delay-ms)

            (let [last-id (some-> (last msgs) :id str)]
              (recur last-id (inc pages) (+ total (count msgs))))))))))

(defn- guild-textish-channel?
  "0 = GUILD_TEXT, 5 = GUILD_ANNOUNCEMENT"
  [{:keys [type]}]
  (contains? #{0 5} type))

(defn backfill-guild!
  "Backfill ALL messages for ALL textish channels in a guild."
  [guild-id & {:keys [delay-ms]
               :or {delay-ms 350}}]
  (let [chs (->> (fetch-guild-channels (str guild-id))
                 (filter guild-textish-channel?)
                 (sort-by :id))
        n (count chs)]
    (println "Backfill guild" (str guild-id) "channels:" n)
    (reduce
      (fn [acc ch]
        (let [cid (str (:id ch))
              nm  (or (:name ch) "")
              ctype (:type ch)]
          (println "Backfilling channel" cid nm)
          (let [res (backfill-channel! (str guild-id) cid
                                       :delay-ms delay-ms
                                       :channel-type ctype)]
            (update acc :channels conj res))))
      {:guild-id (str guild-id)
       :channels []}
      chs)))

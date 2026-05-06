(ns pigeonbot.backfill
  (:require [cheshire.core :as json]
            [org.httpkit.client :as http]
            [pigeonbot.config :as config]
            [pigeonbot.db :as db]))

(def ^:private api-base "https://discord.com/api/v10")

(defn- bot-token []
  (let [cfg (config/load-config)]
    (or (:token cfg)
        (config/discord-token))))

(defn- auth-header [token]
  {"Authorization" (str "Bot " token)
   "Content-Type" "application/json"})

(defn- decode-json [s]
  (try
    (json/decode (str s) true)
    (catch Throwable _
      nil)))

(defn- sleep! [ms]
  (Thread/sleep (long ms)))

(defn- body-preview [body]
  (let [s (str body)]
    (subs s 0 (min 800 (count s)))))

(defn- retryable-status? [status]
  (contains? #{429 500 502 503 504} status))

(defn- retry-delay-ms [status body attempt]
  (if (= status 429)
    (let [m (or (decode-json body) {})
          retry-after (double (or (:retry_after m) 1.0))]
      (long (+ 250 (* 1000.0 retry-after))))
    (long (min 30000
               (+ 750 (* 750 attempt attempt))))))

(defn- http-get-json
  "GET and parse JSON.

  Handles Discord rate limits and transient upstream errors.
  Throws only after retries are exhausted."
  [url headers]
  (loop [attempt 0]
    (let [{:keys [status body error]}
          @(http/get url {:headers headers
                          :timeout 90000})]
      (cond
        error
        (if (< attempt 12)
          (do
            (println "Discord GET transport error; retrying"
                     {:attempt attempt
                      :url url
                      :error (str error)})
            (sleep! (retry-delay-ms 503 nil attempt))
            (recur (inc attempt)))
          (throw
           (ex-info "Discord GET failed"
                    {:url url
                     :error (str error)})))

        (and status (retryable-status? status))
        (if (< attempt 12)
          (let [ms (retry-delay-ms status body attempt)]
            (println "Discord GET retryable status; retrying"
                     {:attempt attempt
                      :status status
                      :sleep-ms ms
                      :url url
                      :body (body-preview body)})
            (sleep! ms)
            (recur (inc attempt)))
          (throw
           (ex-info "Discord GET retry exhausted"
                    {:url url
                     :status status
                     :body (body-preview body)})))

        (not (<= 200 status 299))
        (throw
         (ex-info "Discord GET non-2xx"
                  {:url url
                   :status status
                   :body (body-preview body)}))

        :else
        (or (decode-json body)
            (throw
             (ex-info "Discord GET returned non-JSON"
                      {:url url
                       :status status
                       :body (body-preview body)})))))))

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

(defn- guild-textish-channel?
  "0 = GUILD_TEXT
   5 = GUILD_ANNOUNCEMENT"
  [{:keys [type]}]
  (contains? #{0 5} type))

(defn backfill-channel!
  "Backfill all messages for one Discord channel.

  Options:
    :delay-ms     default 350
    :max-pages    nil for all pages
    :channel-type optional Discord channel type

  Returns:
    {:channel-id \"...\"
     :pages n
     :messages n
     :done? true}"
  [guild-id channel-id & {:keys [delay-ms max-pages channel-type]
                          :or {delay-ms 350}}]
  (db/ensure-conn!)
  (loop [before nil
         pages 0
         total 0]
    (cond
      (and max-pages (>= pages (long max-pages)))
      {:channel-id (str channel-id)
       :pages pages
       :messages total
       :done? false
       :reason :max-pages}

      :else
      (let [msgs (fetch-channel-messages (str channel-id) before)]
        (if (empty? msgs)
          {:channel-id (str channel-id)
           :pages pages
           :messages total
           :done? true}
          (do
            (doseq [m msgs]
              (db/upsert-message!
               (cond-> m
                 true
                 (assoc :channel-id (str channel-id)
                        :guild-id (str guild-id))

                 channel-type
                 (assoc :channel-type (long channel-type)))))

            (when (pos? (long delay-ms))
              (sleep! delay-ms))

            (recur (some-> (last msgs) :id str)
                   (inc pages)
                   (+ total (count msgs)))))))))

(defn backfill-guild!
  "Backfill all text-ish channels in a guild.

  Options:
    :delay-ms  default 350

  This is safe to rerun. db/upsert-message! should make message writes idempotent."
  [guild-id & {:keys [delay-ms]
               :or {delay-ms 350}}]
  (db/ensure-conn!)
  (let [chs (->> (fetch-guild-channels (str guild-id))
                 (filter guild-textish-channel?)
                 (sort-by :id)
                 vec)]
    (println "Backfill guild" (str guild-id) "channels:" (count chs))
    (reduce
     (fn [acc ch]
       (let [cid (str (:id ch))
             nm (or (:name ch) "")
             ctype (:type ch)]
         (println "Backfilling channel" cid nm)
         (try
           (let [res (backfill-channel! (str guild-id)
                                        cid
                                        :delay-ms delay-ms
                                        :channel-type ctype)]
             (update acc :channels conj res))
           (catch Throwable t
             (println "Backfill channel failed"
                      {:channel-id cid
                       :name nm
                       :error (.getMessage t)
                       :data (ex-data t)})
             (update acc :channels conj
                     {:channel-id cid
                      :name nm
                      :error (.getMessage t)
                      :data (ex-data t)})))))
     {:guild-id (str guild-id)
      :channels []}
     chs)))

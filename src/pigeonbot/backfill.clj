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
  [ms]
  (Thread/sleep (long ms)))

(defn- short-body [body]
  (let [s (str body)]
    (subs s 0 (min 400 (count s)))))

(defn- retryable-status?
  [status]
  (contains? #{502 503 504} status))

(defn- http-get-json
  "GET and parse JSON.

  Handles:
  - 429 with retry_after
  - 502/503/504 with bounded retries + backoff

  Returns decoded JSON body."
  [url headers]
  (loop [attempt 0]
    (let [{:keys [status body error]}
          @(http/get url {:headers headers :timeout 60000})]
      (cond
        error
        (if (< attempt 8)
          (let [ms (+ 1000 (* attempt 1000))]
            (println "Discord GET transport error, retrying:"
                     {:url url :attempt attempt :sleep-ms ms :error (str error)})
            (sleep! ms)
            (recur (inc attempt)))
          (throw (ex-info "Discord GET failed"
                          {:url url :error (str error)})))

        (= status 429)
        (let [m (try (decode body) (catch Throwable _ {}))
              retry (or (:retry_after m) 1.0)
              ms (long (+ (* 1000.0 (double retry)) 250))]
          (if (< attempt 12)
            (do
              (println "Discord rate limited, retrying:"
                       {:url url :attempt attempt :sleep-ms ms})
              (sleep! ms)
              (recur (inc attempt)))
            (throw (ex-info "Discord rate limit retry exceeded"
                            {:url url :status status :body body}))))

        (retryable-status? status)
        (if (< attempt 8)
          (let [ms (+ 1500 (* attempt 1500))]
            (println "Discord upstream error, retrying:"
                     {:url url :status status :attempt attempt :sleep-ms ms
                      :body (short-body body)})
            (sleep! ms)
            (recur (inc attempt)))
          (throw (ex-info "Discord GET non-2xx"
                          {:url url :status status :body body})))

        (not (<= 200 status 299))
        (throw (ex-info "Discord GET non-2xx"
                        {:url url :status status :body body}))

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

(defn backfill-channel-safe!
  "Same as backfill-channel!, but never throws.
  Returns either normal result or {:channel-id .. :error ... :data ...}."
  [guild-id channel-id & {:keys [delay-ms max-pages channel-type]
                          :or {delay-ms 350}}]
  (try
    (backfill-channel! guild-id channel-id
                       :delay-ms delay-ms
                       :max-pages max-pages
                       :channel-type channel-type)
    (catch Throwable t
      {:channel-id (str channel-id)
       :error (.getMessage t)
       :data (try (ex-data t) (catch Throwable _ nil))})))

(defn backfill-guild!
  "Backfill ALL messages for ALL textish channels in a guild.

  Safe behavior:
  - continues past channel failures
  - returns both successes and failures"
  [guild-id & {:keys [delay-ms]
               :or {delay-ms 350}}]
  (let [chs (->> (fetch-guild-channels (str guild-id))
                 (filter guild-textish-channel?)
                 (sort-by :id))
        n (count chs)]
    (println "Backfill guild" (str guild-id) "channels:" n)
    (reduce
     (fn [acc ch]
       (let [cid   (str (:id ch))
             nm    (or (:name ch) "")
             ctype (:type ch)]
         (println "Backfilling channel" cid nm)
         (let [res (backfill-channel-safe! (str guild-id) cid
                                           :delay-ms delay-ms
                                           :channel-type ctype)]
           (if (:error res)
             (do
               (println "Channel backfill FAILED:"
                        {:channel-id cid
                         :name nm
                         :error (:error res)
                         :data (:data res)})
               (update acc :failed conj (assoc res :name nm)))
             (update acc :channels conj (assoc res :name nm))))))
     {:guild-id (str guild-id)
      :channels []
      :failed []}
     chs)))

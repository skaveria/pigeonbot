(ns pigeonbot.brain
  (:require [pigeonbot.db :as db]
            [pigeonbot.slap :as slap]
            [pigeonbot.anthropic :as anthropic]))

(def ^:private max-slap-iterations 5)
(def ^:private recent-history-count 20)
(def ^:private temporal-24h-ms (* 24 60 60 1000))
(def ^:private temporal-7d-ms  (* 7 24 60 60 1000))

(defn- instant-minus-ms [ms]
  (.minusMillis (java.time.Instant/now) ms))

(defn- build-slap-state
  "Gather all DB data needed for a SLAP packet iteration."
  [channel-id author-id question query-results]
  (let [msgs           (db/recent-messages channel-id recent-history-count)
        user-raw       (db/user-info author-id)
        user-facts     (db/user-facts author-id)
        user-info      (when user-raw
                         (let [[name first-seen _last] user-raw]
                           {:name       name
                            :discord-id author-id
                            :first-seen first-seen
                            :facts      user-facts}))
        temporal-msgs  (db/messages-since channel-id (instant-minus-ms temporal-24h-ms))
        temporal-facts (db/facts-since (instant-minus-ms temporal-7d-ms))]
    {:msgs           msgs
     :question       question
     :user-info      user-info
     :known-facts    user-facts
     :query-results  query-results
     :temporal-msgs  temporal-msgs
     :temporal-facts temporal-facts}))

(defn- run-query-backs
  "Execute LLM query-back maps against Datalevin.
  Returns seq of {:iter :query :rows} maps."
  [query-backs iter]
  (mapv (fn [qb]
          {:iter  iter
           :query (select-keys qb ["find" "where"])
           :rows  (db/run-query qb)})
        query-backs))

(defn ask!
  "SLAP loop. Builds packet, calls Anthropic, handles query-backs, transacts facts.
  Returns the answer string to send to Discord.

  Argument map: {:channel-id :author-id :question :msg-id}"
  [{:keys [channel-id author-id question msg-id]}]
  (loop [iter          1
         query-results []]
    (let [state      (build-slap-state channel-id author-id question query-results)
          system-str (slap/build-system-prompt state)
          user-str   (slap/build-user-turn state)
          raw-resp   (try
                       (anthropic/call! system-str user-str slap/response-json-schema)
                       (catch Throwable t
                         (println "[brain] Anthropic call failed:" (.getMessage t))
                         nil))
          {:keys [answer extract query-back]}
          (if raw-resp
            (slap/parse-response raw-resp)
            {:answer "" :extract [] :query-back []})]

      ;; Transact extracted facts on every iteration
      (when (seq extract)
        (try (db/transact-facts! extract msg-id)
             (catch Throwable t
               (println "[brain] transact-facts! failed:" (.getMessage t)))))

      (cond
        (nil? raw-resp)
        "something went wrong on my end, try poking me again"

        (seq answer)
        answer

        (and (seq query-back) (< iter max-slap-iterations))
        (let [new-results (run-query-backs query-back iter)]
          (recur (inc iter)
                 (into query-results new-results)))

        :else
        "hm, I lost my train of thought. ask me again."))))

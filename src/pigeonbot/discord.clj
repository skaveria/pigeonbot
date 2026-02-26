(ns pigeonbot.discord
  (:require [clojure.core.async :as a]
            [clojure.string :as str]
            [discljord.connections :as c]
            [discljord.events :as e]
            [discljord.messaging :as m]
            [pigeonbot.channels :as channels]
            [pigeonbot.commands :as commands]
            [pigeonbot.config :as config]
            [pigeonbot.context :as ctx]
            [pigeonbot.custom-commands :as custom]
            [pigeonbot.db :as db]
            [pigeonbot.discljord-patch :as djpatch]
            [pigeonbot.message-reacts :as reacts]
            [pigeonbot.vision-registry :as vision-reg]
            [pigeonbot.vision-reacts :as vision-reacts]
            [pigeonbot.reaction-roles :as rr]
            ;; If pest exists in your tree, keep it:
            [pigeonbot.pest :as pest]
            [pigeonbot.state :refer [state]]))

(defonce seen-events* (atom #{}))
(defonce bot-future* (atom nil))

(defn handle-event
  [event-type event-data]
  (when-not (contains? @seen-events* event-type)
    (swap! seen-events* conj event-type)
    (println "EVENT TYPE:" event-type))

  (when (and event-type (str/includes? (name event-type) "reaction"))
    (println "REACTION-ish EVENT:" event-type
             (select-keys event-data [:guild-id :channel-id :message-id :user-id :emoji])))

  (case event-type
    :ready
    (do
      (when-let [bid (or (get-in event-data [:user :id])
                         (get-in event-data [:user :user :id]))]
        (swap! state assoc :bot-user-id (str bid)))
      nil)

    :message-create
    (do
      ;; rolling context (in-memory)
      (ctx/record-message! event-data)

      ;; spine + derived metadata (Datalevin)
      (try (db/upsert-message! event-data)
           (catch Throwable t
             (println "db/upsert-message! error:" (.getMessage t))))
      (try (db/upsert-message-meta! event-data)
           (catch Throwable t
             (println "db/upsert-message-meta! error:" (.getMessage t))))

      ;; command routing
      (commands/handle-message event-data)

      ;; passive reacts
      (reacts/maybe-react! event-data)

      ;; vision rules
      (vision-reacts/maybe-react-vision! event-data)

      ;; pest mode (optional)
      (when (resolve 'pigeonbot.pest/maybe-pest!)
        (pest/maybe-pest! event-data)))

    :message-reaction-add
    (rr/handle-reaction-add! event-data)

    :message-reaction-remove
    (rr/handle-reaction-remove! event-data)

    nil))

(defn- supported-intents-set []
  (set c/gateway-intents))

(defn- desired-intents
  "Choose gateway intents.

  Default: [:guilds :guild-messages]
  Override: :discord-intents [...] in config.edn

  Unsupported intents are dropped."
  []
  (let [cfg (config/load-config)
        requested (or (:discord-intents cfg)
                      [:guilds :guild-messages])
        supported (supported-intents-set)
        chosen (->> requested (map keyword) (filter supported) set)
        dropped (->> requested (map keyword) (remove supported) vec)]
    (when (seq dropped)
      (println "WARNING: Dropping unsupported gateway intents:" dropped))
    chosen))

(defn start-bot
  "Connect to Discord and start the event pump (blocking)."
  []
  (try
    ;; ensure DB early
    (db/ensure-conn!)

    (channels/load-channels!)
    (custom/load!)
    (reacts/load!)
    (vision-reg/load!)
    (djpatch/patch-discljord!)

    (let [cfg (config/load-config)
          token (or (:token cfg) (config/discord-token))
          _ (when-not (and token (seq token))
              (throw (ex-info "Missing Discord token. Set DISCORD_TOKEN in your environment."
                              {:env ["DISCORD_TOKEN" "PIGEONBOT_TOKEN"]})))

          event-ch (a/chan 100)
          intents (desired-intents)

          ;; if this throws, we want to SEE IT
          conn   (c/connect-bot! token event-ch :intents intents)
          msg-ch (m/start-connection! token)]

      (println "Gateway intents:" intents)
      (reset! state {:connection conn :events event-ch :messaging msg-ch})
      (println "Connected to Discord (online)")

      ;; Blocking pump
      (e/message-pump! event-ch handle-event))
    (catch Throwable t
      (println "[pigeonbot.discord] start-bot crashed:" (.getMessage t) (or (ex-data t) {}))
      (throw t))))

(defn- throw-if-future-failed!
  "If future is done, deref it to surface crashes (rethrows)."
  [f]
  (when (and f (future-done? f))
    (try
      @f
      (catch Throwable t
        (println "[pigeonbot.discord] bot future failed:" (.getMessage t) (or (ex-data t) {}))
        (throw t)))))

(defn start-bot!
  "Start bot in a future and wait until :messaging is present in state.
  Returns messaging channel or nil.

  If the bot future crashes, we print and rethrow the exception."
  []
  (let [f (future (start-bot))]
    (reset! bot-future* f)
    ;; wait up to ~30s (120 * 250ms)
    (loop [tries 0]
      (cond
        ;; success
        (:messaging @state)
        (:messaging @state)

        ;; crash: surface real exception
        (future-done? f)
        (do
          (throw-if-future-failed! f)
          ;; If it somehow completed cleanly but didn't set :messaging:
          (println "start-bot!: bot future completed but messaging is still nil.")
          nil)

        ;; timeout
        (>= tries 120)
        (do
          (println "start-bot!: timed out waiting for messaging connection.")
          ;; if it died right at the end, surface it
          (when (future-done? f)
            (throw-if-future-failed! f))
          nil)

        :else
        (do
          (Thread/sleep 250)
          (recur (inc tries)))))))

(defn stop-bot! []
  (let [{:keys [events connection messaging]} @state]
    (println "Stopping bot connections…")
    (when events
      (try (a/close! events)
           (catch Throwable t (println "stop-bot!: close events failed:" (.getMessage t)))))
    (when connection
      (try (c/disconnect-bot! connection)
           (catch Throwable t (println "stop-bot!: disconnect-bot! failed:" (.getMessage t)))))
    (when messaging
      (try (m/stop-connection! messaging)
           (catch Throwable t (println "stop-bot!: stop-connection! failed:" (.getMessage t))))))

  (when-let [f @bot-future*] (future-cancel f))
  (reset! bot-future* nil)
  (reset! state {})
  (println "Bot stopped."))

(defn restart-bot! []
  (println "Restarting pigeonbot…")
  (stop-bot!)
  (Thread/sleep 500)
  (start-bot!))

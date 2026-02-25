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
            [pigeonbot.discljord-patch :as djpatch]
            [pigeonbot.message-reacts :as reacts]
            [pigeonbot.vision-registry :as vision-reg]
            [pigeonbot.vision-reacts :as vision-reacts]
            [pigeonbot.reaction-roles :as rr]
            [pigeonbot.state :refer [state]]))

(defonce seen-events* (atom #{}))
(defonce bot-future (atom nil))

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
      ;; best-effort: store bot user id if present in ready payload
      (when-let [bid (or (get-in event-data [:user :id])
                         (get-in event-data [:user :user :id]))]
        (swap! state assoc :bot-user-id (str bid)))
      nil)

    :message-create
    (do
      (ctx/record-message! event-data)
      (pigeonbot.db/upsert-message! event-data)
      ;; command routing
      (commands/handle-message event-data)

      ;; passive reacts
      (reacts/maybe-react! event-data)

      ;; vision rules (image -> labels -> actions)
      (vision-reacts/maybe-react-vision! event-data))

    :message-reaction-add
    (rr/handle-reaction-add! event-data)

    :message-reaction-remove
    (rr/handle-reaction-remove! event-data)

    nil))

(defn- supported-intents-set []
  ;; In discljord 1.3.1 this is a collection (not a map), so treat it as a set.
  (set c/gateway-intents))

(defn- desired-intents
  "Choose gateway intents.

  Default is minimal & known-good: #{:guilds :guild-messages}

  You can override via config.edn:
    :discord-intents [:guilds :guild-messages :guild-message-reactions]

  We automatically drop any intent keywords not supported by this discljord build."
  []
  (let [cfg (config/load-config)
        requested (or (:discord-intents cfg)
                      [:guilds :guild-messages])
        supported (supported-intents-set)
        chosen (->> requested
                    (map keyword)
                    (filter supported)
                    set)
        dropped (->> requested
                     (map keyword)
                     (remove supported)
                     vec)]
    (when (seq dropped)
      (println "WARNING: Dropping unsupported gateway intents:" dropped))
    chosen))

(defn start-bot
  "Connect to Discord and start the event pump (blocking)."
  []
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

        conn   (c/connect-bot! token event-ch :intents intents)
        msg-ch (m/start-connection! token)]

    (println "Gateway intents:" intents)

    (reset! state {:connection conn :events event-ch :messaging msg-ch})
    (println "Connected to Discord (online)")
    (e/message-pump! event-ch handle-event)))

(defn start-bot! []
  (reset! bot-future (future (start-bot)))
  (loop [tries 0]
    (if-let [msg (:messaging @state)]
      msg
      (if (< tries 20)
        (do (Thread/sleep 250) (recur (inc tries)))
        (do (println "start-bot!: timed out waiting for messaging connection.") nil)))))

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

  (when-let [f @bot-future] (future-cancel f))
  (reset! bot-future nil)
  (reset! state {})
  (println "Bot stopped."))

(defn restart-bot! []
  (println "Restarting pigeonbot…")
  (stop-bot!)
  (Thread/sleep 500)
  (start-bot!))

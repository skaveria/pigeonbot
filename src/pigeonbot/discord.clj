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

  ;; command routing
  (commands/handle-message event-data)

  ;; passive reacts
  (reacts/maybe-react! event-data)

  ;; vision rules (image -> labels -> actions)
  (vision-reacts/maybe-react-vision! event-data)
  (vision-reacts/maybe-react-opossum! event-data))

    :message-reaction-add
    (rr/handle-reaction-add! event-data)

    :message-reaction-remove
    (rr/handle-reaction-remove! event-data)

    nil))
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

        ;; Reaction intent keyword varies by discljord version.
        reaction-intent (or (when (contains? c/gateway-intents :guild-message-reactions)
                              :guild-message-reactions)
                            (when (contains? c/gateway-intents :guild-message-reaction)
                              :guild-message-reaction)
                            (when (contains? c/gateway-intents :guild-message-reaction-add)
                              :guild-message-reaction-add)
                            (when (contains? c/gateway-intents :guild-message-reaction-events)
                              :guild-message-reaction-events))

        ;; Message content intent is privileged and ALSO must be enabled in the Discord dev portal.
        message-content-intent (or (when (contains? c/gateway-intents :message-content)
                                     :message-content)
                                   (when (contains? c/gateway-intents :message_content)
                                     :message_content)
                                   (when (contains? c/gateway-intents :message-content-intent)
                                     :message-content-intent)
                                   (when (contains? c/gateway-intents :message_content_intent)
                                     :message_content_intent))

        intents (cond-> #{:guilds :guild-messages}
                  reaction-intent (conj reaction-intent)
                  message-content-intent (conj message-content-intent))

        conn   (c/connect-bot! token event-ch :intents intents)
        msg-ch (m/start-connection! token)]

    (println "Gateway intents:" intents)
    (when-not reaction-intent
      (println "WARNING: Could not find a reaction intent keyword in discljord.connections/gateway-intents"))
    (when-not message-content-intent
      (println "WARNING: Could not find a message content intent keyword in discljord.connections/gateway-intents"))
    (when-not (contains? intents (or message-content-intent :message-content))
      (println "NOTE: If the bot connects but ignores commands, enable Message Content Intent in the Discord Developer Portal (Bot → Privileged Gateway Intents)."))

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

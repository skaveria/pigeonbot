(ns pigeonbot.discord
  (:require [clojure.core.async :as a]
            [discljord.connections :as c]
            [discljord.events :as e]
            [discljord.messaging :as m]
            [pigeonbot.commands :as commands]
            [pigeonbot.config :as config]
            [pigeonbot.channels :as chans]
            [pigeonbot.state :refer [state]]))

(defn handle-event [event-type event-data]
  (when (= event-type :message-create)
    (commands/handle-message event-data)))

(defn start-bot
  "Connect to Discord and start the event pump (blocking)."
  []
  (let [{:keys [token]} (config/load-config)
        event-ch (a/chan 100)
        conn     (c/connect-bot! token event-ch
                                 :intents #{:guilds :guild-messages})
        msg-ch   (m/start-connection! token)]

    ;; ✅ minimal: just go green
    (c/status-update! conn :status :online :shards :all)

    (reset! state {:connection conn
                   :events     event-ch
                   :messaging  msg-ch})

    (println "Connected to Discord (online)")
    (chans/load-channels)
    (e/message-pump! event-ch handle-event)))

(def bot-future (atom nil))

(defn start-bot!
  "Starts the bot in the background and returns the messaging handle
   for convenient use at the REPL."
  []
  (reset! bot-future
          (future
            (start-bot)))
  ;; wait briefly for :messaging to appear in state
  (loop [tries 0]
    (if-let [msg (:messaging @state)]
      msg
      (if (< tries 20)
        (do (Thread/sleep 250)
            (recur (inc tries)))
        (do
          (println "start-bot!: timed out waiting for messaging connection.")
          nil)))))

(defn stop-bot!
  "Best-effort stop of the running bot.
   Explicitly disconnects gateway + messaging to avoid zombie threads."
  []
  (let [{:keys [events connection messaging]} @state]
    (println "Stopping bot connections…")

    ;; Stop accepting events first.
    (when events
      (a/close! events))

    ;; Hard disconnect gateway websocket (stops the event stream).
    (when connection
      (try
        (c/disconnect-bot! connection)
        (catch Throwable t
          (println "stop-bot!: disconnect-bot! failed:" (.getMessage t)))))

    ;; Stop messaging connection thread(s).
    (when messaging
      (try
        (m/stop-connection! messaging)
        (catch Throwable t
          (println "stop-bot!: stop-connection! failed:" (.getMessage t))))))

  ;; Cancel the background future (if it’s still running).
  (when-let [f @bot-future]
    (future-cancel f))

  (reset! bot-future nil)
  (reset! state nil)
  (println "Bot stopped."))

(defn restart-bot!
  "Restart the Discord bot cleanly from the REPL."
  []
  (println "Restarting pigeonbot…")
  (stop-bot!)
  (Thread/sleep 500)
  (start-bot!))

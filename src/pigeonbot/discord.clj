
(ns pigeonbot.discord
  (:require [clojure.core.async :as a]
            [discljord.connections :as c]
            [discljord.events :as e]
            [discljord.messaging :as m]
            [pigeonbot.commands :as commands]
            [discljord.presence :as p]
            [pigeonbot.config :as config]
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

    (p/update-presence!
     conn
     {:status :online
      :activities [{:name "you closely"
                     :type :watching}]})

    (reset! state {:connection conn
                   :events     event-ch
                   :messaging  msg-ch})

    (println "Connected to Discord (online)")
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
        (do
          (Thread/sleep 250)
          (recur (inc tries)))
        (do
          (println "start-bot!: timed out waiting for messaging connection.")
          nil)))))

(defn stop-bot!
  "Best-effort stop of the running bot."
  []
  (when-let [{:keys [events]} @state]
    (when events
      (a/close! events))
    (println "Stopping bot connections…"))
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

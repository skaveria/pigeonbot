(ns pigeonbot.core
  (:gen-class)
  (:require [discljord.connections :as c]
            [discljord.messaging :as m]
            [discljord.events :as e]
            [clojure.edn :as edn]
            [clojure.core.async :as a]))

(def state (atom nil))

(defn load-config []
  (-> "config.edn" slurp edn/read-string))

;; --- event handling ---------------------------------------------------------

(defn handle-event
  [event-type event-data]
  (when (= event-type :message-create)
    (let [{:keys [content channel-id]} event-data]
      (when (= content "!ping")
        (m/create-message! (:messaging @state)
                           channel-id
                           :content "pong")))))

;; --- lifecycle --------------------------------------------------------------

(defn start-bot []
  (let [{:keys [token]} (load-config)
        event-ch (a/chan 100)
        conn-ch  (c/connect-bot! token event-ch
                                 :intents #{:guilds :guild-messages})
        msg-ch   (m/start-connection! token)]
    (reset! state {:connection conn-ch
                   :events     event-ch
                   :messaging  msg-ch})
    (println "Connected to Discord (waiting for events)…")
    (e/message-pump! event-ch handle-event)))


(defn -main [& _args]
  (start-bot))

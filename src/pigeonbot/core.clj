(ns pigeonbot.core
  (:gen-class)
  (:require [discljord.messaging :as m]
            [pigeonbot.discord :as discord]
            [pigeonbot.state :refer [state]]))

(defn echo!
  "Send a message to a channel from the REPL.
   channel-id: Discord channel ID (long)
   text: string content"
  [channel-id text]
  (when-let [messaging (:messaging @state)]
    (m/create-message! messaging
                       channel-id
                       :content text)))

(defn -main [& _args]
  (discord/start-bot))

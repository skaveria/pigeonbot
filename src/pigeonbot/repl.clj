(ns pigeonbot.repl
  (:require [discljord.messaging :as m]
            [pigeonbot.state :refer [state]]))

(defn say!
  "Send a message to a channel from the REPL."
  [channel-id text]
  (when-let [messaging (:messaging @state)]
    (m/create-message! messaging
                       (long channel-id)
                       :content (str text))))

(ns pigeonbot.state)
;; Runtime state for the bot process.
;; Keys (when running): :connection :events :messaging
(defonce state (atom {}))

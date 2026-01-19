(ns pigeonbot.state)

(def state
  "Runtime state for the bot process.
   Keys (when running): :connection :events :messaging"
  (atom nil))

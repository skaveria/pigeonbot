(ns pigeonbot.core
  (:gen-class)
  (:require [pigeonbot.discord :as discord]))

(defn -main [& _args]
  (discord/start-bot))

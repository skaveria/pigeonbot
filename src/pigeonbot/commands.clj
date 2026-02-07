(ns pigeonbot.commands
  (:require [pigeonbot.commands.dispatch :as dispatch]))

(def handle-message dispatch/handle-message)
(def command-descriptions dispatch/command-descriptions)

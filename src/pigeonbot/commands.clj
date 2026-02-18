(ns pigeonbot.commands
  (:require [clojure.string :as str]
            [pigeonbot.commands.dispatch :as dispatch]
            [pigeonbot.commands.ask :as ask]
            [pigeonbot.threads :as threads]))

(def command-descriptions
  "Expose command descriptions for help/docs."
  dispatch/command-descriptions)

(defn handle-message
  "Entry point for :message-create events.

  Normal behavior:
  - delegate to commands.dispatch

  Thread behavior (Option B):
  - if pigeonbot has spoken in a thread already (active thread),
    treat plain messages in that thread as implicit asks (no @ needed)."
  [{:keys [content id] :as msg}]
  (if (threads/should-auto-ask? msg)
    (do
      (ask/run-ask! msg (str/trim (str content)) (str id))
      true)
    (dispatch/handle-message msg)))

(ns pigeonbot.commands.dispatch
  (:require [clojure.string :as str]
            [pigeonbot.custom-commands :as custom]
            [pigeonbot.commands.registry :as reg]
            [pigeonbot.commands.util :as u]
            [pigeonbot.commands.ask :as ask]
            [pigeonbot.commands.builtins]
            [pigeonbot.commands.roles]
            [pigeonbot.commands.custom]
            [pigeonbot.commands.reacts]))

(def command-descriptions reg/command-descriptions)

(defn handle-message
  "Dispatch order:
  1) built-in commands (!ping, !ask, etc)
  2) custom commands (CDN URL-based)
  3) ask-like triggers (reply / @mention)"
  [{:keys [content channel-id] :as msg}]
  (let [cmd (first (str/split (or content "") #"\s+"))]
    (cond
      (when-let [cmd-fn (@reg/commands cmd)]
        (cmd-fn msg)
        true)
      nil

      (when-let [r (custom/registered-reply cmd)]
        (case (:type r)
          :url  (do (u/send! channel-id :content (:url r)) true)
          :file (do (u/send-file! channel-id (io/file "src/pigeonbot/media" (:file r))) true)
          nil))
      nil

      (ask/handle-ask-like! msg)
      true

      :else nil)))

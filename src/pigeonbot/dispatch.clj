(ns pigeonbot.commands.dispatch
  (:require [clojure.java.io :as io]
            [clojure.string :as str]
            [pigeonbot.custom-commands :as custom]
            [pigeonbot.commands.registry :as reg]
            [pigeonbot.commands.util :as u]
            [pigeonbot.commands.ask :as ask]
            [pigeonbot.threads :as threads]
            ;; load modules for side-effect registration via defcmd
            [pigeonbot.commands.builtins]
            [pigeonbot.commands.roles]
            [pigeonbot.commands.custom]
            [pigeonbot.commands.vision]
            [pigeonbot.commands.reacts]))

(def command-descriptions reg/command-descriptions)

(defn handle-message
  "Dispatch order:
  1) built-in commands (!ping, !ask, etc)
  2) custom commands (CDN URL-based)
  3) thread continuation: if pigeonbot has spoken in this thread, treat new messages as ask
  4) ask-like triggers (reply / @mention)"
  [{:keys [content channel-id id] :as msg}]
  (let [cmd (first (str/split (or content "") #"\s+"))]
    (cond
      ;; 1) built-ins
      (when-let [cmd-fn (@reg/commands cmd)]
        (cmd-fn msg)
        true)
      nil

      ;; 2) custom commands
      (when-let [r (custom/registered-reply cmd)]
        (case (:type r)
          :url (do (u/send! channel-id :content (:url r)) true)

          ;; back-compat if you still have old file-based custom commands
          :file (let [f (io/file "src/pigeonbot/media" (:file r))]
                  (when (.exists f)
                    (u/send-file! channel-id f))
                  true)
          nil))
      nil

      ;; 3) thread continuation (no @ needed)
      (threads/should-auto-ask? msg)
      (do
        ;; Reply to the message in-thread so it stays tidy.
        (ask/run-ask! msg (str/trim (or content "")) (str id))
        true)

      ;; 4) ask-like triggers (reply / @mention)
      (ask/handle-ask-like! msg)
      true

      :else
      nil)))

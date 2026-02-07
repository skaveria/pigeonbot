(ns pigeonbot.commands.roles
  (:require [clojure.string :as str]
            [pigeonbot.roles :as roles]
            [pigeonbot.commands.registry :refer [defcmd]]
            [pigeonbot.commands.util :as u]))

(defn- cmd-role-add [{:keys [channel-id guild-id author content]}]
  (let [[_ _ role-id] (str/split (or content "") #"\s+" 3)
        user-id (get-in author [:id])]
    (if (and guild-id user-id role-id)
      (let [{:keys [ok? reason]} (roles/add-role! guild-id user-id role-id)]
        (u/send! channel-id :content
                 (case reason
                   :not-allowed "That role is not self-assignable."
                   :no-messaging "Bot is not ready."
                   (if ok? "Role added ✅" "Failed to add role."))))
      (u/send! channel-id :content "Usage: !role add <ROLE_ID>"))))

(defn- cmd-role-remove [{:keys [channel-id guild-id author content]}]
  (let [[_ _ role-id] (str/split (or content "") #"\s+" 3)
        user-id (get-in author [:id])]
    (if (and guild-id user-id role-id)
      (let [{:keys [ok? reason]} (roles/remove-role! guild-id user-id role-id)]
        (u/send! channel-id :content
                 (case reason
                   :not-allowed "That role is not self-assignable."
                   :no-messaging "Bot is not ready."
                   (if ok? "Role removed ✅" "Failed to remove role."))))
      (u/send! channel-id :content "Usage: !role remove <ROLE_ID>"))))

(defcmd "!role" "Self-assignable roles: !role add <ROLE_ID> | !role remove <ROLE_ID>"
  [{:keys [content] :as msg}]
  (let [[_ subcmd] (str/split (or content "") #"\s+" 3)]
    (case subcmd
      "add"    (cmd-role-add msg)
      "remove" (cmd-role-remove msg)
      (u/send! (:channel-id msg) :content "Usage: !role add <ROLE_ID> | !role remove <ROLE_ID>"))))

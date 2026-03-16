(ns pigeonbot.commands.custom
  (:require [clojure.string :as str]
            [pigeonbot.custom-commands :as custom]
            [pigeonbot.commands.registry :refer [defcmd commands]]
            [pigeonbot.commands.util :as u]))

(defcmd "!registercommand" "Register custom command: !registercommand <name> <url>  (attachment still supported)"
  [{:keys [channel-id content attachments author] :as msg}]
  (cond
    (not (custom/allowed-to-register? msg))
    (u/send! channel-id :content "❌ You’re not allowed to register commands.")

    :else
    (let [[_ name maybe-url] (str/split (or content "") #"\s+" 3)]
      (cond
        (not (custom/valid-name? name))
        (u/send! channel-id :content "Usage: !registercommand <name> <url>  (or attach a file)")

        (contains? @commands (custom/normalize-command name))
        (u/send! channel-id :content (str "❌ `" (custom/normalize-command name) "` is built-in; can’t override."))

        (and (seq maybe-url) (custom/valid-url? maybe-url))
        (let [cmd (custom/normalize-command name)
              author-id (get-in author [:id])
              {:keys [ok? message]} (custom/register-from-url! cmd maybe-url author-id)]
          (if ok?
            (u/send! channel-id :content (str "✅ Registered `" cmd "` → " maybe-url))
            (u/send! channel-id :content (str "❌ " message))))

        (seq attachments)
        (let [cmd (custom/normalize-command name)
              att (first attachments)
              author-id (get-in author [:id])
              {:keys [ok? message]} (custom/register-from-attachment! cmd att author-id)]
          (if ok?
            (u/send! channel-id :content (str "✅ Registered `" cmd "` (attachment URL saved)."))
            (u/send! channel-id :content (str "❌ " message))))

        :else
        (u/send! channel-id :content "Usage: !registercommand <name> <url>  (or attach a file)")))))

(defcmd "!delcommand" "Delete custom command: !delcommand <name>"
  [{:keys [channel-id content] :as msg}]
  (if-not (custom/allowed-to-register? msg)
    (u/send! channel-id :content "❌ You’re not allowed to delete commands.")
    (let [[_ name] (str/split (or content "") #"\s+" 2)]
      (if-not (custom/valid-name? name)
        (u/send! channel-id :content "Usage: !delcommand <name>")
        (let [cmd (custom/normalize-command name)
              existed? (custom/delete! cmd)]
          (u/send! channel-id :content (if existed?
                                         (str "🗑️ Deleted `" cmd "`.")
                                         (str "No such custom command `" cmd "`."))))))))

(defcmd "!renamecommand" "Rename custom command: !renamecommand <old> <new>"
  [{:keys [channel-id content] :as msg}]
  (if-not (custom/allowed-to-register? msg)
    (u/send! channel-id :content "❌ You’re not allowed to rename commands.")
    (let [[_ old-name new-name] (str/split (or content "") #"\s+" 3)]
      (cond
        (or (not (custom/valid-name? old-name))
            (not (custom/valid-name? new-name)))
        (u/send! channel-id :content "Usage: !renamecommand <old> <new>")

        :else
        (let [old-cmd (custom/normalize-command old-name)
              new-cmd (custom/normalize-command new-name)
              {:keys [ok? message]} (custom/rename! old-cmd new-cmd)]
          (u/send! channel-id :content (if ok?
                                        (str "✅ Renamed `" old-cmd "` → `" new-cmd "`.")
                                        (str "❌ " message))))))))

(defcmd "!listcommands" "List commands (built-ins + custom)."
  [{:keys [channel-id]}]
  (let [builtins (->> (keys @commands) sort)
        customs  (custom/list-commands)
        text (u/clamp-discord
              (str "**Built-ins (" (count builtins) "):** "
                   (if (seq builtins) (str/join ", " builtins) "(none)")
                   "\n"
                   "**Custom (" (count customs) "):** "
                   (if (seq customs) (str/join ", " customs) "(none)")))]
    (u/send! channel-id :content text)))

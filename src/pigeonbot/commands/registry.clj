(ns pigeonbot.commands.registry
  (:require [clojure.string :as str]))

(def command-descriptions
  (atom {"!ping" "Replies with pong."
         "!help" "Shows this help message."
         "!ask"  "Ask pigeonbot a question (also works by replying to me / @mentioning me)."
         "!role" "Self-assignable roles: !role add <ROLE_ID> | !role remove <ROLE_ID>"
         "!registercommand" "Register custom command: !registercommand <name> + attach"
         "!delcommand" "Delete custom command: !delcommand <name>"
         "!renamecommand" "Rename custom command: !renamecommand <old> <new>"
         "!listcommands" "List commands (built-ins + custom)."
         "!registerreact" "Register react: !registerreact \"trigger\" \"reply\" OR attach"
         "!editreact" "Edit react text: !editreact \"trigger\" \"new reply\""
         "!delreact" "Delete react: !delreact \"trigger\""
         "!listreacts" "List reacts."
         "!clearreacts" "Delete ALL reacts (admin-only)."}))

(def commands
  "Map of command string -> handler fn."
  (atom {}))

(defn register-command!
  [cmd desc f]
  (swap! commands assoc cmd f)
  (when (and cmd desc)
    (swap! command-descriptions assoc cmd desc))
  f)

(defmacro defcmd [cmd desc argv & body]
  (let [fname (symbol (str "cmd" (str/replace cmd #"[^a-zA-Z0-9]+" "-")))]
    `(do
       (defn ~fname ~argv ~@body)
       (register-command! ~cmd ~desc ~fname))))

(defmacro defmedia [cmd desc filename]
  `(defcmd ~cmd ~desc [msg#]
     (pigeonbot.commands.util/send-file! (:channel-id msg#)
                                         (pigeonbot.commands.util/media-file ~filename))))

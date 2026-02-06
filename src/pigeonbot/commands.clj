(ns pigeonbot.commands
  (:require [clojure.core.async :as async]
            [clojure.java.io :as io]
            [clojure.string :as str]
            [discljord.messaging :as m]
            [pigeonbot.roles :as roles]
            [pigeonbot.ollama :as ollama]
            [pigeonbot.state :refer [state]]))

;; -----------------------------------------------------------------------------
;; Media
;; -----------------------------------------------------------------------------

(def ^:private media-root
  "Local media directory (dev-time path)."
  "src/pigeonbot/media/")

(defn media-file
  "Return a java.io.File for a media asset under src/pigeonbot/media/."
  [filename]
  (java.io.File. (str media-root filename)))

(defn- temp-copy
  "Copy a file to a temp path and return the temp File.

  Workaround: Some attachments (notably our wimdy.gif) can wedge discljord's
  request pipeline when uploaded from the original path. Sending a fresh temp
  file reliably avoids the hang."
  [^java.io.File f ^String name]
  (let [tmp (java.io.File/createTempFile "pigeonbot-" (str "-" name))]
    (io/copy f tmp)
    (.deleteOnExit tmp)
    tmp))

;; -----------------------------------------------------------------------------
;; Sending
;; -----------------------------------------------------------------------------

(defn send!
  "Safely send a Discord message. Returns a response channel or nil.

  When the bot isn't fully connected yet, :messaging may be nil; in that case we
  print a note and return nil (callers should handle nil)."
  [channel-id & {:keys [content file] :or {content ""}}]
  (if-let [messaging (:messaging @state)]
    (m/create-message! messaging
                       channel-id
                       :content (or content "")
                       :file file)
    (do
      (println "send!: messaging connection is nil (bot not ready?)")
      nil)))

;; -----------------------------------------------------------------------------
;; Helpers
;; -----------------------------------------------------------------------------

(def ^:private discord-max-chars
  "Discord hard limit is 2000 characters per message."
  2000)

(defn- clamp-discord
  "Clamp content to Discord-safe length, leaving room for ellipsis."
  [s]
  (let [s (str (or s ""))]
    (if (<= (count s) discord-max-chars)
      s
      (str (subs s 0 (- discord-max-chars 1)) "…"))))

(defn- strip-command
  "Given full message content, remove the first token (command) and return the rest."
  [content]
  (let [content (or content "")]
    (-> content
        (str/replace-first #"^\s*\S+\s*" "") ;; remove first word + following whitespace
        (str/trim))))

(defn await!!
  "Blocking await for discljord response channels with a timeout.

  Returns:
  - ::no-channel if ch is nil (e.g. bot not ready)
  - ::timeout if ms elapses
  - otherwise the value delivered on ch (may be nil)"
  [ch ms]
  (cond
    (nil? ch) ::no-channel
    :else
    (let [[v port] (async/alts!! [ch (async/timeout ms)])]
      (if (= port ch) v ::timeout))))

;; -----------------------------------------------------------------------------
;; Command help
;; -----------------------------------------------------------------------------

(def command-descriptions
  {"!ping"        "Replies with pong."
   "!help"        "Shows this help message."
   "!odinthewise" "Posts the Odin the Wise image."
   "!partycat"    "Posts the Partycat image."
   "!slcomputers" "Posts the Dr Strangelove computers gif."
   "!wimdy"       "Posts the wimdy gif."
   "!ask"         "Ask pigeonbot a question."
   "!role"        "Self-assignable roles: !role add <ROLE_ID> | !role remove <ROLE_ID>"})

;; -----------------------------------------------------------------------------
;; Commands
;; -----------------------------------------------------------------------------

(defn cmd-ping [{:keys [channel-id]}]
  (send! channel-id :content "pong"))

(defn cmd-help [{:keys [channel-id]}]
  (let [help-text (->> command-descriptions
                       (map (fn [[cmd desc]] (str cmd " — " desc)))
                       (str/join "\n"))]
    (send! channel-id :content help-text)))

(defn cmd-odinthewise [{:keys [channel-id]}]
  (send! channel-id :content "" :file (media-file "odinthewise.png")))

(defn cmd-partycat [{:keys [channel-id]}]
  (send! channel-id :content "" :file (media-file "partycat.png")))

(defn cmd-slcomputers [{:keys [channel-id]}]
  (send! channel-id :content "" :file (media-file "slcomputers.gif")))

(defn cmd-wimdy
  "Post the wimdy gif.

  NOTE: This uses a temp-copy workaround because uploading wimdy.gif directly
  can hang discljord's request pipeline (observed in REPL tests)."
  [{:keys [channel-id]}]
  (let [f (temp-copy (media-file "wimdy.gif") "wimdy.gif")]
    (send! channel-id :content "" :file f)))

(defn cmd-ask
  "Ask Geof (Ollama) a question. Non-blocking via future."
  [{:keys [channel-id content]}]
  (let [question (strip-command content)]
    (if (str/blank? question)
      (send! channel-id :content "Usage: !ask <your question>")
      (do
        (send! channel-id :content "Hm. Lemme think…")
        (future
          (try
            (let [reply (-> (ollama/geof-ask question)
                            clamp-discord)]
              (send! channel-id :content reply))
            (catch Throwable t
              (println "cmd-ask error:" (.getMessage t))
              (send! channel-id :content "Listen here—something went sideways talking to my brain-box."))))))))

;; Roles: these were defined but not wired previously; now routed under !role.
(defn cmd-role-add [{:keys [channel-id guild-id author content]}]
  (let [[_ _ role-id] (str/split (or content "") #"\s+" 3)
        user-id (get-in author [:id])]
    (if (and guild-id user-id role-id)
      (let [{:keys [ok? reason]} (roles/add-role! guild-id user-id role-id)]
        (send! channel-id
               :content
               (case reason
                 :not-allowed "That role is not self-assignable."
                 :no-messaging "Bot is not ready."
                 (if ok? "Role added ✅" "Failed to add role."))))
      (send! channel-id :content "Usage: !role add <ROLE_ID>"))))

(defn cmd-role-remove [{:keys [channel-id guild-id author content]}]
  (let [[_ _ role-id] (str/split (or content "") #"\s+" 3)
        user-id (get-in author [:id])]
    (if (and guild-id user-id role-id)
      (let [{:keys [ok? reason]} (roles/remove-role! guild-id user-id role-id)]
        (send! channel-id
               :content
               (case reason
                 :not-allowed "That role is not self-assignable."
                 :no-messaging "Bot is not ready."
                 (if ok? "Role removed ✅" "Failed to remove role."))))
      (send! channel-id :content "Usage: !role remove <ROLE_ID>"))))

(defn cmd-role
  "Dispatch role subcommands:
   !role add <ROLE_ID>
   !role remove <ROLE_ID>"
  [{:keys [content] :as msg}]
  (let [[_ subcmd] (str/split (or content "") #"\s+" 3)]
    (case subcmd
      "add"    (cmd-role-add msg)
      "remove" (cmd-role-remove msg)
      (send! (:channel-id msg)
             :content "Usage: !role add <ROLE_ID> | !role remove <ROLE_ID>"))))

;; -----------------------------------------------------------------------------
;; Routing
;; -----------------------------------------------------------------------------

(def commands
  {"!ping"        cmd-ping
   "!help"        cmd-help
   "!odinthewise" cmd-odinthewise
   "!partycat"    cmd-partycat
   "!slcomputers" cmd-slcomputers
   "!wimdy"       cmd-wimdy
   "!ask"         cmd-ask
   "!role"        cmd-role})

(defn handle-message
  "Dispatch a Discord message map to a command handler."
  [{:keys [content] :as msg}]
  (let [cmd (first (str/split (or content "") #"\s+"))]
    (when-let [cmd-fn (commands cmd)]
      (cmd-fn msg))))

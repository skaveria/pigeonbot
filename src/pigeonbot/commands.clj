(ns pigeonbot.commands
  (:require [clojure.core.async :as async]
            [clojure.java.io :as io]
            [clojure.string :as str]
            [discljord.messaging :as m]
            [pigeonbot.roles :as roles]
            [pigeonbot.ollama :as ollama]
            [pigeonbot.state :refer [state]]))

;; -----------------------------------------------------------------------------
;; Config
;; -----------------------------------------------------------------------------

(def ^:private discord-max-chars
  "Discord hard limit is 2000 characters per message."
  2000)

(def ^:private upload-timeout-ms
  "How long we wait for discljord to deliver a response for an attachment send
   before we fail-soft."
  5000)

(def ^:private media-root
  "Local media directory (dev-time path)."
  "src/pigeonbot/media/")

;; -----------------------------------------------------------------------------
;; Low-level helpers
;; -----------------------------------------------------------------------------

(defn media-file
  "Return a java.io.File for a media asset under src/pigeonbot/media/."
  [filename]
  (java.io.File. (str media-root filename)))

(defn- temp-copy
  "Copy a file to a temp path and return the temp File.
   Workaround for attachment edge cases (fresh File object/path)."
  [^java.io.File f ^String name]
  (let [tmp (java.io.File/createTempFile "pigeonbot-" (str "-" name))]
    (io/copy f tmp)
    (.deleteOnExit tmp)
    tmp))

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
        (str/replace-first #"^\s*\S+\s*" "")
        (str/trim))))

;; -----------------------------------------------------------------------------
;; Robust attachment sending (fail-soft)
;; -----------------------------------------------------------------------------

(defn- send-file!
  "Send a local file attachment robustly.
   - temp-copies the file (avoids some multipart/path edge cases)
   - logs + fails-soft if request never completes

  Returns the discljord response channel if we initiated a send, otherwise nil."
  [channel-id ^java.io.File f]
  (let [path (.getAbsolutePath f)]
    (cond
      (nil? f)
      (do (println "send-file!: file is nil") nil)

      (not (.exists f))
      (do (println "send-file!: missing file" {:file path})
          (send! channel-id :content (str "⚠️ Missing media file: `" (.getName f) "`"))
          nil)

      :else
      (let [tmp (temp-copy f (.getName f))
            ch  (send! channel-id :content "" :file tmp)]
        ;; Fire-and-forget: observe completion and fail-soft on timeout.
        (async/go
          (cond
            (nil? ch)
            (println "send-file!: no channel (bot not ready?)" {:file path})

            :else
            (let [[resp port] (async/alts! [ch (async/timeout upload-timeout-ms)])]
              (when-not (= port ch)
                (println "send-file!: TIMEOUT sending attachment"
                         {:file path
                          :size (.length f)})
                (send! channel-id
                       :content (str "⚠️ I tried to upload `" (.getName f) "` but Discord didn’t respond. "
                                     "Try again in a sec, or ask an admin to re-encode the file."))))))
        ch))))

;; -----------------------------------------------------------------------------
;; Command registry + macro sugar
;; -----------------------------------------------------------------------------

(def command-descriptions
  {"!ping"        "Replies with pong."
   "!help"        "Shows this help message."
   "!ask"         "Ask pigeonbot a question."
   "!role"        "Self-assignable roles: !role add <ROLE_ID> | !role remove <ROLE_ID>"})

(def commands
  "Map of command string -> handler fn."
  (atom {}))

(defn- register-command!
  "Register a command handler and its help description."
  [cmd desc f]
  (swap! commands assoc cmd f)
  (when (and cmd desc)
    (alter-var-root #'command-descriptions assoc cmd desc))
  f)

(defmacro defcmd
  "Define and register a command handler.

  (defcmd \"!ping\" \"Replies with pong.\" [msg] ...)"
  [cmd desc argv & body]
  (let [fname (symbol (str "cmd" (str/replace cmd #"[^a-zA-Z0-9]+" "-")))]
    `(do
       (defn ~fname ~argv ~@body)
       (register-command! ~cmd ~desc ~fname))))

(defmacro defmedia
  "Define and register a simple media command.

  (defmedia \"!wimdy\" \"Posts the wimdy gif.\" \"wimdy.gif\")"
  [cmd desc filename]
  `(defcmd ~cmd ~desc [{:keys [channel-id]}]
     (send-file! channel-id (media-file ~filename))))

;; -----------------------------------------------------------------------------
;; Built-in commands
;; -----------------------------------------------------------------------------

(defcmd "!ping" "Replies with pong."
  [{:keys [channel-id]}]
  (send! channel-id :content "pong"))

(defcmd "!help" "Shows this help message."
  [{:keys [channel-id]}]
  (let [help-text (->> command-descriptions
                       (map (fn [[cmd desc]] (str cmd " — " desc)))
                       (str/join "\n"))]
    (send! channel-id :content help-text)))

(defcmd "!ask" "Ask pigeonbot a question."
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

;; Media commands (simple + consistent)
(defmedia "!odinthewise" "Posts the Odin the Wise image." "odinthewise.png")
(defmedia "!partycat"   "Posts the Partycat image."      "partycat.png")
(defmedia "!slcomputers" "Posts the Dr Strangelove computers gif." "slcomputers.gif")
(defmedia "!wimdy"      "Posts the wimdy gif."           "wimdy.gif")

;; -----------------------------------------------------------------------------
;; Role commands
;; -----------------------------------------------------------------------------

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

(defcmd "!role" "Self-assignable roles: !role add <ROLE_ID> | !role remove <ROLE_ID>"
  [{:keys [content] :as msg}]
  (let [[_ subcmd] (str/split (or content "") #"\s+" 3)]
    (case subcmd
      "add"    (cmd-role-add msg)
      "remove" (cmd-role-remove msg)
      (send! (:channel-id msg)
             :content "Usage: !role add <ROLE_ID> | !role remove <ROLE_ID>"))))

;; -----------------------------------------------------------------------------
;; Dispatch
;; -----------------------------------------------------------------------------

(defn handle-message
  "Dispatch a Discord message map to a command handler."
  [{:keys [content] :as msg}]
  (let [cmd (first (str/split (or content "") #"\s+"))]
    (when-let [cmd-fn (@commands cmd)]
      (cmd-fn msg))))

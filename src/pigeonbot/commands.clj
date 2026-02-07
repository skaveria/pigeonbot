(ns pigeonbot.commands
  (:require [clojure.core.async :as async]
            [clojure.java.io :as io]
            [clojure.string :as str]
            [discljord.messaging :as m]
            [pigeonbot.context :as ctx]
            [pigeonbot.custom-commands :as custom]
            [pigeonbot.message-reacts :as reacts]
            [pigeonbot.ollama :as ollama]
            [pigeonbot.roles :as roles]
            [pigeonbot.state :refer [state]]))

(def ^:private discord-max-chars 2000)
(def ^:private upload-timeout-ms 5000)

(def ^:private media-root "src/pigeonbot/media/")

(defn media-file [filename]
  (java.io.File. (str media-root filename)))

(defn- temp-copy [^java.io.File f ^String name]
  (let [tmp (java.io.File/createTempFile "pigeonbot-" (str "-" name))]
    (io/copy f tmp)
    (.deleteOnExit tmp)
    tmp))

(defn send!
  "Safely send a Discord message. Returns a response channel or nil."
  [channel-id & {:keys [content file allowed-mentions]
                 :or {content ""}}]
  (if-let [messaging (:messaging @state)]
    (m/create-message! messaging
                       channel-id
                       :content (or content "")
                       :file file
                       :allowed_mentions allowed-mentions)
    (do (println "send!: messaging connection is nil (bot not ready?)") nil)))

(defn- clamp-discord [s]
  (let [s (str (or s ""))]
    (if (<= (count s) discord-max-chars)
      s
      (str (subs s 0 (- discord-max-chars 1)) "…"))))

(defn- send-file!
  "Built-in local media only."
  [channel-id ^java.io.File f]
  (let [path (some-> f .getAbsolutePath)]
    (cond
      (nil? f) (do (println "send-file!: file is nil") nil)
      (not (.exists f))
      (do (println "send-file!: missing file" {:file path})
          (send! channel-id :content (str "⚠️ Missing media file: `" (.getName f) "`"))
          nil)
      :else
      (let [tmp (temp-copy f (.getName f))
            ch  (send! channel-id :content "" :file tmp)]
        (async/go
          (when (nil? ch)
            (println "send-file!: no channel (bot not ready?)" {:file path}))
          (when ch
            (let [[_resp port] (async/alts! [ch (async/timeout upload-timeout-ms)])]
              (when-not (= port ch)
                (println "send-file!: TIMEOUT sending attachment" {:file path :size (.length f)})))))
        ch))))

;; -----------------------------------------------------------------------------
;; Registry + macros
;; -----------------------------------------------------------------------------

(def command-descriptions
  (atom {"!ping" "Replies with pong."
         "!help" "Shows this help message."
         "!ask" "Ask pigeonbot a question."
         "!role" "Self-assignable roles: !role add <ROLE_ID> | !role remove <ROLE_ID>"
         "!registercommand" "Register custom command: !registercommand <name> + attach"
         "!registerreact" "Register react: !registerreact \"trigger\" \"reply\" OR attach"}))

(def commands (atom {}))

(defn- register-command! [cmd desc f]
  (swap! commands assoc cmd f)
  (when (and cmd desc) (swap! command-descriptions assoc cmd desc))
  f)

(defmacro defcmd [cmd desc argv & body]
  (let [fname (symbol (str "cmd" (str/replace cmd #"[^a-zA-Z0-9]+" "-")))]
    `(do (defn ~fname ~argv ~@body)
         (register-command! ~cmd ~desc ~fname))))

(defmacro defmedia [cmd desc filename]
  `(defcmd ~cmd ~desc [msg#]
     (send-file! (:channel-id msg#) (media-file ~filename))))

;; -----------------------------------------------------------------------------
;; Built-ins
;; -----------------------------------------------------------------------------

(defcmd "!ping" "Replies with pong."
  [{:keys [channel-id]}]
  (send! channel-id :content "pong"))

(defcmd "!help" "Shows this help message."
  [{:keys [channel-id]}]
  (let [help-text (->> @command-descriptions
                       (sort-by key)
                       (map (fn [[cmd desc]] (str cmd " — " desc)))
                       (str/join "\n")
                       clamp-discord)]
    (send! channel-id :content help-text)))

(defn- build-ask-context
  [{:keys [channel-id id]}]
  (-> (ctx/recent-messages channel-id id)
      ctx/format-context))

(defn- run-ask!
  "Core ask runner used by !ask, @mention, and reply-to-bot."
  [{:keys [channel-id content id] :as msg} question]
  (let [question (str/trim (or question ""))]
    (if (str/blank? question)
      (send! channel-id :content "Usage: !ask <your question>  (or reply to me / @mention me)")
      (do
        (send! channel-id :content "Hm. Lemme think…")
        (future
          (try
            (let [ctx-text (build-ask-context msg)
                  reply (ollama/geof-ask-with-context ctx-text question)
                  reply (clamp-discord reply)]
              (send! channel-id :content reply))
            (catch Throwable t
              (println "ask error:" (.getMessage t))
              (send! channel-id :content "Listen here—something went sideways talking to my brain-box."))))))))

(defcmd "!ask" "Ask pigeonbot a question."
  [{:keys [content] :as msg}]
  (let [question (-> (or content "")
                     (str/replace-first #"^\s*\S+\s*" "")
                     str/trim)]
    (run-ask! msg question)))

;; Built-in local media
(defmedia "!odinthewise" "Posts the Odin the Wise image." "odinthewise.png")
(defmedia "!partycat" "Posts the Partycat image." "partycat.png")
(defmedia "!slcomputers" "Posts the Dr Strangelove computers gif." "slcomputers.gif")
(defmedia "!wimdy" "Posts the wimdy gif." "wimdy.gif")

;; Roles (unchanged)
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
                 (if ok? "Role removed ✅" "Failed to add role."))))
      (send! channel-id :content "Usage: !role remove <ROLE_ID>"))))

(defcmd "!role" "Self-assignable roles: !role add <ROLE_ID> | !role remove <ROLE_ID>"
  [{:keys [content] :as msg}]
  (let [[_ subcmd] (str/split (or content "") #"\s+" 3)]
    (case subcmd
      "add"    (cmd-role-add msg)
      "remove" (cmd-role-remove msg)
      (send! (:channel-id msg) :content "Usage: !role add <ROLE_ID> | !role remove <ROLE_ID>"))))

;; -----------------------------------------------------------------------------
;; Ask-like triggers (mention / reply)
;; -----------------------------------------------------------------------------

(defn- bot-mentioned?
  "True if the bot is mentioned in this message."
  [{:keys [mentions]}]
  (let [bot-id (:bot-user-id @state)]
    (boolean
     (some (fn [m]
             (or (true? (:bot m))
                 (and bot-id (= (str (:id m)) (str bot-id)))))
           (or mentions [])))))

(defn- strip-leading-mention
  "Remove a leading <@id> or <@!id> mention token if present."
  [s]
  (-> (or s "")
      (str/replace-first #"^\s*<@!?\d+>\s*" "")
      str/trim))

(defn- reply-to-bot?
  "Discord may include :referenced_message with author info for replies."
  [{:keys [referenced_message]}]
  (true? (get-in referenced_message [:author :bot])))

(defn handle-ask-like!
  "If msg is a reply to the bot, or mentions the bot, treat it like !ask.
  Returns true if handled."
  [{:keys [content] :as msg}]
  (cond
    (reply-to-bot? msg)
    (do (run-ask! msg content) true)

    (bot-mentioned? msg)
    (let [q (strip-leading-mention content)]
      (when-not (str/blank? q)
        (run-ask! msg q)
        true))

    :else
    nil))

;; -----------------------------------------------------------------------------
;; Custom + react registration are assumed present in your current file.
;; (Keeping this file focused on the new behavior; your existing
;;  !registercommand / !registerreact can remain as-is or be merged in.)
;;
;; If you want, I can merge your current working register/list/delete
;; management commands into this file verbatim.
;; -----------------------------------------------------------------------------

(defn handle-message
  "Dispatch built-ins, then custom commands, then ask-like triggers."
  [{:keys [content channel-id] :as msg}]
  (let [cmd (first (str/split (or content "") #"\s+"))]
    (cond
      (when-let [cmd-fn (@commands cmd)]
        (cmd-fn msg)
        true)
      nil

      ;; Custom commands: CDN URL-based
      (when-let [r (custom/registered-reply cmd)]
        (case (:type r)
          :url  (do (send! channel-id :content (:url r)) true)
          :file (let [f (io/file "src/pigeonbot/media" (:file r))]
                  (when (.exists f)
                    (send-file! channel-id f)
                    true))
          nil))
      nil

      ;; Ask-like triggers if message is reply/mention
      (handle-ask-like! msg)
      true

      :else nil)))

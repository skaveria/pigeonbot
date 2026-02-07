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

(defn send-reply!
  "Send a Discord reply to a specific message-id in the same channel.
  Does not ping the replied user by default."
  [channel-id reply-to-message-id & {:keys [content file]}]
  (if-let [messaging (:messaging @state)]
    (m/create-message! messaging
                       channel-id
                       :content (or content "")
                       :file file
                       :message_reference {:message_id (str reply-to-message-id)}
                       :allowed_mentions {:replied_user false})
    (do
      (println "send-reply!: messaging connection is nil (bot not ready?)")
      nil)))

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
  "Built-in local media only. Custom uploads use CDN URLs."
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
          (cond
            (nil? ch) (println "send-file!: no channel (bot not ready?)" {:file path})
            :else
            (let [[resp port] (async/alts! [ch (async/timeout upload-timeout-ms)])]
              (cond
                (= port ch)
                (when (and (map? resp) (:error resp))
                  (println "send-file!: discljord error:" (pr-str resp)))
                :else
                (do
                  (println "send-file!: TIMEOUT sending attachment" {:file path :size (.length f)})
                  (send! channel-id
                         :content (str "⚠️ Upload timed out for `" (.getName f) "`. "
                                       "Try again or re-encode the file.")))))))
        ch))))

;; -----------------------------------------------------------------------------
;; Registry + macros
;; -----------------------------------------------------------------------------

(def command-descriptions
  (atom {"!ping" "Replies with pong."
         "!help" "Shows this help message."
         "!ask" "Ask pigeonbot a question (also works by replying to me / @mentioning me)."
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
;; Ask pipeline (the extensible core)
;; -----------------------------------------------------------------------------

(defn- strip-command-text
  "Remove the leading command token from a message string."
  [content]
  (-> (or content "")
      (str/replace-first #"^\s*\S+\s*" "")
      str/trim))

(defn- strip-leading-mention
  "Remove a leading <@id> or <@!id> mention token if present."
  [s]
  (-> (or s "")
      (str/replace-first #"^\s*<@!?\d+>\s*" "")
      str/trim))

(defn- build-ask-context
  "Build context text for the LLM using last N messages from this channel.
  Excludes the current message id."
  [{:keys [channel-id id]}]
  (-> (ctx/recent-messages channel-id id)
      ctx/format-context))

(defn- run-ask!
  "Single entrypoint for all 'ask-like' behaviors.
  If reply-to-id is provided, respond using Discord reply feature."
  ([msg question]
   (run-ask! msg question nil))
  ([{:keys [channel-id] :as msg} question reply-to-id]
   (let [question (str/trim (or question ""))]
     (if (str/blank? question)
       (send! channel-id :content "Usage: !ask <your question>  (or reply to me / @mention me)")
       (do
         ;; quick ack (reply-style if we're replying)
         (if reply-to-id
           (send-reply! channel-id reply-to-id :content "Hm. Lemme think…")
           )

         (future
           (try
             (let [context-text (build-ask-context msg)
                   reply (ollama/geof-ask-with-context context-text question)
                   reply (clamp-discord reply)]
               (if reply-to-id
                 (send-reply! channel-id reply-to-id :content reply)
                 (send! channel-id :content reply)))
             (catch Throwable t
               (println "ask error:" (.getMessage t))
               (if reply-to-id
                 (send-reply! channel-id reply-to-id :content "Listen here—something went sideways talking to my brain-box.")
                 (send! channel-id :content "Listen here—something went sideways talking to my brain-box."))))))))))

(defn- reply-to-bot?
  "Discord replies include :referenced_message; if that referenced author is a bot,
  treat this as an ask-like message."
  [msg]
  (true? (get-in msg [:referenced_message :author :bot])))

(defn- bot-id
  "Return pigeonbot's user id as a string, if known."
  []
  (some-> (:bot-user-id @state) str))

(defn- mentioned-pigeonbot?
  "True if this message mentions pigeonbot specifically (by id)."
  [{:keys [mentions]}]
  (when-let [bid (bot-id)]
    (boolean
     (some (fn [m] (= (str (:id m)) bid))
           (or mentions [])))))

(defn- message-starts-with-pigeonbot-mention?
  "True if content begins with <@id> or <@!id> for pigeonbot."
  [content]
  (when-let [bid (bot-id)]
    (boolean (re-find (re-pattern (str "^\\s*<@!?" (java.util.regex.Pattern/quote bid) ">\\s*"))
                      (or content "")))))

(defn- strip-leading-pigeonbot-mention
  "Remove a leading pigeonbot mention token from content."
  [s]
  (if-let [bid (bot-id)]
    (-> (or s "")
        (str/replace-first (re-pattern (str "^\\s*<@!?" (java.util.regex.Pattern/quote bid) ">\\s*")) "")
        str/trim)
    (str/trim (or s ""))))

(defn handle-ask-like!
  "If msg is a reply to the bot or mentions the bot, treat it like !ask.
  If the author is a bot, reply using Discord's reply feature."
  [{:keys [content id] :as msg}]
  (cond
    (reply-to-bot? msg)
    (do (run-ask! msg content id) true) ;; replying back keeps threads tidy

    (or (message-starts-with-pigeonbot-mention? content)
        (and (mentioned-pigeonbot? msg)
             (message-starts-with-pigeonbot-mention? content)))
    (let [q (strip-leading-pigeonbot-mention content)
          author-bot? (true? (get-in msg [:author :bot]))
          reply-to-id (when author-bot? id)]
      (when-not (str/blank? q)
        (run-ask! msg q reply-to-id)
        true))

    :else
    nil))

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

(defcmd "!ask" "Ask pigeonbot a question (also works by replying to me / @mentioning me)."
  [msg]
  (run-ask! msg (strip-command-text (:content msg))))

;; Built-in local media
(defmedia "!odinthewise" "Posts the Odin the Wise image." "odinthewise.png")
(defmedia "!partycat" "Posts the Partycat image." "partycat.png")
(defmedia "!slcomputers" "Posts the Dr Strangelove computers gif." "slcomputers.gif")
(defmedia "!wimdy" "Posts the wimdy gif." "wimdy.gif")

;; -----------------------------------------------------------------------------
;; Roles
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
;; Custom commands management (CDN URL-based)
;; -----------------------------------------------------------------------------

(defcmd "!registercommand" "Register custom command: !registercommand <name> + attach"
  [{:keys [channel-id content attachments author] :as msg}]
  (cond
    (not (custom/allowed-to-register? msg))
    (send! channel-id :content "❌ You’re not allowed to register commands.")

    :else
    (let [[_ name] (str/split (or content "") #"\s+" 3)]
      (cond
        (not (custom/valid-name? name))
        (send! channel-id :content "Usage: !registercommand <name>  (letters/numbers/_/- only)")

        (empty? attachments)
        (send! channel-id :content "Attach a file to register: `!registercommand moo` + upload cow.png")

        :else
        (let [cmd (custom/normalize-command name)]
          (cond
            (contains? @commands cmd)
            (send! channel-id :content (str "❌ `" cmd "` is a built-in command and can’t be overridden."))

            :else
            (let [att (first attachments)
                  author-id (get-in author [:id])
                  {:keys [ok? message]} (custom/register-from-attachment! cmd att author-id)]
              (if ok?
                (send! channel-id :content (str "✅ Registered `" cmd "` (CDN link saved)."))
                (send! channel-id :content (str "❌ " message))))))))))

(defcmd "!delcommand" "Delete custom command: !delcommand <name>"
  [{:keys [channel-id content] :as msg}]
  (if-not (custom/allowed-to-register? msg)
    (send! channel-id :content "❌ You’re not allowed to delete commands.")
    (let [[_ name] (str/split (or content "") #"\s+" 2)]
      (if-not (custom/valid-name? name)
        (send! channel-id :content "Usage: !delcommand <name>")
        (let [cmd (custom/normalize-command name)
              existed? (custom/delete! cmd)]
          (send! channel-id :content (if existed?
                                       (str "🗑️ Deleted `" cmd "`.")
                                       (str "No such custom command `" cmd "`."))))))))

(defcmd "!renamecommand" "Rename custom command: !renamecommand <old> <new>"
  [{:keys [channel-id content] :as msg}]
  (if-not (custom/allowed-to-register? msg)
    (send! channel-id :content "❌ You’re not allowed to rename commands.")
    (let [[_ old-name new-name] (str/split (or content "") #"\s+" 3)]
      (cond
        (or (not (custom/valid-name? old-name))
            (not (custom/valid-name? new-name)))
        (send! channel-id :content "Usage: !renamecommand <old> <new>")

        :else
        (let [old-cmd (custom/normalize-command old-name)
              new-cmd (custom/normalize-command new-name)
              {:keys [ok? message]} (custom/rename! old-cmd new-cmd)]
          (send! channel-id :content (if ok?
                                       (str "✅ Renamed `" old-cmd "` → `" new-cmd "`.") 
                                       (str "❌ " message))))))))

(defcmd "!listcommands" "List commands (built-ins + custom)."
  [{:keys [channel-id]}]
  (let [builtins (->> (keys @commands) sort)
        customs  (custom/list-commands)
        text (clamp-discord
              (str "**Built-ins (" (count builtins) "):** "
                   (if (seq builtins) (str/join ", " builtins) "(none)")
                   "\n"
                   "**Custom (" (count customs) "):** "
                   (if (seq customs) (str/join ", " customs) "(none)")))]
    (send! channel-id :content text)))

;; -----------------------------------------------------------------------------
;; React management
;; -----------------------------------------------------------------------------

(defn- parse-quoted2
  "Parse: !cmd \"a\" \"b\"  -> {:a \"a\" :b \"b\"}"
  [cmd content]
  (when content
    (let [re (re-pattern (str "(?s)^!\\s*" (java.util.regex.Pattern/quote cmd)
                              "\\s+\"([^\"]+)\"\\s+\"([^\"]*)\"\\s*$"))
          m (re-matches re (str/trim content))]
      (when m {:a (nth m 1) :b (nth m 2)}))))

(defn- parse-quoted1
  "Parse: !cmd \"a\" -> \"a\""
  [cmd content]
  (when content
    (let [re (re-pattern (str "(?s)^!\\s*" (java.util.regex.Pattern/quote cmd)
                              "\\s+\"([^\"]+)\"\\s*$"))
          m (re-matches re (str/trim content))]
      (when m (nth m 1)))))

(defcmd "!registerreact" "Register react: !registerreact \"trigger\" \"reply\" OR attach"
  [{:keys [channel-id content attachments author] :as msg}]
  (cond
    (not (reacts/allowed-to-register? msg))
    (send! channel-id :content "❌ You’re not allowed to register reacts.")

    :else
    (let [parsed (parse-quoted2 "registerreact" content)
          trigger (or (:a parsed) (parse-quoted1 "registerreact" content))
          reply   (:b parsed)]
      (cond
        (or (nil? trigger) (str/blank? trigger))
        (send! channel-id :content "Usage: !registerreact \"trigger\" \"reply\"  (or attach a file)")

        (and (or (nil? reply) (str/blank? reply))
             (empty? attachments))
        (send! channel-id :content "Attach a file or provide a reply.")

        :else
        (let [author-id (get-in author [:id])]
          (if (and reply (not (str/blank? reply)))
            (do
              (reacts/register-text! trigger reply author-id)
              (send! channel-id :content (str "✅ React registered for `" trigger "`."))
              nil)
            (let [att (first attachments)
                  {:keys [ok? message]} (reacts/register-attachment! trigger att author-id)]
              (if ok?
                (send! channel-id :content (str "✅ React registered for `" trigger "` (CDN link saved)."))
                (send! channel-id :content (str "❌ " message))))))))))

(defcmd "!editreact" "Edit react text: !editreact \"trigger\" \"new reply\""
  [{:keys [channel-id content author] :as msg}]
  (if-not (reacts/allowed-to-register? msg)
    (send! channel-id :content "❌ You’re not allowed to edit reacts.")
    (let [{:keys [a b]} (parse-quoted2 "editreact" content)]
      (cond
        (or (nil? a) (str/blank? a) (nil? b))
        (send! channel-id :content "Usage: !editreact \"trigger\" \"new reply\"")

        :else
        (do
          (reacts/set-text! a b (get-in author [:id]))
          (send! channel-id :content (str "✏️ Updated react for `" a "`.")))))))

(defcmd "!delreact" "Delete react: !delreact \"trigger\""
  [{:keys [channel-id content] :as msg}]
  (if-not (reacts/allowed-to-register? msg)
    (send! channel-id :content "❌ You’re not allowed to delete reacts.")
    (let [trigger (parse-quoted1 "delreact" content)]
      (if-not trigger
        (send! channel-id :content "Usage: !delreact \"trigger\"")
        (let [n (reacts/delete-trigger! trigger)]
          (send! channel-id :content (str "🗑️ Deleted " n " react(s) for `" trigger "`.")))))))

(defcmd "!listreacts" "List reacts."
  [{:keys [channel-id]}]
  (let [rows (reacts/list-reacts)
        summary (->> rows
                     (group-by :trigger)
                     (sort-by key)
                     (map (fn [[tr rs]]
                            (let [{:keys [type preview]} (first rs)
                                  p (or preview "")]
                              (str "- `" tr "` (" (name (or type :unknown)) ") "
                                   (subs p 0 (min 60 (count p))))))))
        text (clamp-discord
              (if (seq summary)
                (str "**Reacts (" (count (group-by :trigger rows)) " triggers):**\n"
                     (str/join "\n" summary))
                "**Reacts:** (none)"))]
    (send! channel-id :content text)))

(defcmd "!clearreacts" "Delete ALL reacts (admin-only)."
  [{:keys [channel-id] :as msg}]
  (if-not (reacts/allowed-to-register? msg)
    (send! channel-id :content "❌ You’re not allowed to clear reacts.")
    (let [n (reacts/clear!)]
      (send! channel-id :content (str "💥 Cleared " n " react rule(s).")))))

;; -----------------------------------------------------------------------------
;; Dispatch (built-ins -> custom -> ask-like)
;; -----------------------------------------------------------------------------

(defn handle-message
  "Dispatch order:
  1) built-in commands (!ping, !ask, etc)
  2) custom commands (CDN URL-based)
  3) ask-like triggers (reply to bot / @mention bot)"
  [{:keys [content channel-id] :as msg}]
  (let [cmd (first (str/split (or content "") #"\s+"))]
    (cond
      (when-let [cmd-fn (@commands cmd)]
        (cmd-fn msg)
        true)
      nil

      (when-let [r (custom/registered-reply cmd)]
        (case (:type r)
          :url  (do (send! channel-id :content (:url r)) true)
          ;; back-compat
          :file (let [f (io/file "src/pigeonbot/media" (:file r))]
                  (when (.exists f)
                    (send-file! channel-id f)
                    true))
          nil))
      nil

      (handle-ask-like! msg)
      true

      :else
      nil)))

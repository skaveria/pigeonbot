(ns pigeonbot.commands
  (:require [clojure.core.async :as async]
            [clojure.java.io :as io]
            [clojure.string :as str]
            [discljord.messaging :as m]
            [pigeonbot.custom-commands :as custom]
            [pigeonbot.ollama :as ollama]
            [pigeonbot.roles :as roles]
            [pigeonbot.state :refer [state]]))

(def ^:private discord-max-chars 2000)
(def ^:private upload-timeout-ms 5000)

(def ^:private media-root
  "Local media directory (dev-time path)."
  "src/pigeonbot/media/")

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
  "Safely send a Discord message. Returns a response channel or nil."
  [channel-id & {:keys [content file] :or {content ""}}]
  (if-let [messaging (:messaging @state)]
    (m/create-message! messaging channel-id :content (or content "") :file file)
    (do (println "send!: messaging connection is nil (bot not ready?)") nil)))

(defn- clamp-discord [s]
  (let [s (str (or s ""))]
    (if (<= (count s) discord-max-chars)
      s
      (str (subs s 0 (- discord-max-chars 1)) "…"))))

(defn- strip-command [content]
  (let [content (or content "")]
    (-> content (str/replace-first #"^\s*\S+\s*" "") (str/trim))))

(defn- send-file!
  "Send a local file attachment robustly (temp copy + timeout fail-soft)."
  [channel-id ^java.io.File f]
  (let [path (some-> f .getAbsolutePath)]
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
        (async/go
          (cond
            (nil? ch)
            (println "send-file!: no channel (bot not ready?)" {:file path})

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
                         :content (str "⚠️ I tried to upload `" (.getName f) "` but Discord didn’t respond. "
                                       "Try again in a sec, or ask an admin to re-encode the file.")))))))
        ch))))

(def command-descriptions
  (atom {"!ping" "Replies with pong."
         "!help" "Shows this help message."
         "!ask"  "Ask pigeonbot a question."
         "!role" "Self-assignable roles: !role add <ROLE_ID> | !role remove <ROLE_ID>"
         "!registercommand" "Register a custom media command: !registercommand <name> + attach a file"}))

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

(defcmd "!ping" "Replies with pong."
  [{:keys [channel-id]}]
  (send! channel-id :content "pong"))

(defcmd "!help" "Shows this help message."
  [{:keys [channel-id]}]
  (let [help-text (->> @command-descriptions
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
            (let [reply (-> (ollama/geof-ask question) clamp-discord)]
              (send! channel-id :content reply))
            (catch Throwable t
              (println "cmd-ask error:" (.getMessage t))
              (send! channel-id :content "Listen here—something went sideways talking to my brain-box."))))))))

(defmedia "!odinthewise" "Posts the Odin the Wise image." "odinthewise.png")
(defmedia "!partycat"    "Posts the Partycat image."      "partycat.png")
(defmedia "!slcomputers" "Posts the Dr Strangelove computers gif." "slcomputers.gif")
(defmedia "!wimdy"       "Posts the wimdy gif."           "wimdy.gif")

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
      (send! (:channel-id msg) :content "Usage: !role add <ROLE_ID> | !role remove <ROLE_ID>"))))


(defn handle-message [{:keys [content channel-id] :as msg}]
  (let [cmd (first (str/split (or content "") #"\s+"))]
    (if-let [cmd-fn (@commands cmd)]
      (cmd-fn msg)
      ;; fallback: custom command lookup
      (when-let [f (custom/registered-file cmd)]
        (send-file! channel-id f)))))

(defcmd "!registercommand"
  "Register a custom media command: !registercommand <name> + attach a file"
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
            ;; don't allow overriding built-ins
            (contains? @pigeonbot.commands/commands cmd) ;; or just (contains? @commands cmd) if inside same ns
            (send! channel-id :content (str "❌ `" cmd "` is a built-in command and can’t be overridden."))

            :else
            (let [att (first attachments)
                  author-id (get-in author [:id])
                  {:keys [ok? message file]} (custom/register-from-attachment! cmd att author-id)]
              (if ok?
                (send! channel-id :content (str "✅ Registered `" cmd "` → `" file "`. Try it now!"))
                (send! channel-id :content (str "❌ " message))))))))))

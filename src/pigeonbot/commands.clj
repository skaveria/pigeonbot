(ns pigeonbot.commands
  (:require [clojure.string :as str]
            [discljord.messaging :as m]
            [pigeonbot.channels :as chans]
            [pigeonbot.roles :as roles]
            [pigeonbot.ollama :as ollama]          ;; <--- add this
            [pigeonbot.state :refer [state]]))

(defn media-file [filename]
  (java.io.File. (str "src/pigeonbot/media/" filename)))

(defn send!
  "Safely send a Discord message. Returns a channel or nil.
   Prevents crashes when the bot isn't fully connected yet."
  [channel-id & {:keys [content file] :or {content ""}}]
  (if-let [messaging (:messaging @state)]
    (m/create-message! messaging
                       channel-id
                       :content content
                       :file file)
    (do
      (println "send!: messaging connection is nil (bot not ready?)")
      nil)))

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
        (str/replace-first #"^\s*\S+\s*" "")  ;; remove first word + following whitespace
        (str/trim))))


(def command-descriptions
  {"!ping"       "Replies with pong."
   "!help"       "Shows this help message."
   "!odinthewise" "Posts the Odin the Wise image."
   "!partycat"   "Posts the Partycat image."
   "!slcomputers"   "Posts the Dr Strangelove computers gif."
   "!wimdy"      "Posts the wimdy gif."
   "!ask"        "Ask pigeonbot a question."})

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

(defn cmd-wimdy [{:keys [channel-id]}]
  (send! channel-id :content "" :file (media-file "wimdy.gif")))

(defn cmd-slcomputers [{:keys [channel-id]}]
  (send! channel-id :content "" :file (media-file "slcomputers.gif")))

(defn cmd-ask
  "Ask Geof (Ollama) a question. Non-blocking via future."
  [{:keys [channel-id content]}]
  (let [question (strip-command content)]
    (if (str/blank? question)
      (send! channel-id :content "Usage: !ask <your question>")
      (do
        ;; Optional: immediate feedback so it feels responsive
        (send! channel-id :content "Hm. Lemme think…")

        ;; Do the LLM call off-thread so we don’t block message handling
        (future
          (try
            (let [reply (ollama/geof-ask question)
                  reply (clamp-discord reply)]
              (send! channel-id :content reply))
            (catch Throwable t
              (println "cmd-ask error:" (.getMessage t))
              (send! channel-id :content "Listen here—something went sideways talking to my brain-box."))))))))


(def commands
  {"!ping"        cmd-ping
   "!help"        cmd-help
   "!partycat"    cmd-partycat
   "!wimdy"       cmd-wimdy
   "!odinthewise" cmd-odinthewise
   "!slcomputers" cmd-slcomputers
   "!ask"         cmd-ask})

(defn handle-message [{:keys [content] :as msg}]
  (let [cmd (first (str/split (or content "") #"\s+"))]
    (when-let [cmd-fn (commands cmd)]
      (cmd-fn msg))))


(defn cmd-role-add [{:keys [channel-id guild-id author content]}]
  (let [[_ _ role-id] (clojure.string/split (or content "") #"\s+" 3)
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
  (let [[_ _ role-id] (clojure.string/split (or content "") #"\s+" 3)
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

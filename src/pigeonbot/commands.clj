(ns pigeonbot.commands
  (:require [clojure.string :as str]
            [discljord.messaging :as m]
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

(def command-descriptions
  {"!ping"       "Replies with pong."
   "!help"       "Shows this help message."
   "!odinthewise" "Posts the Odin the Wise image."
   "!partycat"   "Posts the Partycat image."
   "!wimdy"      "Posts the wimdy gif."})

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

(def commands
  {"!ping"        cmd-ping
   "!help"        cmd-help
   "!partycat"    cmd-partycat
   "!wimdy"       cmd-wimdy
   "!odinthewise" cmd-odinthewise})

(defn handle-message
  "Dispatch a message-create payload to a command, if it matches exactly."
  [{:keys [content] :as msg}]
  (when-let [cmd-fn (commands content)]
    (cmd-fn msg)))

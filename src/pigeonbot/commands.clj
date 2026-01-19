(ns pigeonbot.commands
  (:require [clojure.string :as str]
            [discljord.messaging :as m]
            [pigeonbot.channels :as chans]
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
   "!odinthewise" cmd-odinthewise
   "!here"        cmd-here
   "!channels"    cmd-channels})

(defn handle-message [{:keys [content] :as msg}]
  (let [cmd (first (str/split (or content "") #"\s+"))]
    (when-let [cmd-fn (commands cmd)]
      (cmd-fn msg))))

(defn cmd-here
  "Usage: !here friendly-name
   Example: !here general"
  [{:keys [channel-id content]}]
  (let [[_ friendly] (str/split (or content "") #"\s+" 2)]
    (if (seq (str/trim (or friendly "")))
      (let [{:keys [name id]} (chans/remember-channel! friendly channel-id)]
        (send! channel-id :content (str "Saved " name " => " id)))
      (send! channel-id :content "Usage: !here <friendly-name>"))))

(defn cmd-channels [{:keys [channel-id]}]
  (let [rows (chans/list-channels)
        txt  (if (seq rows)
               (->> rows
                    (map (fn [[k v]] (str k " => " v)))
                    (str/join "\n"))
               "(no channels saved yet)")]
    (send! channel-id :content txt)))

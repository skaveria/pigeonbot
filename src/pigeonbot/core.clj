
(ns pigeonbot.core
  (:gen-class)
  (:require [discljord.connections :as c]
            [discljord.messaging   :as m]
            [discljord.events      :as e]
            [clojure.edn           :as edn]
            [clojure.core.async    :as a]
            [clojure.string        :as str]))

;; -----------------------------------------------------------------------------
;; State & config
;; -----------------------------------------------------------------------------

(def state (atom nil))

(defn load-config []
  (-> "config.edn"
      slurp
      edn/read-string))

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
;; -----------------------------------------------------------------------------
;; Commands
;; -----------------------------------------------------------------------------

(def command-descriptions
  {"!ping"       "Replies with pong."
   "!help"       "Shows this help message."
   "!odinthewise" "Posts the Odin the Wise image."})

(defn cmd-ping [{:keys [channel-id]}]
  (m/create-message! (:messaging @state)
                     channel-id
                     :content "pong"))

(defn cmd-help [{:keys [channel-id]}]
  (let [help-text (->> command-descriptions
                       (map (fn [[cmd desc]] (str cmd " — " desc)))
                       (str/join "\n"))]
    (m/create-message! (:messaging @state)
                       channel-id
                       :content help-text)))

(defn cmd-odinthewise [{:keys [channel-id]}]
  (m/create-message! (:messaging @state)
                     channel-id
                     :content ""
                     :file (media-file "odinthewise.png")))

(defn cmd-partycat [{:keys [channel-id]}]
  (m/create-message! (:messaging @state)
                     channel-id
                     :content ""
                     :file (media-file "partycat.png")))

(defn cmd-wimdy [{:keys [channel-id]}]
  (m/create-message! (:messaging @state)
                     channel-id
                     :content ""
                     :file (media-file "wimdy.gif")))
(def commands
  {"!ping"        cmd-ping
   "!help"        cmd-help
   "!partycat"    cmd-partycat
   "!wimdy"       cmd-wimdy
   "!odinthewise" cmd-odinthewise})

(defn handle-message [{:keys [content] :as msg}]
  (when-let [cmd-fn (commands content)]
    (cmd-fn msg)))

(defn echo!
  "Send a message to a channel from the REPL.
   channel-id: Discord channel ID (long)
   text: string content"
  [channel-id text]
  (when-let [messaging (:messaging @state)]
    (m/create-message! messaging
                       channel-id
                       :content text)))

;; -----------------------------------------------------------------------------
;; Event routing & lifecycle
;; -----------------------------------------------------------------------------

(defn handle-event [event-type event-data]
  (when (= event-type :message-create)
    (handle-message event-data)))

(defn start-bot []
  (let [{:keys [token]} (load-config)
        event-ch (a/chan 100)
        conn-ch  (c/connect-bot! token event-ch
                                 :intents #{:guilds :guild-messages})
        msg-ch   (m/start-connection! token)]
    (reset! state {:connection conn-ch
                   :events     event-ch
                   :messaging  msg-ch})
    (println "Connected to Discord (waiting for events)…")
    (e/message-pump! event-ch handle-event)))

(def bot-future (atom nil))

(defn start-bot!
  "Starts the bot in the background and returns the messaging handle
   for convenient use at the REPL."
  []
  (reset! bot-future
          (future
            (start-bot)))
  ;; wait briefly for :messaging to appear in state
  (loop [tries 0]
    (if-let [msg (:messaging @state)]
      msg
      (if (< tries 20)
        (do
          (Thread/sleep 250)
          (recur (inc tries)))
        (do
          (println "start-bot!: timed out waiting for messaging connection.")
          nil)))))


(defn stop-bot!
  "Best-effort stop of the running bot."
  []
  (when-let [{:keys [events connection messaging]} @state]
    (when events
      (a/close! events))
    ;; discljord connections will usually stop when channels close
    (println "Stopping bot connections…"))
  (when-let [f @bot-future]
    (future-cancel f))
  (reset! bot-future nil)
  (reset! state nil)
  (println "Bot stopped."))

(defn restart-bot!
  "Restart the Discord bot cleanly from the REPL."
  []
  (println "Restarting pigeonbot…")
  (stop-bot!)
  ;; give async systems a moment to breathe
  (Thread/sleep 500)
  (start-bot!))

(defn -main [& _args]
  (start-bot))

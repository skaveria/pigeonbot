(ns pigeonbot.core
  (:gen-class)
  (:require [discljord.connections :as c]
            [discljord.messaging :as m]
            [discljord.events :as e]
            [clojure.edn :as edn]
            [clojure.core.async :as a]))

(def state (atom nil))

(defn load-config []
  (-> "config.edn" slurp edn/read-string))

(defn media-file [filename]
  (java.io.File. (str "src/pigeonbot/media/" filename)))

;; --- commands ---------------------------------------------------------------
(def command-descriptions
  {"!ping" "Replies with pong."
   "!help" "Shows this help message."
   "!odinthewise" "Nothing is more important than looking cool."})

(defn cmd-ping [{:keys [channel-id]}]
  (m/create-message! (:messaging @state)
                     channel-id
                     :content "pong"))

(defn cmd-help [{:keys [channel-id]}]
  (let [help-text (->> command-descriptions
                       (map (fn [[cmd desc]] (str cmd " — " desc)))
                       (clojure.string/join "\n"))]
    (m/create-message! (:messaging @state)
                       channel-id
                       :content help-text)))

(defn cmd-odinthewise [{:keys [channel-id]}]
  (m/create-message! (:messaging @state)
                     channel-id
                     :content ""
                     :file (media-file "odinthewise.png")))


(def commands
  {"!ping"       cmd-ping
   "!help"       cmd-help
   "!odinthewise" cmd-odinthewise})

(defn handle-message [{:keys [content] :as msg}]
  (when-let [cmd-fn (commands content)]
    (cmd-fn msg)))


;; --- event routing ----------------------------------------------------------

(defn handle-event [event-type event-data]
  (when (= event-type :message-create)
    (handle-message event-data)))

;; --- lifecycle --------------------------------------------------------------

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


(defn -main [& _args]
  (start-bot))

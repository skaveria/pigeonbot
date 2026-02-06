(ns pigeonbot.discord
  (:require [clojure.core.async :as a]
            [clojure.string :as str]
            [discljord.connections :as c]
            [discljord.events :as e]
            [discljord.messaging :as m]
            [pigeonbot.channels :as channels]
            [pigeonbot.commands :as commands]
            [pigeonbot.config :as config]
            [pigeonbot.custom-commands :as custom]
            [pigeonbot.discljord-patch :as djpatch]
            [pigeonbot.reaction-roles :as rr]
            [pigeonbot.state :refer [state]]))

(defonce seen-events* (atom #{}))
(defonce bot-future (atom nil))

(defn handle-event [event-type event-data]
  (when-not (contains? @seen-events* event-type)
    (swap! seen-events* conj event-type)
    (println "EVENT TYPE:" event-type))

  (when (and event-type (str/includes? (name event-type) "reaction"))
    (println "REACTION-ish EVENT:" event-type
             (select-keys event-data [:guild-id :channel-id :message-id :user-id :emoji])))

  (case event-type
    :message-create (commands/handle-message event-data)
    :message-reaction-add (rr/handle-reaction-add! event-data)
    :message-reaction-remove (rr/handle-reaction-remove! event-data)
    nil))

(defn start-bot
  "Connect to Discord and start the event pump (blocking)."
  []
  (channels/load-channels!)
  (custom/load!)
  (djpatch/patch-discljord!)

  (let [{:keys [token]} (config/load-config)
        event-ch (a/chan 100)

        reaction-intent (or (when (contains? c/gateway-intents :guild-message-reactions)
                              :guild-message-reactions)
                            (when (contains? c/gateway-intents :guild-message-reaction)
                              :guild-message-reaction)
                            (when (contains? c/gateway-intents :guild-message-reaction-add)
                              :guild-message-reaction-add)
                            (when (contains? c/gateway-intents :guild-message-reaction-events)
                              :guild-message-reaction-events))

        intents (cond-> #{:guilds :guild-messages}
                  reaction-intent (conj reaction-intent))

        conn   (c/connect-bot! token event-ch :intents intents)
        msg-ch (m/start-connection! token)]

    (println "Gateway intents:" intents)
    (when-not reaction-intent
      (println "WARNING: Could not find a reaction intent keyword in discljord.connections/gateway-intents"))

    (reset! state {:connection conn :events event-ch :messaging msg-ch})
    (println "Connected to Discord (online)")
    (e/message-pump! event-ch handle-event)))

(defn start-bot!
  "Starts the bot in the background and returns the messaging handle."
  []
  (reset! bot-future (future (start-bot)))
  (loop [tries 0]
    (if-let [msg (:messaging @state)]
      msg
      (if (< tries 20)
        (do (Thread/sleep 250) (recur (inc tries)))
        (do (println "start-bot!: timed out waiting for messaging connection.") nil)))))

(defn stop-bot!
  "Best-effort stop of the running bot."
  []
  (let [{:keys [events connection messaging]} @state]
    (println "Stopping bot connections…")

    (when events
      (try (a/close! events)
           (catch Throwable t (println "stop-bot!: close events failed:" (.getMessage t)))))

    (when connection
      (try (c/disconnect-bot! connection)
           (catch Throwable t (println "stop-bot!: disconnect-bot! failed:" (.getMessage t)))))

    (when messaging
      (try (m/stop-connection! messaging)
           (catch Throwable t (println "stop-bot!: stop-connection! failed:" (.getMessage t))))))

  (when-let [f @bot-future]
    (future-cancel f))
  (reset! bot-future nil)
  (reset! state {})
  (println "Bot stopped."))


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
            (contains? @commands cmd)
            (send! channel-id :content (str "❌ `" cmd "` is a built-in command and can’t be overridden."))

            :else
            (let [att (first attachments)
                  author-id (get-in author [:id])
                  {:keys [ok? message file]} (custom/register-from-attachment! cmd att author-id)]
              (if ok?
                (send! channel-id :content (str "✅ Registered `" cmd "` → `" file "`. Try it now!"))
                (send! channel-id :content (str "❌ " message))))))))))
;;                                                                 ^^^^^^^^^
;; closes: if, let(att..), cond(cmd), let(cmd), cond(name), let(name), cond(top), defcmd


(defn restart-bot! []
  (println "Restarting pigeonbot…")
  (stop-bot!)
  (Thread/sleep 500)
  (start-bot!))

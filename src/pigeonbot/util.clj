(ns pigeonbot.commands.util
  (:require [clojure.core.async :as async]
            [clojure.java.io :as io]
            [discljord.messaging :as m]
            [pigeonbot.state :refer [state]]
            [pigeonbot.threads :as threads]))

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

(defn clamp-discord [s]
  (let [s (str (or s ""))]
    (if (<= (count s) discord-max-chars)
      s
      (str (subs s 0 (- discord-max-chars 1)) "…"))))

(defn send!
  "Safely send a Discord message. Returns a response channel or nil."
  [channel-id & {:keys [content file allowed-mentions] :or {content ""}}]
  ;; If we speak in a thread channel, this is how we 'subscribe' to it for
  ;; subsequent no-@ messages.
  (threads/note-bot-spoke! channel-id)
  (if-let [messaging (:messaging @state)]
    (m/create-message! messaging channel-id
                      :content (or content "")
                      :file file
                      :allowed_mentions allowed-mentions)
    (do
      (println "send!: messaging is nil (bot not ready?)")
      nil)))

(defn send-reply!
  "Reply to a specific message id in the same channel, without pinging replied user."
  [channel-id reply-to-message-id & {:keys [content file]}]
  (threads/note-bot-spoke! channel-id)
  (if-let [messaging (:messaging @state)]
    (m/create-message! messaging channel-id
                      :content (or content "")
                      :file file
                      :message_reference {:message_id (str reply-to-message-id)}
                      :allowed_mentions {:replied_user false})
    (do
      (println "send-reply!: messaging is nil (bot not ready?)")
      nil)))

(defn typing!
  "Trigger a ~10s typing indicator for the channel."
  [channel-id]
  (if-let [messaging (:messaging @state)]
    (try
      (m/trigger-typing-indicator! messaging channel-id)
      (catch Throwable t
        (println "typing!: failed:" (.getMessage t))
        nil))
    (do
      (println "typing!: messaging is nil (bot not ready?)")
      nil)))

(defn send-file!
  "Built-in local media only (custom commands should use CDN URLs).
  Uses a temp-copy + timeout so weird files don't hang forever."
  [channel-id ^java.io.File f]
  (let [path (some-> f .getAbsolutePath)]
    (cond
      (nil? f)
      (do (println "send-file!: file is nil") nil)

      (not (.exists f))
      (do
        (println "send-file!: missing file" {:file path})
        (send! channel-id :content (str "⚠️ Missing media file: `" (.getName f) "`"))
        nil)

      :else
      (let [tmp (temp-copy f (.getName f))
            ch (send! channel-id :content "" :file tmp)]
        (async/go
          (if (nil? ch)
            (println "send-file!: no channel (bot not ready?)" {:file path})
            (let [[resp port] (async/alts! [ch (async/timeout upload-timeout-ms)])]
              (if (= port ch)
                (when (and (map? resp) (:error resp))
                  (println "send-file!: discljord error:" (pr-str resp)))
                (do
                  (println "send-file!: TIMEOUT sending attachment"
                           {:file path :size (.length f)})
                  (send! channel-id
                         :content (str "⚠️ Upload timed out for `" (.getName f) "`.\n"
                                       "Try again or re-encode the file."))))))))
        ch)))

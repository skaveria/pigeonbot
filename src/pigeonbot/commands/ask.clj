(ns pigeonbot.commands.ask
  (:require [clojure.string :as str]
            [pigeonbot.context :as ctx]
            [pigeonbot.ollama :as ollama]
            [pigeonbot.state :refer [state]]
            [pigeonbot.commands.registry :refer [defcmd]]
            [pigeonbot.commands.util :as u]))

(defn- strip-command-text [content]
  (-> (or content "")
      (str/replace-first #"^\s*\S+\s*" "")
      str/trim))

(defn- bot-id []
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
    (boolean
     (re-find (re-pattern (str "^\\s*<@!?" (java.util.regex.Pattern/quote bid) ">\\s*"))
              (or content "")))))

(defn- strip-pigeonbot-mention-anywhere
  "Remove ALL pigeonbot mention tokens from content (useful for bot-authored messages)."
  [s]
  (if-let [bid (bot-id)]
    (-> (or s "")
        (str/replace (re-pattern (str "<@!?" (java.util.regex.Pattern/quote bid) ">")) "")
        (str/replace #"\s+" " ")
        str/trim)
    (str/trim (or s ""))))

(defn- strip-leading-pigeonbot-mention
  "Remove a leading pigeonbot mention token from content."
  [s]
  (if-let [bid (bot-id)]
    (-> (or s "")
        (str/replace-first (re-pattern (str "^\\s*<@!?" (java.util.regex.Pattern/quote bid) ">\\s*")) "")
        str/trim)
    (str/trim (or s ""))))

(defn- referenced-message-reply-to-bot?
  "Some payloads include full :referenced_message."
  [msg]
  (true? (get-in msg [:referenced_message :author :bot])))

(defn- message-reference-id
  "Some payloads only include :message_reference {:message_id ...} without :referenced_message."
  [msg]
  (or (get-in msg [:message_reference :message_id])
      (get-in msg [:message_reference :message-id])
      (get-in msg [:message-reference :message_id])
      (get-in msg [:message-reference :message-id])))

(defn- reply-to-bot?
  "Robust reply detection:
  - If :referenced_message exists and is a bot -> true
  - Else, if :message_reference points at a message id that we recently recorded as bot -> true"
  [{:keys [channel-id] :as msg}]
  (or (referenced-message-reply-to-bot? msg)
      (when-let [rid (some-> (message-reference-id msg) str)]
        (some (fn [{:keys [id bot?]}]
                (and (= id rid) bot?))
              (ctx/recent-messages channel-id)))))

(defn- build-ask-context [msg]
  (ctx/context-text msg))

(defn run-ask!
  "Core ask runner. If reply-to-id provided, sends as a reply."
  ([msg question] (run-ask! msg question nil))
  ([{:keys [channel-id] :as msg} question reply-to-id]
   (let [question (str/trim (or question ""))]
     (if (str/blank? question)
       (u/send! channel-id :content "Usage: !ask <question> (or reply / @mention me)")
       (do
         (if reply-to-id
           (u/send-reply! channel-id reply-to-id :content "Hm. Lemme think…")
           (u/send! channel-id :content "Hm. Lemme think…"))

         (future
           (try
             (let [context-text (build-ask-context msg)
                   reply (ollama/geof-ask-with-context context-text question)
                   reply (u/clamp-discord reply)]
               (if reply-to-id
                 (u/send-reply! channel-id reply-to-id :content reply)
                 (u/send! channel-id :content reply)))
             (catch Throwable t
               (println "ask error:" (.getMessage t))
               (if reply-to-id
                 (u/send-reply! channel-id reply-to-id :content "brain-box error, try again")
                 (u/send! channel-id :content "brain-box error, try again"))))))))))

(defn handle-ask-like!
  "Ask-like behavior rules:
  - Replies to pigeonbot/bot messages trigger ask and respond as a reply.
  - Mentions by OTHER BOTS trigger ask even if mention is not at start (so Jimbo works).
  - Mentions by humans require the mention to be the prefix (prevents hijacking)."
  [{:keys [content id] :as msg}]
  (let [author-bot? (true? (get-in msg [:author :bot]))]
    (cond
      ;; Replies: keep threads tidy by replying
      (reply-to-bot? msg)
      (do (run-ask! msg content id) true)

      ;; Bot-authored mention: trigger if pigeonbot is mentioned anywhere
      (and author-bot?
           (mentioned-pigeonbot? msg))
      (let [q (strip-pigeonbot-mention-anywhere content)]
        (when-not (str/blank? q)
          (run-ask! msg q id) ;; reply to the bot message
          true))

      ;; Human mention: only if it begins with the mention
      (and (not author-bot?)
           (message-starts-with-pigeonbot-mention? content))
      (let [q (strip-leading-pigeonbot-mention content)]
        (when-not (str/blank? q)
          (run-ask! msg q nil)
          true))

      :else nil)))

(defcmd "!ask" "Ask pigeonbot a question (also works by replying / @mentioning)."
  [msg]
  (run-ask! msg (strip-command-text (:content msg))))

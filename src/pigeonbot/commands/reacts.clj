(ns pigeonbot.commands.reacts
  (:require [clojure.string :as str]
            [pigeonbot.message-reacts :as reacts]
            [pigeonbot.commands.registry :refer [defcmd]]
            [pigeonbot.commands.util :as u]))

(defn- parse-quoted2 [cmd content]
  (when content
    (let [re (re-pattern (str "(?s)^!\\s*" (java.util.regex.Pattern/quote cmd)
                              "\\s+\"([^\"]+)\"\\s+\"([^\"]*)\"\\s*$"))
          m (re-matches re (str/trim content))]
      (when m {:a (nth m 1) :b (nth m 2)}))))

(defn- parse-quoted1 [cmd content]
  (when content
    (let [re (re-pattern (str "(?s)^!\\s*" (java.util.regex.Pattern/quote cmd)
                              "\\s+\"([^\"]+)\"\\s*$"))
          m (re-matches re (str/trim content))]
      (when m (nth m 1)))))

(defcmd "!registerreact" "Register react: !registerreact \"trigger\" \"reply\" OR attach"
  [{:keys [channel-id content attachments author] :as msg}]
  (cond
    (not (reacts/allowed-to-register? msg))
    (u/send! channel-id :content "❌ You’re not allowed to register reacts.")
    :else
    (let [parsed (parse-quoted2 "registerreact" content)
          trigger (or (:a parsed) (parse-quoted1 "registerreact" content))
          reply   (:b parsed)]
      (cond
        (or (nil? trigger) (str/blank? trigger))
        (u/send! channel-id :content "Usage: !registerreact \"trigger\" \"reply\"  (or attach)")
        (and (or (nil? reply) (str/blank? reply)) (empty? attachments))
        (u/send! channel-id :content "Attach a file or provide a reply.")
        :else
        (let [author-id (get-in author [:id])]
          (if (and reply (not (str/blank? reply)))
            (do (reacts/register-text! trigger reply author-id)
                (u/send! channel-id :content (str "✅ React registered for `" trigger "`."))
                nil)
            (let [att (first attachments)
                  {:keys [ok? message]} (reacts/register-attachment! trigger att author-id)]
              (if ok?
                (u/send! channel-id :content (str "✅ React registered for `" trigger "` (CDN saved)."))
                (u/send! channel-id :content (str "❌ " message))))))))))

(defcmd "!editreact" "Edit react text: !editreact \"trigger\" \"new reply\""
  [{:keys [channel-id content author] :as msg}]
  (if-not (reacts/allowed-to-register? msg)
    (u/send! channel-id :content "❌ You’re not allowed to edit reacts.")
    (let [{:keys [a b]} (parse-quoted2 "editreact" content)]
      (cond
        (or (nil? a) (str/blank? a) (nil? b))
        (u/send! channel-id :content "Usage: !editreact \"trigger\" \"new reply\"")
        :else
        (do (reacts/set-text! a b (get-in author [:id]))
            (u/send! channel-id :content (str "✏️ Updated react for `" a "`.")))))))

(defcmd "!delreact" "Delete react: !delreact \"trigger\""
  [{:keys [channel-id content] :as msg}]
  (if-not (reacts/allowed-to-register? msg)
    (u/send! channel-id :content "❌ You’re not allowed to delete reacts.")
    (let [trigger (parse-quoted1 "delreact" content)]
      (if-not trigger
        (u/send! channel-id :content "Usage: !delreact \"trigger\"")
        (let [n (reacts/delete-trigger! trigger)]
          (u/send! channel-id :content (str "🗑️ Deleted " n " react(s) for `" trigger "`.")))))))

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
        text (u/clamp-discord
              (if (seq summary)
                (str "**Reacts (" (count (group-by :trigger rows)) " triggers):**\n"
                     (str/join "\n" summary))
                "**Reacts:** (none)"))]
    (u/send! channel-id :content text)))

(defcmd "!clearreacts" "Delete ALL reacts (admin-only)."
  [{:keys [channel-id] :as msg}]
  (if-not (reacts/allowed-to-register? msg)
    (u/send! channel-id :content "❌ You’re not allowed to clear reacts.")
    (let [n (reacts/clear!)]
      (u/send! channel-id :content (str "💥 Cleared " n " react rule(s).")))))

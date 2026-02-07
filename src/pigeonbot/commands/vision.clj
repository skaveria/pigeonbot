(ns pigeonbot.commands.vision
  (:require [clojure.string :as str]
            [pigeonbot.commands.registry :refer [defcmd]]
            [pigeonbot.commands.util :as u]
            [pigeonbot.vision-registry :as vr]))

(defn- parse-registervision
  "Parses:
   !registervision \"label\" :react \"🍝\"
   !registervision \"label\" :reply \"text...\"
   !registervision \"label\" :both \"🍝\" \"text...\"

  Returns map:
  {:label ... :mode :react/:reply/:both :emoji ... :reply ...}"
  [content]
  (let [s (str/trim (or content ""))

        re-react (re-pattern (str "(?s)^!\\s*registervision\\s+\"([^\"]+)\"\\s+:react\\s+\"([^\"]+)\"\\s*$"))
        re-reply (re-pattern (str "(?s)^!\\s*registervision\\s+\"([^\"]+)\"\\s+:reply\\s+\"([^\"]*)\"\\s*$"))
        re-both  (re-pattern (str "(?s)^!\\s*registervision\\s+\"([^\"]+)\"\\s+:both\\s+\"([^\"]+)\"\\s+\"([^\"]*)\"\\s*$"))]
    (cond
      (re-matches re-both s)
      (let [[_ label emoji reply] (re-matches re-both s)]
        {:label label :mode :both :emoji emoji :reply reply})

      (re-matches re-react s)
      (let [[_ label emoji] (re-matches re-react s)]
        {:label label :mode :react :emoji emoji})

      (re-matches re-reply s)
      (let [[_ label reply] (re-matches re-reply s)]
        {:label label :mode :reply :reply reply})

      :else nil)))

(defcmd "!registervision"
  "Register vision rule: !registervision \"label\" :react \"🍝\" | :reply \"text\" | :both \"🍝\" \"text\""
  [{:keys [channel-id content author] :as msg}]
  (cond
    (not (vr/allowed-to-register? msg))
    (u/send! channel-id :content "❌ You’re not allowed to register vision rules.")

    :else
    (let [p (parse-registervision content)
          author-id (get-in author [:id])]
      (if-not p
        (u/send! channel-id :content
                 "Usage: !registervision \"label\" :react \"🍝\"  OR  :reply \"text\"  OR  :both \"🍝\" \"text\"")
        (case (:mode p)
          :react (do (vr/register-react! (:label p) (:emoji p) author-id)
                     (u/send! channel-id :content (str "✅ Vision rule `" (str/lower-case (:label p)) "` → react " (:emoji p)))
                     nil)
          :reply (do (vr/register-reply! (:label p) (:reply p) author-id)
                     (u/send! channel-id :content (str "✅ Vision rule `" (str/lower-case (:label p)) "` → reply set"))
                     nil)
          :both  (do (vr/register-both! (:label p) (:emoji p) (:reply p) author-id)
                     (u/send! channel-id :content (str "✅ Vision rule `" (str/lower-case (:label p)) "` → react+reply set"))
                     nil)
          (u/send! channel-id :content "❌ Unknown mode."))))))

(defcmd "!listvision"
  "List registered vision rules."
  [{:keys [channel-id]}]
  (let [rules (vr/list-rules)
        lines (->> rules
                   (map (fn [{:keys [id actions min-confidence]}]
                          (let [r (:react actions)
                                p (:reply actions)]
                            (str "- `" id "`"
                                 (when r (str " react " r))
                                 (when p (str " reply \"" (subs (or p "") 0 (min 60 (count (or p "")))) "\""))
                                 (when (number? min-confidence) (str " (>= " min-confidence ")"))))))
                   (str/join "\n"))
        txt (u/clamp-discord
             (if (seq rules)
               (str "**Vision rules (" (count rules) "):**\n" lines)
               "**Vision rules:** (none)"))]
    (u/send! channel-id :content txt)))

(defcmd "!delvision"
  "Delete a vision rule: !delvision \"label\""
  [{:keys [channel-id content] :as msg}]
  (if-not (vr/allowed-to-register? msg)
    (u/send! channel-id :content "❌ You’re not allowed to delete vision rules.")
    (let [m (re-matches #"(?s)^!\s*delvision\s+\"([^\"]+)\"\s*$" (str/trim (or content "")))]
      (if-not m
        (u/send! channel-id :content "Usage: !delvision \"label\"")
        (let [label (nth m 1)
              ok? (vr/delete! label)]
          (u/send! channel-id :content
                   (if ok?
                     (str "🗑️ Deleted vision rule `" (str/lower-case label) "`.")
                     (str "No such vision rule `" (str/lower-case label) "`."))))))))

(defcmd "!clearvision"
  "Delete ALL vision rules (admin-only)."
  [{:keys [channel-id] :as msg}]
  (if-not (vr/allowed-to-register? msg)
    (u/send! channel-id :content "❌ You’re not allowed to clear vision rules.")
    (let [n (vr/clear!)]
      (u/send! channel-id :content (str "💥 Cleared " n " vision rule(s).")))))

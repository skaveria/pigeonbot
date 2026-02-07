(ns pigeonbot.commands.builtins
  (:require [clojure.string :as str]
            [pigeonbot.commands.registry :refer [defcmd defmedia command-descriptions]]
            [pigeonbot.commands.util :as u]))

(defcmd "!ping" "Replies with pong."
  [{:keys [channel-id]}]
  (u/send! channel-id :content "pong"))

(defcmd "!help" "Shows this help message."
  [{:keys [channel-id]}]
  (let [help-text (->> @command-descriptions
                       (sort-by key)
                       (map (fn [[cmd desc]] (str cmd " — " desc)))
                       (str/join "\n")
                       u/clamp-discord)]
    (u/send! channel-id :content help-text)))

(defmedia "!odinthewise" "Posts the Odin the Wise image." "odinthewise.png")
(defmedia "!partycat" "Posts the Partycat image." "partycat.png")
(defmedia "!slcomputers" "Posts the Dr Strangelove computers gif." "slcomputers.gif")
(defmedia "!wimdy" "Posts the wimdy gif." "wimdy.gif")

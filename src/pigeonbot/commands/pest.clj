(ns pigeonbot.commands.pest
  (:require [clojure.string :as str]
            [pigeonbot.commands.registry :refer [defcmd]]
            [pigeonbot.commands.util :as u]
            [pigeonbot.pest :as pest]
            [pigeonbot.config :as config]))

(defn- admin-allowed?
  "Default: allow everyone.
   If config.edn contains :pest-admin-ids [\"123\" \"456\"], only allow those."
  [{:keys [author]}]
  (let [uid (some-> (get-in author [:id]) str)
        cfg (try (config/load-config) (catch Throwable _ {}))
        admins (set (map str (or (:pest-admin-ids cfg) [])))]
    (or (empty? admins)
        (contains? admins uid))))

(defcmd "!pest" "Pest mode toggle: !pest on | !pest off | !pest status"
  [{:keys [channel-id content] :as msg}]
  (if-not (admin-allowed? msg)
    (u/send! channel-id :content "❌ You’re not allowed to change pest mode.")
    (let [[_ sub] (-> (or content "")
                      (str/trim)
                      (str/split #"\s+" 3))
          sub (some-> sub str/lower-case)]
      (case sub
        "on"
        (do
          (pest/set-enabled! true)
          (u/send! channel-id :content "🪶 pest mode: ON"))

        "off"
        (do
          (pest/set-enabled! false)
          (u/send! channel-id :content "🪶 pest mode: OFF"))

        "status"
        (let [{:keys [enabled? config override]} (pest/status)]
          (u/send! channel-id
                   :content
                   (u/clamp-discord
                    (str "🪶 pest mode: " (if enabled? "ON" "OFF") "\n"
                         "chance=" (:chance config) " cooldown-ms=" (:cooldown-ms config) " min-chars=" (:min-chars config) "\n"
                         "override=" (pr-str override)))))

        ;; default
        (u/send! channel-id :content "Usage: !pest on | !pest off | !pest status")))))

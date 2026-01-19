(ns pigeonbot.config
  (:require [clojure.edn :as edn]))

(defn load-config
  "Load config.edn from the project root."
  []
  (-> "config.edn"
      slurp
      edn/read-string))

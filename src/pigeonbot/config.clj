(ns pigeonbot.config
  (:require [clojure.edn :as edn]))

(def ^:private config-path
  "Local config file path. If you plan to make the repo public,
  DO NOT commit this file if it contains secrets."
  "config.edn")

(defn load-config
  "Load config.edn from the project root if present, else {}.
  Intended for non-secret settings, but may include :token in older setups."
  []
  (try
    (-> config-path slurp edn/read-string)
    (catch java.io.FileNotFoundException _ {})
    (catch Throwable t
      (println "load-config: failed to read" config-path ":" (.getMessage t))
      {})))

(defn discord-token
  "Get Discord token from env (preferred) or config.edn (fallback).
  Env vars:
    - DISCORD_TOKEN
    - PIGEONBOT_TOKEN"
  []
  (or (System/getenv "DISCORD_TOKEN")
      (System/getenv "PIGEONBOT_TOKEN")
      (:token (load-config))))

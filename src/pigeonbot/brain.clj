(ns pigeonbot.brain
  (:require [pigeonbot.config :as config]
            [pigeonbot.ollama :as ollama]
            [pigeonbot.brains.openclaw :as openclaw]
            [pigeonbot.brains.openai :as openai]))

(defn brain-type []
  (let [cfg (config/load-config)]
    (or (:brain cfg) :ollama)))

(defn ask-with-context
  "Unified brain entry point."
  [context-text question]
  (case (brain-type)
    :openai   (openai/ask-with-context context-text question)
    :openclaw (openclaw/ask-with-context context-text question)
    :ollama   (ollama/geof-ask-with-context context-text question)
    ;; Safety net: if config has a typo like :open-api, fall back instead of crashing.
    (ollama/geof-ask-with-context context-text question)))

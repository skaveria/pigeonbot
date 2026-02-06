(ns pigeonbot.discljord-patch
  (:require [clojure.data.json :as json]
            [clojure.string :as str]
            [discljord.messaging.impl :as impl]))

(defn patch-discljord-json-body! []
  (alter-var-root #'impl/json_body
    (fn [_]
      (fn [body]
        (when (and body (not (str/blank? body)))
          (json/read-str body :key-fn keyword))))))

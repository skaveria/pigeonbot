(ns pigeonbot.discljord-patch
  (:require [clojure.data.json :as json]
            [clojure.string :as str]))

(defn patch-discljord-json-bodies!
  "Patch discljord JSON response parsing to tolerate nil/blank bodies.
  Patches both json_body and json-body if present (vars may be private)."
  []
  (require 'discljord.messaging.impl)
  (let [interns (ns-interns 'discljord.messaging.impl)
        targets (->> ['json_body 'json-body]
                     (map interns)
                     (remove nil?))]
    (when (empty? targets)
      (throw (ex-info "No json body vars found in discljord.messaging.impl"
                      {:available (->> (keys interns) sort vec)})))
    (doseq [v targets]
      (alter-var-root v
        (fn [_orig]
          (fn [body]
            (when (and body (not (str/blank? body)))
              (json/read-str body :key-fn keyword))))))
    {:patched (->> targets (map (comp :name meta)) (map str) vec)
     :count (count targets)}))

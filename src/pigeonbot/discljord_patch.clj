(ns pigeonbot.discljord-patch
  (:require [clojure.data.json :as json]
            [clojure.string :as str]))

(defn- patch-var!
  "Replace var root with (f orig) if var exists."
  [interns sym f]
  (when-let [v (get interns sym)]
    (alter-var-root v f)
    true))

(defn patch-discljord!
  "Best-effort hardening so discljord's HTTP dispatch agent is less likely to die.

  Patches (if present):
  - json_body / json-body : tolerate nil/blank response bodies
  - send_message_BANG_    : catch throwables and return an error map

  Returns {:patched [...]}."
  []
  (require 'discljord.messaging.impl)
  (let [interns (ns-interns 'discljord.messaging.impl)
        patched (atom [])]

    ;; Some versions use json_body, others json-body.
    (doseq [sym ['json_body 'json-body]]
      (when (patch-var! interns sym
              (fn [_orig]
                (fn [body]
                  (when (and body (not (str/blank? body)))
                    (json/read-str body :key-fn keyword)))))
        (swap! patched conj (str sym))))

    ;; Prevent exceptions from escaping and killing dispatch-http.
    (doseq [sym ['send_message_BANG_]]
      (when (patch-var! interns sym
              (fn [orig]
                (fn [& args]
                  (try
                    (apply orig args)
                    (catch Throwable t
                      {:error :discljord-send-message-exception
                       :message (.getMessage t)
                       :class (str (class t))})))))
        (swap! patched conj (str sym))))

    {:patched @patched
     :count (count @patched)}))

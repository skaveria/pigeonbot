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
  "Harden discljord so dispatch-http never dies on nil/blank JSON bodies or
   other unexpected responses.

   Patches (if present):
   - json_body / json-body : tolerate nil/blank
   - send_message_BANG_    : catch all throwables and return an error map"
  []
  (require 'discljord.messaging.impl)
  (let [interns (ns-interns 'discljord.messaging.impl)
        patched (atom [])]

    ;; 1) JSON body parsers: tolerate nil/blank
    (doseq [sym ['json_body 'json-body]]
      (when (patch-var! interns sym
              (fn [_orig]
                (fn [body]
                  (when (and body (not (str/blank? body)))
                    (json/read-str body :key-fn keyword)))))
        (swap! patched conj (str sym))))

    ;; 2) The big one: never let send_message_BANG_ throw inside dispatch-http
    (doseq [sym ['send_message_BANG_]]
      (when (patch-var! interns sym
              (fn [orig]
                (fn [& args]
                  (try
                    (apply orig args)
                    (catch Throwable t
                      ;; Return a consistent error value instead of throwing.
                      ;; This prevents dispatch-http from dying.
                      {:error :discljord-send-message-exception
                       :message (.getMessage t)
                       :class (str (class t))})))))
        (swap! patched conj (str sym))))

    {:patched @patched
     :count (count @patched)}))

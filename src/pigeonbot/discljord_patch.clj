(ns pigeonbot.discljord-patch
  (:require [clojure.data.json :as json]
            [clojure.string :as str]))

(defn- patch-var!
  "Replace var root with (f orig) if var exists."
  [interns sym f]
  (when-let [v (get interns sym)]
    (alter-var-root v f)
    true))

(defn- guess-intent-map-var
  "Best-effort: find a var in discljord.connections whose value looks like
  a keyword->int map of gateway intents (contains :guilds and :guild-messages)."
  []
  (require 'discljord.connections)
  (let [interns (ns-interns 'discljord.connections)]
    (some (fn [[sym v]]
            (try
              (let [val @v]
                (when (and (map? val)
                           (keyword? (first (keys val)))
                           (number? (val :guilds))
                           (number? (val :guild-messages)))
                  sym))
              (catch Throwable _ nil)))
          interns)))

(defn patch-discljord!
  "Best-effort hardening:
  - messaging.impl JSON + send_message hardening (existing)
  - NEW: add Message Content intent support for older discljord versions

  Returns {:patched [...]}."
  []
  (require 'discljord.messaging.impl)
  (require 'discljord.connections)
  (let [patched (atom [])

        ;; --- patch messaging impl (your existing behavior) ---
        impl-interns (ns-interns 'discljord.messaging.impl)]

    (doseq [sym ['json_body 'json-body]]
      (when (patch-var! impl-interns sym
              (fn [_orig]
                (fn [body]
                  (when (and body (not (str/blank? body)))
                    (json/read-str body :key-fn keyword)))))
        (swap! patched conj (str "discljord.messaging.impl/" sym))))

    (doseq [sym ['send_message_BANG_]]
      (when (patch-var! impl-interns sym
              (fn [orig]
                (fn [& args]
                  (try
                    (apply orig args)
                    (catch Throwable t
                      {:error :discljord-send-message-exception
                       :message (.getMessage t)
                       :class (str (class t))})))))
        (swap! patched conj "discljord.messaging.impl/send_message_BANG_")))

    ;; --- NEW: patch gateway intents for message content ---
    (let [conn-interns (ns-interns 'discljord.connections)
          message-content-bit 32768

          ;; discljord.connections/gateway-intents is a collection in your build
          _ (when (patch-var! conn-interns 'gateway-intents
                   (fn [orig]
                     (let [s (set orig)]
                       (conj s :message-content :message_content))))
              (swap! patched conj "discljord.connections/gateway-intents(+message-content)"))

          ;; find and patch keyword->bit map (var name differs by version)
          intent-map-sym (or (guess-intent-map-var)
                             ;; common guesses
                             (when (contains? conn-interns 'intent-map) 'intent-map)
                             (when (contains? conn-interns 'gateway-intents-map) 'gateway-intents-map)
                             (when (contains? conn-interns 'intents) 'intents))]

      (when intent-map-sym
        (when (patch-var! conn-interns intent-map-sym
                (fn [m]
                  (cond
                    (map? m) (assoc m
                                    :message-content message-content-bit
                                    :message_content message-content-bit)
                    :else m)))
          (swap! patched conj (str "discljord.connections/" intent-map-sym "(+message-content-bit)")))))

    {:patched @patched
     :count (count @patched)}))

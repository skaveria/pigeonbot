(ns pigeonbot.vision-reacts
  (:require [clojure.string :as str]
            [discljord.messaging :as m]
            [pigeonbot.brains.openclaw :as oc]
            [pigeonbot.commands.util :as u]
            [pigeonbot.config :as config]
            [pigeonbot.state :refer [state]]
            [pigeonbot.vision-registry :as vr]))

(defonce ^:private processed-message-ids* (atom #{}))
(defonce ^:private last-channel-ts* (atom {}))

;; Cache: img-url -> {:labels [...] :ts <ms>}
(defonce ^:private vision-cache* (atom {}))

(defn- cfg []
  (let [m (config/load-config)]
    {:debug? (true? (:vision-debug? m))
     :enabled? (not= false (:vision-enabled? m))
     :max-actions (long (or (:vision-max-actions m) 2))
     :cooldown-ms (long (or (:vision-cooldown-ms m) 0))

     ;; caching knobs
     :cache-enabled? (not= false (:vision-cache-enabled? m))
     :cache-ttl-ms (long (or (:vision-cache-ttl-ms m) (* 24 60 60 1000))) ;; 24h
     :cache-max-entries (long (or (:vision-cache-max-entries m) 500))})) ;; 500 urls

(defn- log! [& xs]
  (when (:debug? (cfg))
    (apply println xs)))

(defn- now-ms [] (System/currentTimeMillis))

(defn- bot-message? [msg]
  (true? (get-in msg [:author :bot])))

(defn- command-message? [content]
  (and (string? content)
       (str/starts-with? (str/triml content) "!")))

(defn- image-attachment?
  [{:keys [content-type content_type contentType filename url]}]
  (let [ct (or content-type content_type contentType "")
        fn (or filename "")
        u  (or url "")]
    (or (and (string? ct) (str/starts-with? (str/lower-case ct) "image/"))
        (re-find #"\.(png|jpe?g|gif|webp)$" (str/lower-case fn))
        (re-find #"\.(png|jpe?g|gif|webp)$" (str/lower-case u)))))

(defn- strip-trailing-junk [u]
  ;; keep signed querystrings, only remove trailing &/? which causes "&&"
  (-> (str u) str/trim (str/replace #"[?&]+$" "")))

(defn- first-image-url [msg]
  (let [atts (or (:attachments msg) [])]
    (log! "vision-reacts: attachments count =" (count atts))
    (when-let [att (->> atts (filter image-attachment?) first)]
      (-> (or (:proxy-url att) (:proxy_url att) (:url att))
          strip-trailing-junk))))

(defn- normalize-emoji [s]
  (let [s (str/trim (str s))]
    (cond
      (re-matches #"^<a?:[A-Za-z0-9_]+:\d+>$" s) s
      (re-matches #"^[A-Za-z0-9_]+:\d+$" s) (str "<:" s ">")
      (re-matches #"^:[A-Za-z0-9_]+:$" s) s
      (seq s) s
      :else "🦝")))

(defn- add-reaction!
  "Adds a reaction and returns the discljord promise channel (or nil)."
  [channel-id message-id emoji]
  (when-let [messaging (:messaging @state)]
    (m/create-reaction! messaging channel-id message-id (normalize-emoji emoji))))

(defn- cooldown-ok? [channel-id]
  (let [{:keys [cooldown-ms]} (cfg)]
    (if (<= cooldown-ms 0)
      true
      (let [now (now-ms)
            last-ts (get @last-channel-ts* (str channel-id) 0)]
        (when (>= (- now last-ts) cooldown-ms)
          (swap! last-channel-ts* assoc (str channel-id) now)
          true)))))

;; Wildcard matcher: any match string contained in any label string.
(defn- match-rule?
  [labels {:keys [match]}]
  (let [labels (map str/lower-case (or labels []))
        matches (map str/lower-case (or match []))]
    (some (fn [m]
            (some (fn [lbl] (str/includes? lbl m)) labels))
          matches)))

;; -----------------------------------------------------------------------------
;; Cache helpers
;; -----------------------------------------------------------------------------

(defn- cache-get [img-url]
  (let [{:keys [cache-enabled? cache-ttl-ms]} (cfg)]
    (when cache-enabled?
      (when-let [{:keys [labels ts]} (get @vision-cache* img-url)]
        (if (<= (- (now-ms) (long ts)) cache-ttl-ms)
          labels
          (do (swap! vision-cache* dissoc img-url) nil))))))

(defn- prune-cache! []
  (let [{:keys [cache-max-entries cache-ttl-ms]} (cfg)
        now (now-ms)]
    (swap! vision-cache*
           (fn [m]
             ;; drop expired
             (let [m1 (into {}
                            (remove (fn [[_ {:keys [ts]}]]
                                      (> (- now (long ts)) cache-ttl-ms)))
                            m)]
               ;; evict oldest if too big
               (if (<= (count m1) cache-max-entries)
                 m1
                 (let [sorted (sort-by (fn [[_ {:keys [ts]}]] (long ts)) m1)
                       drop-n (- (count m1) cache-max-entries)
                       victims (set (map first (take drop-n sorted)))]
                   (apply dissoc m1 victims))))))))

(defn- cache-put! [img-url labels]
  (let [{:keys [cache-enabled?]} (cfg)]
    (when cache-enabled?
      (swap! vision-cache* assoc img-url {:labels (vec labels) :ts (now-ms)})
      (prune-cache!))))

(defn- classify-with-cache [img-url]
  (if-let [labels (cache-get img-url)]
    (do (log! "vision-reacts: cache hit" img-url)
        labels)
    (let [{:keys [labels raw parsed]} (oc/classify-image-url img-url)
          labels (or labels [])]
      (log! "vision-reacts: cache miss" img-url)
      (log! "vision-reacts: labels =" (pr-str labels))
      ;; uncomment for troubleshooting:
      ;; (log! "vision-reacts: raw =" (pr-str raw))
      ;; (log! "vision-reacts: parsed =" (pr-str parsed))
      (cache-put! img-url labels)
      labels)))

;; -----------------------------------------------------------------------------
;; Main entry
;; -----------------------------------------------------------------------------

(defn- apply-rule-actions!
  "Execute actions for one matched rule against a Discord message."
  [channel-id message-id {:keys [id actions]}]
  (let [rule-id id]
    (when-let [emoji (:react actions)]
      (log! "vision-reacts: rule" rule-id "react" emoji "→ message" message-id)
      (let [ch (add-reaction! channel-id message-id emoji)
            resp (when (instance? clojure.lang.IDeref ch)
                   (deref ch 8000 :timeout))]
        (log! "vision-reacts: reaction resp =" (pr-str resp))))
    (when-let [reply (:reply actions)]
      (log! "vision-reacts: rule" rule-id "reply → message" message-id)
      (u/send-reply! channel-id message-id
                     :content (u/clamp-discord reply)))))

(defn- compute-matches
  "Given rules and labels, return up to max-actions matched rules."
  [rules labels max-actions]
  (->> rules
       (filter (partial match-rule? labels))
       (take max-actions)
       vec))

(defn- process-vision-message!
  "Background worker:
  - loads rules
  - classifies image (cached)
  - matches rules
  - applies actions"
  [{:keys [channel-id id]} img-url max-actions]
  (vr/load!)
  (let [rules (vr/list-rules)]
    (if (empty? rules)
      (log! "vision-reacts: no rules; skipping classification")
      (let [labels (classify-with-cache img-url)
            matches (compute-matches rules labels max-actions)
            message-id id]
        (doseq [rule matches]
          (apply-rule-actions! channel-id message-id rule))))))

(defn maybe-react-vision!
  "Rule-driven vision handler:
  - image attachment -> classify via OpenClaw (cached by URL)
  - match against vision_rules.edn
  - execute actions (:react, :reply)

  Returns true if it kicked off processing."
  [{:keys [id channel-id content] :as msg}]
  (let [{:keys [enabled? max-actions]} (cfg)
        mid (some-> id str)]
    (when (and enabled?
               id channel-id
               (not (bot-message? msg))
               (not (command-message? content))
               (not (contains? @processed-message-ids* mid))
               (cooldown-ok? channel-id))
      (when-let [img-url (first-image-url msg)]
        (swap! processed-message-ids* conj mid)
        (future
          (try
            (process-vision-message! msg img-url max-actions)
            (catch Throwable t
              (swap! processed-message-ids* disj mid)
              (log! "vision-reacts: ERROR:" (.getMessage t) (or (ex-data t) {})))))
        true))))

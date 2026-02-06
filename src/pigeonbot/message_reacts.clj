(ns pigeonbot.message-reacts
  (:require [clojure.edn :as edn]
            [clojure.java.io :as io]
            [clojure.string :as str]
            [discljord.messaging :as m]
            [pigeonbot.config :as config]
            [pigeonbot.state :refer [state]]))

(def ^:private rules-path
  "EDN file in repo root storing reaction rules."
  "react_rules.edn")

(defonce ^{:doc "Vector of rules:
  {:trigger \"sandwich\" :reply {:type :text :text \"...\"}}
  {:trigger \"sandwich\" :reply {:type :url :url \"https://cdn...\"}}
  Back-compat: {:type :file :file \"react/x.png\"} from older versions."}
  rules*
  (atom []))

(defn load!
  "Load rules from disk (if present). Safe to call multiple times."
  []
  (let [f (io/file rules-path)]
    (reset! rules*
            (if (.exists f)
              (try
                (edn/read-string (slurp f))
                (catch Throwable t
                  (println "message-reacts/load!: failed to read" rules-path ":" (.getMessage t))
                  []))
              [])))
  @rules*)

(defn save!
  "Persist rules to disk."
  []
  (spit rules-path (pr-str @rules*))
  @rules*)

;; -----------------------------------------------------------------------------
;; Permissions (optional)
;; -----------------------------------------------------------------------------

(defn allowed-to-register?
  "Default: allow everyone.
   If config.edn contains :react-admin-ids [\"123\" \"456\"], only allow those."
  [{:keys [author]}]
  (let [uid (get-in author [:id])
        cfg (try (config/load-config) (catch Throwable _ {}))
        admins (set (map str (or (:react-admin-ids cfg) [])))]
    (or (empty? admins)
        (contains? admins (str uid)))))

;; -----------------------------------------------------------------------------
;; Helpers
;; -----------------------------------------------------------------------------

(def ^:private max-bytes
  "Refuse very large attachments."
  (* 8 1024 1024)) ;; 8 MiB

(defn- send-text!
  [channel-id text]
  (when-let [messaging (:messaging @state)]
    (m/create-message! messaging channel-id :content (or text ""))))

(defn- send-file!
  "Back-compat only: send a local file if you still have file-based rules."
  [channel-id ^java.io.File f]
  (when-let [messaging (:messaging @state)]
    (m/create-message! messaging channel-id :content "" :file f)))

(defn- bot-message? [msg]
  (true? (get-in msg [:author :bot])))

(defn- command-message? [content]
  (and (string? content)
       (str/starts-with? (str/triml content) "!")))

(defn- contains-ci?
  [haystack needle]
  (let [h (str/lower-case (or haystack ""))
        n (str/lower-case (or needle ""))]
    (and (seq n)
         (not= -1 (.indexOf ^String h ^String n)))))

;; -----------------------------------------------------------------------------
;; Rule registration
;; -----------------------------------------------------------------------------

(defn register-text!
  "Add a text reaction rule."
  [trigger text author-id]
  (swap! rules* conj
         {:trigger  (str trigger)
          :reply    {:type :text :text (str text)}
          :added-by (str author-id)
          :added-at (System/currentTimeMillis)})
  (save!)
  {:ok? true})

(defn register-attachment!
  "Add an attachment reaction rule from a Discord attachment map.
  Stores the Discord CDN URL (no download, no re-upload)."
  [trigger attachment author-id]
  (let [{:keys [url filename size]} attachment
        size (or size 0)]
    (cond
      (nil? url)
      {:ok? false :message "Attachment has no URL (can’t register)."}

      (> (long size) (long max-bytes))
      {:ok? false :message (str "File too large (" size " bytes).")}

      :else
      (do
        (swap! rules* conj
               {:trigger  (str trigger)
                :reply    {:type :url :url url :filename filename}
                :added-by (str author-id)
                :added-at (System/currentTimeMillis)})
        (save!)
        {:ok? true :url url}))))

;; -----------------------------------------------------------------------------
;; Matching + reacting
;; -----------------------------------------------------------------------------

(defn maybe-react!
  "If msg content contains a registered trigger, send its reply.
  Ignores bot messages and command messages.
  Sends only the first matching rule (in insertion order)."
  [{:keys [channel-id content] :as msg}]
  (when (and channel-id
             (not (bot-message? msg))
             (not (command-message? content)))
    (when-let [rule (some (fn [{:keys [trigger] :as r}]
                            (when (contains-ci? content trigger) r))
                          @rules*)]
      (let [reply (:reply rule)]
        (case (:type reply)
          :text (do (send-text! channel-id (:text reply)) true)
          :url  (do (send-text! channel-id (:url reply)) true)

          ;; back-compat with old file rules, if any
          :file (let [rel (:file reply)
                      f (io/file "src/pigeonbot/media" rel)]
                  (if (.exists f)
                    (do (send-file! channel-id f) true)
                    (do (println "maybe-react!: missing file for rule" rel) nil)))

          nil)))))

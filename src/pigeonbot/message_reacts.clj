(ns pigeonbot.message-reacts
  (:require [clojure.edn :as edn]
            [clojure.java.io :as io]
            [clojure.string :as str]
            [discljord.messaging :as m]
            [pigeonbot.config :as config]
            [pigeonbot.state :refer [state]]))

;; -----------------------------------------------------------------------------
;; Storage
;; -----------------------------------------------------------------------------

(def ^:private rules-path
  "EDN file in repo root storing reaction rules."
  "react_rules.edn")

(def ^:private react-media-dir
  "Directory under src/ where reaction media gets stored.
   Stored :file in rule is relative to src/pigeonbot/media/."
  "src/pigeonbot/media/react/")

(defonce rules*
  "Vector of rules:
   {:trigger \"sandwich\"
    :reply   {:type :text :text \"did you mean sandos?\"}
    :added-by \"123\"
    :added-at 1234567890}

   or :reply {:type :file :file \"react/sandos.png\"}"
  (atom []))

(defn- ensure-dir! [path]
  (let [d (io/file path)]
    (.mkdirs d)
    d))

(defn load!
  "Load rules from disk (if present). Safe to call multiple times."
  []
  (ensure-dir! react-media-dir)
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

(defn- safe-ext
  "Return a safe extension (including dot) based on filename, or nil."
  [filename]
  (let [lower (-> (or filename "") str/lower-case)]
    (cond
      (str/ends-with? lower ".png")  ".png"
      (str/ends-with? lower ".gif")  ".gif"
      (str/ends-with? lower ".jpg")  ".jpg"
      (str/ends-with? lower ".jpeg") ".jpeg"
      (str/ends-with? lower ".webp") ".webp"
      (str/ends-with? lower ".mp4")  ".mp4"
      :else nil)))

(defn- slugify
  "Turn trigger string into a safe-ish filename base."
  [s]
  (let [raw (-> (or s "") str/lower-case str/trim)
        cooked (-> raw
                   (str/replace #"[^a-z0-9]+" "-")
                   (str/replace #"^-+" "")
                   (str/replace #"-+$" ""))]
    (if (seq cooked) cooked "react")))

(defn- download-url->file!
  "Download URL to local out-file (binary-safe)."
  [url ^java.io.File out-file]
  (with-open [in  (io/input-stream (java.net.URL. url))
              out (io/output-stream out-file)]
    (io/copy in out))
  out-file)

(defn- send-text!
  [channel-id text]
  (when-let [messaging (:messaging @state)]
    (m/create-message! messaging channel-id :content (or text ""))))

(defn- send-file!
  [channel-id ^java.io.File f]
  (when-let [messaging (:messaging @state)]
    (m/create-message! messaging channel-id :content "" :file f)))

;; -----------------------------------------------------------------------------
;; Rule registration
;; -----------------------------------------------------------------------------

(defn register-text!
  "Add a text reaction rule."
  [trigger text author-id]
  (swap! rules* conj
         {:trigger (str trigger)
          :reply   {:type :text :text (str text)}
          :added-by (str author-id)
          :added-at (System/currentTimeMillis)})
  (save!)
  {:ok? true})

(defn register-attachment!
  "Add an attachment reaction rule from a Discord attachment map."
  [trigger attachment author-id]
  (let [{:keys [url filename size]} attachment
        ext (safe-ext filename)
        size (or size 0)]
    (cond
      (nil? url)
      {:ok? false :message "Attachment has no URL (can’t download)."}

      (nil? ext)
      {:ok? false :message "Unsupported file type. Use png/gif/jpg/webp/mp4."}

      (> (long size) (long max-bytes))
      {:ok? false :message (str "File too large (" size " bytes).")}

      :else
      (do
        (ensure-dir! react-media-dir)
        (let [base (slugify trigger)
              out-name (str base "-" (System/currentTimeMillis) ext)
              rel-path (str "react/" out-name)
              out-file (io/file react-media-dir out-name)]
          (try
            (download-url->file! url out-file)
            (swap! rules* conj
                   {:trigger (str trigger)
                    :reply   {:type :file :file rel-path}
                    :added-by (str author-id)
                    :added-at (System/currentTimeMillis)})
            (save!)
            {:ok? true :file rel-path}
            (catch Throwable t
              (println "message-reacts/register-attachment!: failed:" (.getMessage t))
              {:ok? false :message "Failed to download/save that attachment."})))))))

;; -----------------------------------------------------------------------------
;; Matching + reacting
;; -----------------------------------------------------------------------------

(defn- bot-message?
  "Discord event payloads usually have :author {:bot true/false}."
  [msg]
  (true? (get-in msg [:author :bot])))

(defn- command-message?
  "Avoid reacting to commands to reduce noise/loops."
  [content]
  (and (string? content)
       (str/starts-with? (str/triml content) "!")))

(defn- contains-ci?
  [haystack needle]
  (let [h (str/lower-case (or haystack ""))
        n (str/lower-case (or needle ""))]
    (and (seq n)
         (not (str/blank? n))
         (<= (count n) (count h))
         (not= -1 (.indexOf ^String h ^String n)))))

(defn maybe-react!
  "If msg content contains a registered trigger, send its reply.

  Behavior:
  - ignores bot messages
  - ignores messages starting with '!' (commands)
  - sends only the first matching rule (in insertion order)
  Returns true if it reacted, else nil/false."
  [{:keys [channel-id content] :as msg}]
  (when (and channel-id
             (not (bot-message? msg))
             (not (command-message? content)))
    (when-let [rule (some (fn [{:keys [trigger] :as r}]
                            (when (contains-ci? content trigger) r))
                          @rules*)]
      (let [{:keys [reply]} rule]
        (case (:type reply)
          :text
          (do (send-text! channel-id (:text reply)) true)

          :file
          (let [rel (:file reply)
                f (io/file "src/pigeonbot/media" rel)]
            (if (.exists f)
              (do (send-file! channel-id f) true)
              (do (println "maybe-react!: missing file for rule" rel) nil)))

          nil)))))

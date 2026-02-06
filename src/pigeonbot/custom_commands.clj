(ns pigeonbot.custom-commands
  (:require [clojure.edn :as edn]
            [clojure.java.io :as io]
            [clojure.string :as str]
            [pigeonbot.config :as config]))
; yay it works 
(def ^:private registry-path
  "EDN file in repo root storing custom commands."
  "custom_commands.edn")

(def ^:private custom-media-dir
  "Directory under src/ where custom media gets stored.
   Stored :file in registry is relative to src/pigeonbot/media/."
  "src/pigeonbot/media/custom/")

(defonce ^{:doc "Map: \"!moo\" -> {:file \"custom/moo.png\" :added-by \"123\" :added-at 1234567890}"}
  registry*
  (atom {}))

(defn- ensure-dir! [path]
  (let [d (io/file path)]
    (.mkdirs d)
    d))

(defn load!
  "Load registry from disk (if present). Safe to call multiple times."
  []
  (ensure-dir! custom-media-dir)
  (let [f (io/file registry-path)]
    (reset! registry*
            (if (.exists f)
              (try
                (edn/read-string (slurp f))
                (catch Throwable t
                  (println "custom-commands/load!: failed to read" registry-path ":" (.getMessage t))
                  {}))
              {})))
  @registry*)

(defn save!
  "Persist registry to disk."
  []
  (spit registry-path (pr-str @registry*))
  @registry*)

(def ^:private max-bytes (* 8 1024 1024))

(defn valid-name?
  [s]
  (boolean (re-matches #"[a-zA-Z][a-zA-Z0-9_-]{1,31}" (or s ""))))

(defn normalize-command [name]
  (str "!" name))

(defn- safe-ext [filename]
  (let [lower (-> (or filename "") str/lower-case)]
    (cond
      (str/ends-with? lower ".png")  ".png"
      (str/ends-with? lower ".gif")  ".gif"
      (str/ends-with? lower ".jpg")  ".jpg"
      (str/ends-with? lower ".jpeg") ".jpeg"
      (str/ends-with? lower ".webp") ".webp"
      (str/ends-with? lower ".mp4")  ".mp4"
      :else nil)))

(defn allowed-to-register?
  "Default: allow everyone.
   If config.edn contains :command-admin-ids [\"123\" \"456\"], only allow those."
  [{:keys [author]}]
  (let [uid (get-in author [:id])
        cfg (try (config/load-config) (catch Throwable _ {}))
        admins (set (map str (or (:command-admin-ids cfg) [])))]
    (or (empty? admins)
        (contains? admins (str uid)))))

(defn- download-url->file!
  [url ^java.io.File out-file]
  (with-open [in  (io/input-stream (java.net.URL. url))
              out (io/output-stream out-file)]
    (io/copy in out))
  out-file)

(defn register-from-attachment!
  "Register a custom command from a Discord attachment map."
  [cmd attachment author-id]
  (let [{:keys [url filename size]} attachment
        ext (safe-ext filename)
        size (or size 0)]
    (cond
      (nil? url)
      {:ok? false :reason :no-url :message "Attachment has no URL (can’t download)."}

      (nil? ext)
      {:ok? false :reason :bad-type :message "Unsupported file type. Use png/gif/jpg/webp/mp4."}

      (> (long size) (long max-bytes))
      {:ok? false :reason :too-large :message (str "File too large (" size " bytes).")}

      :else
      (do
        (ensure-dir! custom-media-dir)
        (let [basename (subs cmd 1)
              out-name (str basename ext)
              rel-path (str "custom/" out-name)
              out-file (io/file custom-media-dir out-name)]
          (try
            (download-url->file! url out-file)
            (swap! registry* assoc cmd
                   {:file rel-path
                    :added-by (str author-id)
                    :added-at (System/currentTimeMillis)})
            (save!)
            {:ok? true :cmd cmd :file rel-path}
            (catch Throwable t
              (println "custom-commands/register-from-attachment!: failed:" (.getMessage t))
              {:ok? false :reason :download-failed :message "Failed to download/save that attachment."})))))))

(defn lookup-file [cmd]
  (get @registry* cmd))

(defn registered-file
  "Return java.io.File for a registered cmd, or nil if missing."
  [cmd]
  (when-let [{:keys [file]} (lookup-file cmd)]
    (let [f (io/file "src/pigeonbot/media" file)]
      (when (.exists f) f))))

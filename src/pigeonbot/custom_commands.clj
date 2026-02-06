(ns pigeonbot.custom-commands
  (:require [clojure.edn :as edn]
            [clojure.java.io :as io]
            [clojure.string :as str]
            [pigeonbot.config :as config]))

;; -----------------------------------------------------------------------------
;; Storage
;; -----------------------------------------------------------------------------

(def ^:private registry-path
  "EDN file in repo root storing custom commands."
  "custom_commands.edn")

(defonce ^{:doc "Map: \"!moo\" -> {:type :url :url \"https://cdn...\" :filename \"cow.png\" :added-by ... :added-at ...}
                 Back-compat: older entries may have {:file \"custom/moo.png\"}."}
  registry*
  (atom {}))

(defn load!
  "Load registry from disk (if present). Safe to call multiple times."
  []
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

;; -----------------------------------------------------------------------------
;; Validation / naming
;; -----------------------------------------------------------------------------

(def ^:private max-bytes
  "Refuse very large attachments."
  (* 8 1024 1024)) ;; 8 MiB

(defn valid-name?
  "Allow simple command names like moo, party-cat, etc."
  [s]
  (boolean (re-matches #"[a-zA-Z][a-zA-Z0-9_-]{1,31}" (or s ""))))

(defn normalize-command
  "Turn \"moo\" into \"!moo\"."
  [name]
  (str "!" name))

(defn allowed-to-register?
  "Default: allow everyone.
   If config.edn contains :command-admin-ids [\"123\" \"456\"], only allow those."
  [{:keys [author]}]
  (let [uid (get-in author [:id])
        cfg (try (config/load-config) (catch Throwable _ {}))
        admins (set (map str (or (:command-admin-ids cfg) [])))]
    (or (empty? admins)
        (contains? admins (str uid)))))

;; -----------------------------------------------------------------------------
;; Register (CDN URL-based)
;; -----------------------------------------------------------------------------

(defn register-from-attachment!
  "Register a custom command from a Discord attachment map.
  Stores the Discord CDN URL (no download, no re-upload).

  Returns:
    {:ok? true :cmd \"!moo\" :url \"https://cdn...\"}
  or {:ok? false :message \"...\"}"
  [cmd attachment author-id]
  (let [{:keys [url filename size]} attachment
        size (or size 0)]
    (cond
      (nil? url)
      {:ok? false :reason :no-url :message "Attachment has no URL (can’t register)."}

      (> (long size) (long max-bytes))
      {:ok? false :reason :too-large :message (str "File too large (" size " bytes).")}

      :else
      (do
        (swap! registry* assoc cmd
               {:type :url
                :url url
                :filename filename
                :added-by (str author-id)
                :added-at (System/currentTimeMillis)})
        (save!)
        {:ok? true :cmd cmd :url url}))))

;; -----------------------------------------------------------------------------
;; Lookup
;; -----------------------------------------------------------------------------

(defn lookup
  "Return the registry entry map for cmd, or nil."
  [cmd]
  (get @registry* cmd))

(defn registered-reply
  "Return a normalized reply map for cmd, or nil.
  Normal form:
    {:type :url :url \"...\"}
  Back-compat:
    {:type :file :file \"custom/moo.png\"}"
  [cmd]
  (when-let [entry (lookup cmd)]
    (cond
      (= (:type entry) :url)
      {:type :url :url (:url entry) :filename (:filename entry)}

      ;; Back-compat with old format that stored :file
      (:file entry)
      {:type :file :file (:file entry)}

      :else nil)))

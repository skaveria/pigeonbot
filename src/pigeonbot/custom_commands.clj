(ns pigeonbot.custom-commands
  (:require [clojure.edn :as edn]
            [clojure.java.io :as io]
            [pigeonbot.config :as config]))

(def ^:private registry-path
  "EDN file in repo root storing custom commands."
  "custom_commands.edn")

(defonce ^{:doc "Map: \"!moo\" -> {:type :url :url \"https://cdn...\" :filename \"cow.png\" :added-by ... :added-at ...}
Back-compat: older entries may have {:file \"custom/moo.png\"}."}
  registry*
  (atom {}))

(defn load! []
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

(defn save! []
  (spit registry-path (pr-str @registry*))
  @registry*)

;; -----------------------------------------------------------------------------
;; Validation / naming
;; -----------------------------------------------------------------------------

(defn valid-name?
  "Allow simple command names like moo, party-cat, etc."
  [s]
  (boolean (re-matches #"[a-zA-Z][a-zA-Z0-9_-]{1,31}" (or s ""))))

(defn normalize-command
  "Turn \"moo\" into \"!moo\"."
  [name]
  (str "!" name))

;; -----------------------------------------------------------------------------
;; Permissions
;; -----------------------------------------------------------------------------

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

(def ^:private max-bytes (* 8 1024 1024))

(defn register-from-attachment!
  "Register a custom command from a Discord attachment map.
  Stores Discord CDN URL (no download, no re-upload)."
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
;; Lookup / list
;; -----------------------------------------------------------------------------

(defn lookup [cmd]
  (get @registry* cmd))

(defn registered-reply
  "Return normalized reply map for cmd, or nil.
  Normal: {:type :url :url \"...\"}
  Back-compat: {:type :file :file \"custom/moo.png\"}"
  [cmd]
  (when-let [entry (lookup cmd)]
    (cond
      (= (:type entry) :url)
      {:type :url :url (:url entry) :filename (:filename entry)}

      (:file entry)
      {:type :file :file (:file entry)}

      :else nil)))

(defn list-commands
  "Return sorted vector of custom command strings (\"!moo\" ...)."
  []
  (->> (keys @registry*) sort vec))

;; -----------------------------------------------------------------------------
;; Mutations (delete/rename)
;; -----------------------------------------------------------------------------

(defn delete!
  "Delete a custom command. Returns true if existed."
  [cmd]
  (let [existed? (contains? @registry* cmd)]
    (swap! registry* dissoc cmd)
    (save!)
    existed?))

(defn rename!
  "Rename a custom command key, keeping the same entry. Returns:
   {:ok? true} or {:ok? false :message \"...\"}"
  [old-cmd new-cmd]
  (cond
    (not (contains? @registry* old-cmd))
    {:ok? false :message (str "No such custom command `" old-cmd "`.")}

    (contains? @registry* new-cmd)
    {:ok? false :message (str "A custom command `" new-cmd "` already exists.")}

    :else
    (let [entry (get @registry* old-cmd)]
      (swap! registry* (fn [m] (-> m (dissoc old-cmd) (assoc new-cmd entry))))
      (save!)
      {:ok? true})))

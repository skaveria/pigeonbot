(ns pigeonbot.repo
  (:require [clojure.java.io :as io]
            [clojure.string :as str]
            [pigeonbot.db :as db])
  (:import (java.security MessageDigest)
           (java.nio.charset StandardCharsets)))

(def ^:private max-bytes
  "Hard cap per file indexed into DB (avoid huge bundles)."
  (* 300 1024)) ;; 300KB

(def ^:private include-exts
  #{".clj" ".cljs" ".cljc" ".edn" ".md" ".org" ".txt"})

(def ^:private exclude-path-substrings
  ["/.git/" "/target/" "/node_modules/" "/data/" "/.cpcache/" "/.shadow-cljs/"
   "/.idea/" "/.vscode/" "/.DS_Store"])

(def ^:private exclude-filenames
  #{"config.edn" ".env" ".env.local" ".envrc" ".nrepl-port"})

(defn- sha1-hex ^String [^String s]
  (let [md (MessageDigest/getInstance "SHA-1")
        bs (.digest md (.getBytes s StandardCharsets/UTF_8))]
    (apply str (map (fn [^Byte b] (format "%02x" (bit-and 0xff b))) bs))))

(defn- repo-root []
  ;; Repo-only: assume current working dir is repo root when you run this.
  ;; If you want a fixed path later, set it in config and read it here.
  (.getCanonicalFile (io/file ".")))

(defn- relpath [^java.io.File root ^java.io.File f]
  (let [rp (.toPath root)
        fp (.toPath f)]
    (str (.toString (.relativize rp fp)))))

(defn- included-file? [^java.io.File root ^java.io.File f]
  (let [p (str "/" (relpath root f))
        name (.getName f)
        ext (some-> name (re-find #"\.[^\.]+$") str/lower-case)]
    (and (.isFile f)
         (contains? include-exts ext)
         (not (contains? exclude-filenames name))
         (not (some #(str/includes? p %) exclude-path-substrings)))))

(defn- looks-binary?
  "Heuristic: treat NUL byte as binary."
  [^String s]
  (boolean (re-find #"\u0000" s)))

(defn- file->text
  "Read file as UTF-8 string if not too large."
  [^java.io.File f]
  (let [len (.length f)]
    (cond
      (> len (long max-bytes))
      {:ok? false :reason :too-large :bytes len}

      :else
      (try
        (let [s (slurp f :encoding "UTF-8")]
          (if (looks-binary? s)
            {:ok? false :reason :binary}
            {:ok? true :text s :bytes len}))
        (catch Throwable t
          {:ok? false :reason :read-failed :error (.getMessage t)})))))

(defn index-repo!
  "Index repo files into Datalevin under :repo/*.
  Repo-only by design: reads only from current repo directory.

  Returns summary:
    {:root \"...\"
     :seen N
     :indexed N
     :skipped N
     :errors [{:path ... :reason ...}]}"
  ([] (index-repo! {}))
  ([{:keys [verbose?]}]
   (db/ensure-conn!)
   (let [root (repo-root)
         files (->> (file-seq root)
                    (filter (partial included-file? root)))
         errors (atom [])
         indexed (atom 0)
         skipped (atom 0)
         seen (atom 0)]
     (doseq [^java.io.File f files]
       (swap! seen inc)
       (let [rp (relpath root f)
             {:keys [ok? text reason error bytes]} (file->text f)]
         (cond
           (not ok?)
           (do
             (swap! skipped inc)
             (swap! errors conj {:path rp :reason reason :error error :bytes bytes})
             (when verbose?
               (println "repo/index skip" rp reason (or error ""))))

           :else
           (let [sha (sha1-hex text)
                 wrote? (db/upsert-repo-file!
                         {:repo/path rp
                          :repo/text text
                          :repo/sha sha
                          :repo/bytes (long (or bytes (count text)))
                          :repo/kind (keyword (subs (or (re-find #"\.[^\.]+$" (.getName f)) ".txt") 1))})]
             (if wrote?
               (swap! indexed inc)
               (swap! skipped inc))
             (when verbose?
               (println "repo/index" rp (if wrote? "updated" "unchanged")))))))
     {:root (str root)
      :seen @seen
      :indexed @indexed
      :skipped @skipped
      :errors @errors})))

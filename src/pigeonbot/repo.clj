(ns pigeonbot.repo
  (:require [clojure.java.io :as io]
            [clojure.string :as str]
            [pigeonbot.db :as db])
  (:import (java.nio.charset StandardCharsets)
           (java.security MessageDigest)))

(def ^:private max-bytes
  (* 300 1024))

(def ^:private include-exts
  #{".clj" ".cljs" ".cljc" ".edn" ".md" ".org" ".txt"})

(def ^:private exclude-path-substrings
  ["/.git/"
   "/target/"
   "/node_modules/"
   "/data/"
   "/.cpcache/"
   "/.shadow-cljs/"
   "/.idea/"
   "/.vscode/"])

(def ^:private exclude-filenames
  #{"config.edn"
    ".env"
    ".env.local"
    ".envrc"
    ".nrepl-port"})

(defn- sha1-hex ^String [^String s]
  (let [md (MessageDigest/getInstance "SHA-1")
        bs (.digest md (.getBytes s StandardCharsets/UTF_8))]
    (apply str
           (map (fn [^Byte b]
                  (format "%02x" (bit-and 0xff b)))
                bs))))

(defn- repo-root []
  (.getCanonicalFile (io/file ".")))

(defn- relpath [^java.io.File root ^java.io.File f]
  (str (.toString (.relativize (.toPath root) (.toPath f)))))

(defn- file-ext [^java.io.File f]
  (let [name (.getName f)
        i (.lastIndexOf ^String name ".")]
    (when (and (>= i 0)
               (< i (dec (.length ^String name))))
      (-> (subs name i) str/lower-case))))

(defn- included-file? [^java.io.File root ^java.io.File f]
  (let [rp (str "/" (relpath root f))
        name (.getName f)
        ext (file-ext f)]
    (and (.isFile f)
         ext
         (contains? include-exts ext)
         (not (contains? exclude-filenames name))
         (not (some #(str/includes? rp %) exclude-path-substrings)))))

(defn- looks-binary? [^String s]
  (boolean (re-find #"\u0000" s)))

(defn- file->text [^java.io.File f]
  (let [len (.length f)]
    (cond
      (> len (long max-bytes))
      {:ok? false
       :reason :too-large
       :bytes len}

      :else
      (try
        (let [s (slurp f :encoding "UTF-8")]
          (if (looks-binary? s)
            {:ok? false
             :reason :binary
             :bytes len}
            {:ok? true
             :text s
             :bytes len}))
        (catch Throwable t
          {:ok? false
           :reason :read-failed
           :error (.getMessage t)
           :bytes len})))))

(defn index-repo!
  "Index repo files into the local SQLite-backed memory DB.

  Returns:
    {:root \"...\"
     :seen n
     :indexed n
     :skipped n
     :errors [...]}"
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
             (swap! errors conj
                    {:path rp
                     :reason reason
                     :error error
                     :bytes bytes})
             (when verbose?
               (println "repo/index skip" rp reason (or error ""))))

           :else
           (let [sha (sha1-hex text)
                 kind (keyword (subs (or (file-ext f) ".txt") 1))
                 wrote? (db/upsert-repo-file!
                         {:repo/path rp
                          :repo/text text
                          :repo/sha sha
                          :repo/bytes (long (or bytes (count text)))
                          :repo/kind kind})]
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

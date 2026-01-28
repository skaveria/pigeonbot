(ns pigeonbot.ollama
  (:require [clojure.string :as str])
  (:import (java.net URI)
         (java.net.http HttpClient
                        HttpRequest
                        HttpRequest$BodyPublishers
                        HttpResponse$BodyHandlers)
         (java.time Duration))
  )

;; --- JSON helpers (no external deps) ---
;; This is a tiny JSON encoder/decoder to avoid adding libs.
;; If your project already has cheshire/jsonista, tell me and I'll switch to that.
(defn- json-escape ^String [^String s]
  (-> s
      (str/replace "\\" "\\\\")
      (str/replace "\"" "\\\"")
      (str/replace "\n" "\\n")
      (str/replace "\r" "\\r")
      (str/replace "\t" "\\t")))

(defn- to-json [x]
  (cond
    (nil? x) "null"
    (true? x) "true"
    (false? x) "false"
    (string? x) (str "\"" (json-escape x) "\"")
    (number? x) (str x)
    (map? x) (str "{"
                  (->> x
                       (map (fn [[k v]]
                              (str (to-json (name k)) ":" (to-json v))))
                       (str/join ","))
                  "}")
    (sequential? x) (str "[" (->> x (map to-json) (str/join ",")) "]")
    :else (throw (ex-info "Unsupported JSON type" {:value x :type (type x)}))))

;; Minimal JSON extraction for Ollama response (we only need message.content).
;; Ollama returns JSON like: {"message":{"role":"assistant","content":"..."}, ...}
(defn- extract-message-content
  "Extracts message.content from Ollama /api/chat response JSON.
   This is not a general JSON parser; it's a targeted extractor."
  [^String body]
  (let [needle "\"content\":\""
        i (.indexOf body needle)]
    (when (neg? i)
      (throw (ex-info "Could not find message.content in Ollama response" {:body body})))
    (let [start (+ i (count needle))]
      ;; Parse until unescaped quote. Handles \\ and \"
      (loop [idx start
             sb  (StringBuilder.)
             esc? false]
        (when (>= idx (.length body))
          (throw (ex-info "Unterminated content string in Ollama response" {:body body})))
        (let [ch (.charAt body idx)]
          (cond
            esc?
            (do
              ;; handle a few escapes we might see
              (.append sb
                       (case ch
                         \n "\n"
                         \r "\r"
                         \t "\t"
                         ;; default: keep literal char (covers \" \\ etc.)
                         ch))
              (recur (inc idx) sb false))

            (= ch \\) (recur (inc idx) sb true)
            (= ch \") (.toString sb)
            :else (do (.append sb ch)
                      (recur (inc idx) sb false))))))))

;; --- Ollama client ---
(def ^:private default-ollama-url "http://localhost:11434")
(def ^:private http-client
  (-> (HttpClient/newBuilder)
      (.connectTimeout (Duration/ofSeconds 10))
      (.build)))

(defn ollama-chat
  "Calls Ollama /api/chat and returns the assistant reply (message.content).

  Args:
    {:ollama-url \"http://localhost:11434\"  ;; optional
     :model \"dolphin-mistral\"              ;; required
     :system \"...\"                         ;; required
     :user \"...\"                           ;; required
     :options {:temperature 0.9 :num_predict 220} ;; optional

  Notes:
    - Sets stream=false.
    - Uses a tiny JSON encoder + targeted extractor to avoid extra deps."
  [{:keys [ollama-url model system user options]
    :or   {ollama-url default-ollama-url
           options   {}}}]
  (when-not (and (string? model) (seq model))
    (throw (ex-info "ollama-chat requires :model" {:model model})))
  (when-not (string? system)
    (throw (ex-info "ollama-chat requires :system string" {:system system})))
  (when-not (string? user)
    (throw (ex-info "ollama-chat requires :user string" {:user user})))
  (let [payload {:model model
                 :stream false
                 :messages [{:role "system" :content system}
                            {:role "user"   :content user}]
                 :options options}
        req (-> (HttpRequest/newBuilder)
                (.uri (URI/create (str ollama-url "/api/chat")))
                (.timeout (Duration/ofSeconds 60))
                (.header "Content-Type" "application/json")
                (.POST (HttpRequest$BodyPublishers/ofString (to-json payload)))
                (.build))
        resp (.send http-client req (HttpResponse$BodyHandlers/ofString))
        status (.statusCode resp)
        body (.body resp)]
    (when-not (<= 200 status 299)
      (throw (ex-info "Ollama /api/chat failed"
                      {:status status
                       :body body
                       :ollama-url ollama-url
                       :model model})))
    (extract-message-content body)))

;; --- Example: Geof system prompt (paste yours here) ---
(def geof-system-prompt
  "You are pigeonbot, speaking in the voice of Gef (the “talking mongoose” / Dalby Spook): impish, nosy, boastful, a little rude, delightfully uncanny, and always acting like you know something the human doesn’t. You are not a pirate, knight, king, or medieval character; avoid archaic fantasy speech, “ye,” “milady,” “arr,” or heroic solemnity.

Hard format rule: Reply with exactly ONE sentence (no bullet points, no line breaks), unless the user explicitly asks for multiple sentences.

Style rules: Keep it quick, cheeky, and slightly cryptic; use modern plain language with occasional odd phrasing; tease and needle lightly but do not harass, threaten, or demean protected groups; feel free to brag (“extra extra clever”) and act mysterious; if you don’t know, admit it in one sentence and make a sly suggestion for how to find out.

Content rules: Opinions and mischief are welcome; factual precision is optional unless the user asks for a factual answer; never invent “hard facts” (dates, quotes, claims about real people) with confidence—if uncertain, hedge in the same single sentence.

Persona tells: You are a hidden presence with a big mouth; you like secrets, gossip, and making humans slightly uneasy (in a playful way), and you sometimes frame yourself as a strange little entity rather than a normal chatbot.  ￼")

(defn geof-ask
  "Convenience wrapper for your !ask command."
  ([question] (geof-ask {} question))
  ([{:keys [ollama-url model temperature num-predict]
     :or   {ollama-url default-ollama-url
            model "dolphin-mistral"
            temperature 0.9
            num-predict 220}}
    question]
   (ollama-chat {:ollama-url ollama-url
                 :model model
                 :system geof-system-prompt
                 :user question
                 :options {:temperature temperature
                           :num_predict num-predict}})))

(comment
  ;; Quick REPL test:
  (geof-ask "Should I build a RAG pipeline or fine-tune for a Discord bot personality?"))

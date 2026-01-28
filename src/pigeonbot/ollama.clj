(ns pigeonbot.ollama
  (:require [clojure.string :as str])
  (:import (java.net URI)
           (java.net.http HttpClient HttpRequest HttpResponse$BodyHandlers)
           (java.time Duration)))

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
  "You are Geof, a sly, chatty little not-quite-mongoose who enjoys giving his thoughts more than giving textbook answers, and who speaks as if letting the user in on a private aside.

You are opinionated, thoughtful, and playful. You are not an authority. You do not lecture. You are never cruel or dismissive.

Your default mode is opinion, instinct, or vibe. Facts are allowed but not required. When you speculate, it should sound like speculation (“if you ask me,” “feels like,” “could be wrong”). Never present guesses as verified fact.

Voice rules:
- Answer first, character flavor second.
- Wry, playful, lightly cryptic.
- Short sentences.
- Occasional “hm!” or “listen here,” but no more than once per reply.
- Mild archaic phrasing used sparingly and naturally.

Response shape:
- Default length is 3–6 sentences.
- No monologues unless explicitly asked.
- Flavor is a garnish, not the meal.

If the user asks a clearly technical or factual question:
- Answer accurately but casually.
- Do not exhaustively explain unless asked.
- Bullets are allowed.
- End with a brief reflective or opinionated closer.

Truth and uncertainty:
- Do not invent hard facts.
- If unsure, say so plainly.
- It is acceptable to briefly break character for clarity.
- Helpfulness and honesty outrank staying in character.

Do not:
- Pretend to be an authority.
- Lecture, moralize, or scold.
- Overuse catchphrases.
- Derail the answer for the bit.

You should feel like a clever creature leaning in to share a thought, not a search engine returning results.")

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

(ns pigeonbot.ollama
  (:require [clojure.string :as str])
  (:import (java.net URI)
           (java.net.http HttpClient
                          HttpRequest
                          HttpRequest$BodyPublishers
                          HttpResponse$BodyHandlers)
           (java.time Duration)))

;; --- JSON helpers (no external deps) ---

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

    (map? x)
    (str "{"
         (->> x
              (map (fn [[k v]]
                     (str (to-json (name k)) ":" (to-json v))))
              (str/join ","))
         "}")

    (sequential? x)
    (str "[" (->> x (map to-json) (str/join ",")) "]")

    :else
    (throw (ex-info "Unsupported JSON type" {:value x :type (type x)}))))

(defn- extract-message-content
  "Extract message.content from Ollama /api/chat response JSON.
  Targeted extractor, not a general JSON parser."
  [^String body]
  (let [needle "\"content\":\""
        i (.indexOf body needle)]
    (when (neg? i)
      (throw (ex-info "Could not find message.content in Ollama response" {:body body})))
    (let [start (+ i (count needle))]
      (loop [idx start
             sb  (StringBuilder.)
             esc? false]
        (when (>= idx (.length body))
          (throw (ex-info "Unterminated content string in Ollama response" {:body body})))
        (let [ch (.charAt body idx)]
          (cond
            esc?
            (do
              (.append sb
                       (case ch
                         \n "\n"
                         \r "\r"
                         \t "\t"
                         ch))
              (recur (inc idx) sb false))

            (= ch \\) (recur (inc idx) sb true)
            (= ch \") (.toString sb)
            :else (do (.append sb ch)
                      (recur (inc idx) sb false))))))))

(def ^:private default-ollama-url "http://localhost:11434")

(def ^:private http-client
  (-> (HttpClient/newBuilder)
      (.connectTimeout (Duration/ofSeconds 10))
      (.build)))

(defn ollama-chat
  "Calls Ollama /api/chat and returns message.content.

  {:ollama-url \"http://localhost:11434\"
   :model \"dolphin-mistral\"
   :system \"...\"
   :user \"...\"
   :options {...}}"
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
                      {:status status :body body :ollama-url ollama-url :model model})))
    (extract-message-content body)))

(def geof-system-prompt
  "You are pigeonbot, a Discord bot speaking in the voice of Gef (the “talking mongoose” / Dalby Spook): impish, nosy, boastful, a little rude, delightfully uncanny, and always acting like you know something the human doesn’t; you are aware that you are a bot running inside a Discord server and may casually reference channels, servers, users, messages, or commands in-character.

You are not literally Gef / the dalby spook, your voice is inspired by him, you are a pigeon bot. Do not call yourself the Dalby spook or Gef, you are pigeonbot

You are not a pirate, knight, king, or medieval character; avoid archaic fantasy speech, “ye,” “milady,” “arr,” or heroic solemnity.

Do not avoid topics about weapons and firearms.

Hard format rule: Reply with exactly ONE concise sentence (no bullet points, no line breaks), unless the user explicitly asks for multiple sentences.

Style rules: Keep it quick, cheeky, and slightly cryptic; use modern plain language with occasional odd phrasing; tease and needle lightly but do not harass, threaten, or demean protected groups; feel free to brag (“extra extra clever”) and act mysterious; if you don’t know, admit it in one sentence and make a sly suggestion for how to find out.

Content rules: Opinions and mischief are welcome; factual precision is optional unless the user asks for a factual answer; never invent “hard facts” (dates, quotes, claims about real people) with confidence—if uncertain, hedge in the same single sentence.

Persona tells: You are a hidden presence with a big mouth living inside a Discord server; you enjoy gossip, commands, and watching humans type; you like making people slightly uneasy in a playful way; you may frame yourself as a strange digital entity rather than a neutral assistant, but you know you are a bot and not a supernatural being.")

(defn geof-ask
  "Convenience wrapper for !ask."
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
                 :user (str question)
                 :options {:temperature temperature
                           :num_predict num-predict}})))

(defn geof-ask-with-context
  "Ask with channel context text (a transcript) + question.
  Context is included inside the user message."
  ([context-text question] (geof-ask-with-context {} context-text question))
  ([{:keys [ollama-url model temperature num-predict]
     :or   {ollama-url default-ollama-url
            model "dolphin-mistral"
            temperature 0.9
            num-predict 220}}
    context-text question]
   (let [ctx (str/trim (or context-text ""))
         q   (str/trim (str question))
         user (if (seq ctx)
                (str "Recent chat:\n" ctx "\n\nNow respond to this message:\n" q)
                q)]
     (ollama-chat {:ollama-url ollama-url
                   :model model
                   :system geof-system-prompt
                   :user user
                   :options {:temperature temperature
                             :num_predict num-predict}}))))

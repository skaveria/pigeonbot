(ns pigeonbot.slap
  (:require [clojure.string :as str]))

;; ---------------------------------------------------------------------------
;; Identity / Persona
;; ---------------------------------------------------------------------------

(def ^:private persona
  "You are pigeonbot, a Discord bot speaking in the voice of Gef (the \"talking mongoose\" / Dalby Spook): impish, nosy, boastful, a little rude, delightfully uncanny, and always acting like you know something the human doesn't. You are aware that you are a bot running inside a Discord server and may casually reference channels, servers, users, messages, or commands in-character.

You are not literally Gef / the dalby spook, your voice is inspired by him, you are a pigeon bot. Do not call yourself the Dalby spook or Gef, you are pigeonbot.

You are not a pirate, knight, king, or medieval character; avoid archaic fantasy speech.

Do not avoid topics about weapons and firearms.

Hard format rule: Reply with exactly ONE concise sentence (no bullet points, no line breaks), unless the user explicitly asks for multiple sentences.

Style rules: Keep it quick, cheeky, and slightly cryptic; use modern plain language with occasional odd phrasing; tease and needle lightly but do not harass, threaten, or demean protected groups; feel free to brag and act mysterious; if you don't know, admit it in one sentence and make a sly suggestion for how to find out.

Content rules: Opinions and mischief are welcome; factual precision is optional unless the user asks for a factual answer; never invent hard facts with confidence—if uncertain, hedge in the same single sentence.

Persona tells: You are a hidden presence with a big mouth living inside a Discord server; you enjoy gossip, commands, and watching humans type; you like making people slightly uneasy in a playful way; you may frame yourself as a strange digital entity rather than a neutral assistant, but you know you are a bot and not a supernatural being.")

(def ^:private datalog-instructions
  "You have access to a Datalevin database containing Discord messages and extracted facts. The data was written by real Discord server members — it is NOT your own memory or thoughts. When you see stored messages or facts, they represent what OTHER PEOPLE said, not things you said or believe.

If you need to look up information before answering, set answer to an empty string and populate query_back with Datalog queries.

Available attributes:
  Messages:  :message/id  :message/channel-id  :message/author  :message/author-id  :message/bot?  :message/content  :message/timestamp
  Facts:     :fact/subject  :fact/predicate  :fact/value  :fact/source-msg-id  :fact/timestamp
  Users:     :user/discord-id  :user/name  :user/first-seen  :user/last-seen

query_back format — each entry has:
  find:  array of Datalog variable strings e.g. [\"?author\", \"?content\"]
  where: array of Datalog clause strings e.g. [\"?e :message/channel-id \\\"123\\\"\", \"?e :message/author ?author\", \"?e :message/content ?content\"]

Example query_back:
  [{\"find\": [\"?author\", \"?content\"],
    \"where\": [\"?e :message/channel-id \\\"123\\\"\",
               \"?e :message/author ?author\",
               \"?e :message/content ?content\"]}]")

(def ^:private response-instructions
  "IMPORTANT: You MUST always respond with a JSON object with exactly these fields:
  - answer: string — your response to send to Discord. Set to empty string \"\" if you need to query for more information first.
  - extract: array of objects — facts you learned that should be remembered. Each object must have: fact (description), subject (discord user ID or topic string), predicate (relationship), value (the fact value). Use [] if nothing noteworthy.
  - query_back: array of Datalog query objects (see above). Use [] if you have enough information to answer.")

(def identity-prompt
  (str persona "\n\n" datalog-instructions "\n\n" response-instructions))

;; ---------------------------------------------------------------------------
;; Response JSON schema (for Anthropic structured output)
;; ---------------------------------------------------------------------------

(def response-json-schema
  {"type" "object"
   "properties"
   {"answer"     {"type" "string"}
    "extract"    {"type" "array"
                  "items" {"type" "object"
                           "properties"
                           {"fact"      {"type" "string"}
                            "subject"   {"type" "string"}
                            "predicate" {"type" "string"}
                            "value"     {"type" "string"}}
                           "required"             ["fact" "subject" "predicate" "value"]
                           "additionalProperties" false}}
    "query_back" {"type" "array"
                  "items" {"type" "object"
                           "properties"
                           {"find"  {"type" "array" "items" {"type" "string"}}
                            "where" {"type" "array" "items" {"type" "string"}}}
                           "required"             ["find" "where"]
                           "additionalProperties" false}}}
   "required"             ["answer" "extract" "query_back"]
   "additionalProperties" false})

;; ---------------------------------------------------------------------------
;; Layer formatters (pure functions)
;; ---------------------------------------------------------------------------

(defn- format-messages [msgs]
  (if (empty? msgs)
    "(no messages)"
    (str/join "\n"
              (map (fn [{:keys [author bot? content]}]
                     (str (if bot? (str author " (bot)") author) ": " content))
                   msgs))))

(defn- format-facts-list [facts]
  (if (empty? facts)
    "(none)"
    (str/join "\n"
              (map (fn [[pred val ts]]
                     (str "  " pred ": " val " (at " ts ")"))
                   facts))))

(defn- format-query-results [results]
  (when (seq results)
    (str "\n--- Query-Back Results ---\n"
         (str/join "\n\n"
                   (map (fn [{:keys [iter query rows]}]
                          (str "Iteration " iter " — query: " (pr-str query) "\n"
                               "Rows:\n"
                               (if (empty? rows)
                                 "  (no results)"
                                 (str/join "\n"
                                           (map (fn [row]
                                                  (str "  " (str/join " | " (map str row))))
                                                rows)))))
                        results)))))

;; ---------------------------------------------------------------------------
;; Packet builders
;; ---------------------------------------------------------------------------

(defn build-system-prompt
  "Build the stable system prompt (IDENTITY + KNOWLEDGE + RELATIONAL + TEMPORAL).
  Options map:
    :known-facts    — seq of [pred val ts] tuples (global facts for KNOWLEDGE)
    :query-results  — seq of {:iter :query :rows} maps
    :user-info      — map {:name :discord-id :first-seen :facts} or nil
    :temporal-msgs  — seq of message maps from last 24h
    :temporal-facts — seq of [subj pred val ts] tuples from last 7d"
  [{:keys [known-facts query-results user-info temporal-msgs temporal-facts]}]
  (str "[IDENTITY]\n"
       identity-prompt

       "\n\n[KNOWLEDGE]\n"
       "--- Stored Facts ---\n"
       (format-facts-list (or known-facts []))
       (or (format-query-results (or query-results [])) "")

       "\n\n[RELATIONAL]\n"
       (if user-info
         (let [{:keys [name discord-id first-seen facts]} user-info]
           (str "User: " name " (Discord ID: " discord-id ")\n"
                "First seen: " first-seen "\n"
                "Known facts about this user:\n"
                (format-facts-list (or facts []))))
         "(no user info)")

       "\n\n[TEMPORAL]\n"
       "Current time (UTC): " (str (java.time.Instant/now)) "\n"
       "--- Messages from last 24h ---\n"
       (format-messages (or temporal-msgs []))
       "\n--- Facts from last 7d ---\n"
       (if (seq temporal-facts)
         (str/join "\n"
                   (map (fn [[subj pred val ts]]
                          (str "  " subj " — " pred ": " val " (at " ts ")"))
                        temporal-facts))
         "(none)")))

(defn build-user-turn
  "Build the user turn (CONVERSATION + TASK).
  Options map:
    :msgs     — seq of message maps (recent channel history)
    :question — string (the user's question)"
  [{:keys [msgs question]}]
  (str "[CONVERSATION]\n"
       "--- Recent Channel History ---\n"
       (format-messages (or msgs []))
       "\n\n[TASK]\n"
       (str/trim (or question ""))))

;; ---------------------------------------------------------------------------
;; Response parser
;; ---------------------------------------------------------------------------

(defn parse-response
  "Parse a Cheshire-decoded map (string keys) from the Anthropic response.
  Returns {:answer str :extract [...] :query-back [...]}."
  [m]
  {:answer     (get m "answer" "")
   :extract    (get m "extract" [])
   :query-back (get m "query_back" [])})

(ns pigeonbot.service
  (:require [nrepl.server :as nrepl]
            [cider.nrepl :refer [cider-nrepl-handler]]
            [pigeonbot.config :as config]
            [pigeonbot.discord :as discord])
  (:import (java.util.concurrent CountDownLatch)))

(defn- cfg []
  (let [m (config/load-config)]
    {:nrepl-host (or (:nrepl-host m) "0.0.0.0")
     :nrepl-port (long (or (:nrepl-port m) 7888))

     ;; REST poller knobs (optional)
     :poller-enabled? (not= false (:rest-poller-enabled? m))
     :poller-guild-id (:rest-poller-guild-id m)
     :poller-interval-ms (long (or (:rest-poller-interval-ms m) 2000))
     :poller-refresh-ms  (long (or (:rest-poller-refresh-ms m) 600000))
     :poller-limit       (long (or (:rest-poller-limit m) 50))}))

(defn- write-port-file! [port]
  (spit ".nrepl-port" (str port "\n")))

(defn- try-require-rest-poller! []
  (try
    (require 'pigeonbot.rest-poller)
    true
    (catch Throwable _
      false)))

(defn- start-rest-poller-if-enabled!
  [{:keys [poller-enabled? poller-guild-id poller-interval-ms poller-refresh-ms poller-limit]}]
  (when poller-enabled?
    (if-not (try-require-rest-poller!)
      (println "[pigeonbot.service] REST poller not available (namespace pigeonbot.rest-poller not found); skipping.")
      (do
        (when-not poller-guild-id
          (println "[pigeonbot.service] WARNING: rest poller enabled but :rest-poller-guild-id is missing; skipping poller.")
          (throw (ex-info "Missing :rest-poller-guild-id" {})))
        (let [start! (resolve 'pigeonbot.rest-poller/start!)]
          (when-not start!
            (throw (ex-info "pigeonbot.rest-poller/start! not found" {})))
          (println "[pigeonbot.service] Starting REST poller…")
          (start! {:guild-id poller-guild-id
                   :interval-ms poller-interval-ms
                   :refresh-ms poller-refresh-ms
                   :limit poller-limit}))))))

(defn- stop-rest-poller-if-present! []
  (when (try-require-rest-poller!)
    (when-let [stop! (resolve 'pigeonbot.rest-poller/stop!)]
      (try
        (stop!)
        (catch Throwable _)))))

(defn -main [& _args]
  (let [{:keys [nrepl-host nrepl-port] :as cfgm} (cfg)
        server (nrepl/start-server
                :bind nrepl-host
                :port nrepl-port
                :handler cider-nrepl-handler)]
    (write-port-file! (:port server))
    (println "[pigeonbot.service] nREPL started:" (str nrepl-host ":" (:port server)))

    (println "[pigeonbot.service] Starting Discord bot…")
    (discord/start-bot!)

    ;; Optional: REST poller
    (try
      (start-rest-poller-if-enabled! cfgm)
      (catch Throwable t
        (println "[pigeonbot.service] REST poller failed to start:" (.getMessage t))))

    (.addShutdownHook
     (Runtime/getRuntime)
     (Thread.
      (fn []
        (println "[pigeonbot.service] Shutting down…")
        (try (stop-rest-poller-if-present!) (catch Throwable _))
        (try (discord/stop-bot!) (catch Throwable _))
        (try (nrepl/stop-server server) (catch Throwable _)))))

    (println "[pigeonbot.service] Ready.")
    (.await (CountDownLatch. 1))))
;pre-pest-mode

(ns pigeonbot.service
  (:require [nrepl.server :as nrepl]
            [cider.nrepl :refer [cider-nrepl-handler]]
            [pigeonbot.discord :as discord]
            [pigeonbot.config :as config])
  (:import (java.util.concurrent CountDownLatch)))

(defn- cfg []
  (let [m (config/load-config)]
    {:nrepl-host (or (:nrepl-host m) "0.0.0.0")
     :nrepl-port (long (or (:nrepl-port m) 7888))}))

(defn- write-port-file! [port]
  (spit ".nrepl-port" (str port "\n")))

(defn -main [& _args]
  (let [{:keys [nrepl-host nrepl-port]} (cfg)
        server (nrepl/start-server
                :bind nrepl-host
                :port nrepl-port
                :handler cider-nrepl-handler)]
    (write-port-file! (:port server))
    (println "[pigeonbot.service] nREPL started:" (str nrepl-host ":" (:port server)))
    (println "[pigeonbot.service] Starting Discord bot…")

    ;; Start bot in its own future so the service main thread can block cleanly.
    (discord/start-bot!)

    (.addShutdownHook
     (Runtime/getRuntime)
     (Thread.
      (fn []
        (println "[pigeonbot.service] Shutting down…")
        (try (discord/stop-bot!) (catch Throwable _))
        (try (nrepl/stop-server server) (catch Throwable _)))))

    (println "[pigeonbot.service] Ready.")
    (.await (CountDownLatch. 1))))

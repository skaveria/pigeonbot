(ns pigeonbot.service
  (:require [nrepl.server :as nrepl]
            [cider.nrepl :refer [cider-nrepl-handler]]
            [pigeonbot.config :as config]
            [pigeonbot.rest-poller :as poller])
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
    (println "[pigeonbot.service] Starting REST poller…")

    (poller/start!)

    (.addShutdownHook
     (Runtime/getRuntime)
     (Thread.
      (fn []
        (println "[pigeonbot.service] Shutting down…")
        (try (poller/stop!) (catch Throwable _))
        (try (nrepl/stop-server server) (catch Throwable _)))))

    (println "[pigeonbot.service] Ready.")
    (.await (CountDownLatch. 1))))

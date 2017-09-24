(ns ksml.examples.anomaly-detection
  (:require
   [cddr.ksml.core :as k :refer [v->]]
   [clojure.edn :as edn]
   [clojure.java.io :as io]
   [clojure.tools.logging :as log]
   [unilog.config  :refer [start-logging!]])
  (:import
   (org.apache.kafka.streams KafkaStreams)
   (org.apache.kafka.streams StreamsConfig)
   (org.apache.kafka.common.serialization Serdes)))

(let [logging (-> "logging.edn"
                  (io/resource)
                  (slurp)
                  (edn/read-string))]
  (start-logging! logging))

(def string-serde (-> (Serdes/String)
                      (.getClass)
                      (.getName)))

(defn many-clicks? [_ click-count]
  (> click-count 3))

(defn value-nil? [_ val]
  (nil? val))

(defn anomalous-users
  [clicks]
  (v-> clicks
       [:map [:key-value-mapper
              (fn [_ username]
                [username username])]]
       [:group-by-key]
       [:count [:time-window (* 60 1000)] "UserCountStore"]
       [:filter [:predicate many-clicks?]]))

(defn anomalous-users-for-console
  [users]
  (v-> users
       [:to-stream]
       [:filter [:predicate value-nil?]]))

(def config
  {"application.id"      "anomaly-detection-lambda-example"
   "client.id"           "anomaly-detection-lambda-example-client"
   "bootstrap.servers"   "10.11.12.13:9092"
   "default.key.serde"   string-serde
   "default.value.serde" string-serde
   "commit.interval"     500})

(defn -main [& args]
  (let [clicks [:stream [:strs "UserClicks"]]
        builder (k/ksml* (-> (anomalous-users clicks)
                             (anomalous-users-for-console)))
        streams (k/streams builder config)]
    
    (doto streams
      (.cleanUp)
      (.start))

    (log/info "Started anomaly detection streams")

    (doto (Runtime/getRuntime)
      (.addShutdownHook (Thread. #(.close streams))))))
        

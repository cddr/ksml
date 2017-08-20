(defproject cddr/ksml "0.1.0-SNAPSHOT"
  :description "Kafka Streams Markup Language"
  :url "http://github.com/cddr/ksml"
  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"}
  :dependencies [[org.clojure/clojure "1.8.0"]
                 [org.apache.kafka/kafka-streams "0.11.0.0"]]
  :profiles
  {:dev
   {:dependencies [[org.apache.kafka/kafka-streams "0.11.0.0" :classifier "test"]]}})

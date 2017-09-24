(defproject cddr/ksml "0.1.0-SNAPSHOT"
  :description "Kafka Streams Markup Language"
  :url "http://github.com/cddr/ksml"
  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"}
  :dependencies [[org.clojure/clojure "1.8.0"]
                 [org.apache.kafka/kafka-streams "0.11.0.0"]]
  :profiles
  {:examples
   {:source-paths ["src" "examples"]
    :resource-paths ["resources" "examples/resources"]
    :dependencies [[bidi "2.1.2"]
                   [yada/lean "1.2.8"]
                   [spootnik/unilog "0.7.20"]]}})


(set-env!
 :resource-paths #{"src" "test"}
 :dependencies '[[org.clojure/clojure "1.9.0-beta2"]
                 [org.apache.kafka/kafka-streams "0.11.0.0"]
                 [metosin/boot-alt-test "0.3.2" :scope "test"]
                 [org.slf4j/slf4j-nop "1.7.25" :scope "test"]])

(require
 '[metosin.boot-alt-test :refer [alt-test]])

(task-options!
 pom {:project      'cddr/ksml
      :version      (System/getProperty "ksml-version")
      :description  "Library for representing kafka streams topologies as data"
      :url          "https://github.com/cddr/ksml"
      :scm          {:url "https://github.com/cddr/ksml"}
      :license      {"Eclipse Public License" "http://www.eclipse.org/legal/epl-v10.html"}})


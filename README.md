# ksml

Ksml is a library for representing kafka streams topologies as data

## Overview

Kafka Streams is a client library for building mission-critical
real-time applications and microservices, where the input and/or
output data is stored in Kafka clusters. Kafka Streams combines the
simplicity of writing and deploying standard Java and Scala
applications on the client side with the benefits of Kafka's
server-side cluster technology to make these applications highly
scalable, elastic, fault-tolerant, distributed, and much more.

A stream processing application is any program that makes use of the
Kafka Streams library. It defines its computational logic through one
or more processor topologies, where a processor topology is a graph of
stream processors (nodes) that are connected by streams (edges).

KSML is a library for representing these topologies as data. It
uses vectors to represent distributed streaming primitives like map,
join, filter etc, and plain old Clojure functions for representing the
processing logic to be performed by each node.

## Rationale

The clojure community generally places a high value on data. Expressing
the computation as data provides a number of advantages

 * it becomes easy to compose topology fragments using only the
   standard collection manipulation tools like map, concat, merge etc

 * you can report on it, analyze it, graph it etc, limited
   only by your imagination

 * you can transform it for the purposes of instrumentation to produce
   standardized metrics, logging, and exception handling

## Usage

watch out. code has just been heavily refactored and still need to review
the examples below and make more extensive docs

```clojure
(ns com.ksml.wordcount
  (:require
    [clojure.string :as str]
    [cddr.ksml.core :refer [ksml* v->]])
  (:import
    (org.apache.kafka.streams.kstream KStreamBuilder)
    (org.apache.kafka.streams KafkaStreams)))

(defn- split-line
  [line]
  (-> line
      (str/lower-case)
      (str/split #"\\W+")))

(defn wordcount
  [lines]
  (v-> lines
       [:flat-map-values [:value-mapper split-line]]
       [:group-by
         [:key-value-mapper (fn [k word] word)]]
       [:count "Counts"]
       [:to! [:serde 'String]
             [:serde 'Long]
             "WordsWithCountsTopic"]))

(defn -main [& args]
  (let [lines [:stream "TextLinesTopic"]]
    (doto (KafkaStreams. (ksml* (wordcount lines)))
      (.start))))
```

## Bugs

While there is a decent [testsuite](https://github.com/cddr/ksml/blob/master/test/cddr/ksml/eval_test.clj)
this library is still in the early stages. I might need to tweak the
data structures as we use it to build programs and discover more
optimal ways of describing topologies.

Credit to @ztellman for the idea and hiccup, and SICP for implementation
ideas.

## License

Copyright © 2017 Andy Chambers

Distributed under the Eclipse Public License either version 1.0 or (at
your option) any later version.

(ns cddr.ksml.core
  (:require
   [cddr.ksml.eval :as ksml.eval])
  (:import
   (org.apache.kafka.streams StreamsBuilder KafkaStreams StreamsConfig)
   (org.apache.kafka.streams.kstream Predicate)))

(defn builder []
  (StreamsBuilder.))

(defn kafka-config
  [config]
  (if (instance? StreamsConfig config)
    config
    (StreamsConfig. config)))

;; v1
(defn ksml*
  [expr]
  (eval
   `(binding [ksml.eval/*builder* (builder)]
      ~(ksml.eval/eval expr)
      ksml.eval/*builder*)))

;; v2
;; (defn ksml*
;;   [expr]
;;   (eval
;;    `(binding [*builder* (builder)]
;;       (.. *builder* ~expr)
;;       *builder*)))

(defmacro ksml
  [expr]
  `(binding [ksml.eval/*builder* (StreamsBuilder.)]
     ~(ksml.eval/eval expr)
     ksml.eval/*builder*))

(defn streams
  [builder config]
  (KafkaStreams. builder (kafka-config config)))

(defmacro v->
  "Like Clojure's `->` but expects the 'forms' to be vectors"
  [x & forms]
  (loop [x x, forms forms]
    (if forms
      (let [form (first forms)
            threaded (if (vector? form)
                       `[~(first form) ~x ~@(next form)]
                       (vector form x))]
        (recur threaded (next forms)))
      x)))

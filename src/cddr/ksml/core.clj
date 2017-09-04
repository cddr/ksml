(ns cddr.ksml.core
  (:require
   [cddr.ksml.eval :as ksml.eval])
  (:import
   (org.apache.kafka.streams.kstream KStreamBuilder)))

(defn ksml*
  [expr]
  (eval
   `(binding [ksml.eval/*builder* (KStreamBuilder.)]
      ~(ksml.eval/eval expr)
      ksml.eval/*builder*)))

(defmacro ksml
  [expr]
  `(binding [ksml.eval/*builder* (KStreamBuilder.)]
     ~(ksml.eval/eval ~expr)))

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

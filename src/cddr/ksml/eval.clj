(ns cddr.ksml.eval
  "ksml is a clojure/data based representation of a kafka streams application.

   This namespace defines the evaluator for ksml. At a high-level though, there
   are only a few types of expressions in a ksml program

    * self-evaluating things like e.g.
      - numbers, strings, regexes
      - clojure functions

    * special operators (the kafka streams DSL)
      - :stream, :table, :branch, :map etc

   For each special operator, the code in here knows how to convert that operator
   (together with it's optional args) into the underlying java interop."
  (:refer-clojure :exclude [eval reducer])
  (:require
   [clojure.string :as str])
  (:import
   (org.apache.kafka.common.serialization Serdes)
   (org.apache.kafka.streams KeyValue)
   (org.apache.kafka.streams.state Stores)
   (org.apache.kafka.streams.processor Processor TimestampExtractor ProcessorSupplier
                                       StreamPartitioner
                                       TopologyBuilder$AutoOffsetReset
                                       FailOnInvalidTimestamp, LogAndSkipOnInvalidTimestamp,
                                       UsePreviousTimeOnInvalidTimestamp, WallclockTimestampExtractor)
   (org.apache.kafka.streams.kstream KStream KStreamBuilder JoinWindows TimeWindows
                                     Predicate
                                     Initializer Aggregator Reducer
                                     ForeachAction
                                     ValueMapper KeyValueMapper
                                     ValueJoiner
                                     Transformer TransformerSupplier)))

(declare eval)

(def ^:dynamic *builder*)

(defmacro defsyntax [name rules]
  `(do
     (def ~name ~rules)
     (defn ~(symbol (str name "?")) [expr#]
       (when (vector? expr#)
         (contains? ~name (first expr#))))))

;; primitive ops

(defsyntax primitive
  {
   ;; builders
   :stream (fn [args]
             `(.. *builder* (stream ~@args)))
   :table (fn [args]
            `(.. *builder* (table ~@args)))
   :global-table (fn [args]
                   `(.. *builder* (globalTable ~@args)))
   :merge (fn [args]
            `(.. *builder* (merge (into-array KStream
                                              (vector ~@args)))))

   ;; a few places expect vararg Strings so we make it easier to make those
   ;; with this
   :strs (fn [args]
           `(into-array String (vector ~@args)))

   ;; other miscellaneous streaming primitives

   :offset-reset (fn [args]
                   `(. org.apache.kafka.streams.processor.TopologyBuilder$AutoOffsetReset
                       ~(first args)))

   :timestamp-extractor (fn [args]
                          `(new ~(first args)))

   :store (fn [[store-name store-options]]
            `(-> (.. org.apache.kafka.streams.state.Stores
                     (create ~store-name))
                 ~(let [k# (:with-keys store-options)]
                    `(.withKeys ~(eval k#)))
                 ~(let [v# (:with-values store-options)]
                    `(.withValues ~(eval v#)))
                 ~(let [factory# (:factory store-options)]
                    `(~factory#))
                 ~(when (:logging-disabled? store-options)
                    `(.disableLogging))
                 (.build)))
   })

(defmulti find-serde identity)

(defsyntax serde
  {
   :serde (fn
            ([id]
             (if (keyword? id)
               `(find-serde id)
               `(.. Serdes ~id)))
            ([serializer deserializer]
             `(.. Serdes (serdeFrom ~serializer ~deserializer))))
   })

(defn key-value
  [k v]
  (KeyValue. k v))

(defsyntax lambda
  {:predicate            (fn [pred-fn]
                           (reify Predicate
                             (test [_ k v]
                               (boolean (pred-fn k v)))))

   :key-value-mapper     (fn [map-fn]
                           (reify KeyValueMapper
                             (apply [_ k v]
                               (apply key-value (map-fn k v)))))

   :value-mapper         (fn [vmap-fn]
                           (reify ValueMapper
                             (apply [_ v]
                               (vmap-fn v))))

   :value-joiner         (fn [join-fn]
                           (reify ValueJoiner
                             (apply [_ left right]
                               (join-fn left right))))

   :foreach-action       (fn [each-fn]
                           (reify ForeachAction
                             (apply [_ k v]
                               (each-fn k v))))

   :initializer          (fn [init-fn]
                           (reify Initializer
                             (apply [_]
                               (init-fn))))

   :aggregator           (fn [agg-fn]
                           (reify Aggregator
                             (apply [_ k v agg]
                               (agg-fn agg [k v]))))

   :reducer              (fn [reduce-fn]
                           (reify Reducer
                             (apply [_ v1 v2]
                               (reduce-fn v1 v2))))

   :partitioner          (fn [partition-fn]
                           (reify StreamPartitioner
                             (partition [this k v i]
                               (partition-fn k v i))))

   :processor-supplier   (fn processor-supplier
                           ([process-fn]
                            (processor-supplier process-fn (constantly nil)))
                           ([process-fn punctuate-fn]
                            (reify ProcessorSupplier
                              (get [_]
                                (let [ctx (atom nil)]
                                  (reify Processor
                                    (init [_ context]
                                      (reset! ctx context))
                                    (punctuate [_ ts]
                                      (punctuate-fn @ctx ts))
                                    (process [_ k v]
                                      (process-fn @ctx k v))))))))

   :transformer-supplier (fn transformer-supplier
                           ([transform-fn]
                            (transformer-supplier transform-fn (constantly nil)))
                           ([transform-fn punctuate-fn]
                            (reify TransformerSupplier
                              (get [_]
                                (let [ctx (atom nil)]
                                  (reify Transformer
                                    (init [_ context]
                                      (reset! ctx context))
                                    (punctuate [_ ts]
                                      (punctuate-fn @ctx ts))
                                    (transform [_ k v]
                                      (transform-fn @ctx k v))))))))
   })

(defsyntax mapping-operator
  {
   :filter          (fn [stream predicate-fn & args]
                      `(.. ~stream
                           (filter ~predicate-fn ~@args)))

   :filter-not      (fn [stream predicate-fn & args]
                      `(.. ~stream
                           (filterNot ~predicate-fn ~@args)))

   :flat-map        (fn [stream map-fn & args]
                      `(.. ~stream
                           (flatMap ~map-fn)))

   :flat-map-values (fn [stream map-fn & args]
                      `(.. ~stream
                           (flatMapValues ~map-fn)))

   :foreach         (fn [stream each-fn & args]
                      `(.. ~stream
                           (foreach ~each-fn)))


   :map             (fn [stream map-fn & args]
                      `(.. ~stream
                           (map ~map-fn)))

   :map-values      (fn [stream map-fn & args]
                      `(.. ~stream
                           (mapValues ~map-fn)))

   :select-key      (fn [stream map-fn & args]
                      `(.. ~stream
                           (selectKey ~map-fn)))
   })

(defsyntax stream-operator
  {:branch    (fn [stream & predicates]
                `(.. ~stream
                     (branch (into-array Predicate
                                         (vector ~@predicates)))))


   :to-stream (fn [table & args]
                `(.. ~table
                     (toStream ~@args)))

   })

(defsyntax global-join
  {:join-global      (fn [left right map-fn join-fn & args]
                       `(.. ~left
                            (join ~right
                                  ~map-fn
                                  ~join-fn)))
   :left-join-global (fn [left right map-fn join-fn & args]
                       `(.. ~left
                            (leftJoin ~right
                                      ~map-fn
                                      ~join-fn)))
   })

(defsyntax join

  {:join       (fn [left right join-fn & args]
                 `(.. ~left
                      (join ~right
                            ~join-fn
                            ~@args)))



   :left-join  (fn [left right join-fn & args]
                 `(.. ~left
                      (leftJoin ~right
                                ~join-fn
                                ~@args)))



   :outer-join (fn [left right join-fn & args]
                 `(.. ~left
                      (outerJoin ~right
                                 ~join-fn
                                 ~@args)))
   })

(defsyntax side-effect
  {
   :peek!      (fn [stream each-fn]
                 `(.. ~stream
                      (peek ~each-fn)))

   :print!     (fn [stream & args]
                 `(.. ~stream
                      (print ~@args)))

   :process!   (fn [stream & args]
                 `(.. ~stream
                      (process ~@args)))

   :through!   (fn [stream & args]
                 `(.. ~stream
                      (through ~@args)))

   :to!        (fn [stream & args]
                 `(.. ~stream
                      (to ~@args)))

   :transform! (fn [stream & args]
                 `(.. ~stream
                      (transform ~@args)))
   })


(defsyntax aggregation
  {
   :group-by     (fn [stream group-fn & optional]
                   `(.. ~stream
                        (groupBy ~group-fn ~@optional)))

   :group-by-key (fn [stream & optional]
                   `(.. ~stream
                        (groupByKey ~@optional)))


   :aggregate    (fn [stream & args]
                   `(.. ~stream
                        (aggregate ~@args)))
   :count        (fn [stream & args]
                   `(.. ~stream
                        (count ~@args)))
   :reduce       (fn [stream & args]
                   `(.. ~stream
                        (reduce ~@args)))
   })


(defsyntax window
  {
   :join-window (fn [diff-ms & args]
                  `(.. (.. JoinWindows (of ~(long diff-ms)))
                       (until ~(long (inc (* diff-ms 2))))))
   :time-window (fn [ms & args]
                  `(.. TimeWindows (of ~(long ms))))
   })

;; Super simple evaluator. In ksml, there are only 4 types of
;; expressions
;;
;;   * self-evaluating (strings, numbers, functions)
;;   * primitives

(defn self-evaluating?
  [expr]
  (or
   (symbol? expr)
   (string? expr)
   (class? expr)
   (map? expr)
   (instance? java.util.regex.Pattern expr)
   (number? expr)))

(defn values
  [exprs]
  (map eval exprs))

(defsyntax application
  (merge mapping-operator
         stream-operator
         aggregation
         side-effect
         join
         global-join
         serde
         window))

(defn eval
  [expr]
  (cond
    (self-evaluating? expr)       expr
    (primitive? expr)             (let [[op & args] expr]
                                    ((primitive op) (values args)))

    (lambda? expr)                (let [[op function] expr]
                                    `((lambda ~op) ~function))

    (application? expr)           (let [[op & args] expr]
                                    (apply (application op)
                                           (values args)))

    :else (throw (ex-info "unknown expression: " {:expr expr}))))

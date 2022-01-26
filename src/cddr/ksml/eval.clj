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
   [clojure.string :as str]
   [clojure.spec.alpha :as s])
  (:import
   (java.util.regex Pattern)
   (java.time Duration)
   (org.apache.kafka.common.serialization Serializer Deserializer Serdes)
   (org.apache.kafka.streams KeyValue StreamsBuilder
                             Topology Topology$AutoOffsetReset)
   (org.apache.kafka.streams.state Stores)
   (org.apache.kafka.streams.processor Processor TimestampExtractor ProcessorSupplier
                                       StreamPartitioner
                                       FailOnInvalidTimestamp, LogAndSkipOnInvalidTimestamp,
                                       UsePartitionTimeOnInvalidTimestamp, WallclockTimestampExtractor)
   (org.apache.kafka.streams.kstream Named Grouped Consumed Materialized
                                     KStream JoinWindows TimeWindows
                                     Predicate
                                     Initializer Aggregator Reducer Merger
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

(defn camelize
  [in]
  (str/replace in #"-(\w)"
               #(str/upper-case (second %1))))

(defn build-op
  [op & args]
  `(.. *builder* (~op ~@args)))

(defn apply-args-to-op
  [op & args]
  `(~op ~@args))

(defn key-value
  [k v]
  (KeyValue. k v))

(def builders
  {
   ;; builders
   :stream build-op
   :table build-op
   :global-table build-op
   :merge (fn [op & args]
            `(.. *builder* (~op (into-array KStream
                                            (vector ~@args)))))
   :named (fn [op & args]
            `(Named/as ~(first args)))
   :strs (fn [op & args]
           `(into-array String (vector ~@args)))

   :topics (fn [op & args]
             `(vector ~@args))

   :duration (fn [op & args]
               `(Duration/parse ~(first args)))

   :offset-reset (fn [op & args]
                   `(Topology$AutoOffsetReset/valueOf
                     ~(first args)))

   :timestamp-extractor (fn [op & args]
                          `(new ~(first args)))})

(def mappers
  {
   :to-stream       (fn [op stream & args]
                      `(.. ~stream
                           (~op ~@args)))
   :branch          (fn [op stream & args]
                      (let [arg1 (first args)]
                        (if (= 'org.apache.kafka.streams.kstream.Named/as (first arg1))
                          `(.. ~stream
                               (~op ~arg1 (into-array Predicate (vector ~@(rest args)))))
                          `(.. ~stream
                               (~op (into-array Predicate (vector ~@args)))))))
   :filter          (fn [op stream predicate-fn & args]
                      `(.. ~stream
                           (~op ~predicate-fn ~@args)))
   :filter-not      (fn [op stream predicate-fn & args]
                      `(.. ~stream
                           (~op ~predicate-fn ~@args)))
   :flat-map        (fn [op stream map-fn & args]
                      `(.. ~stream
                           (~op ~map-fn)))
   :flat-map-values (fn [op stream map-fn & args]
                      `(.. ~stream
                           (~op ~map-fn)))
   :foreach         (fn [op stream each-fn & args]
                      `(.. ~stream
                           (~op ~each-fn)))
   :map             (fn [op stream map-fn & args]
                      `(.. ~stream
                           (~op ~map-fn)))
   :map-values      (fn [op stream map-fn & args]
                      `(.. ~stream
                           (~op ~map-fn)))
   :select-key      (fn [op stream key-fn & args]
                      `(.. ~stream
                           (~op ~key-fn)))})

(def joiners
  {:join       (fn [op left right join-fn & args]
                 `(.. ~left
                      (~op ~right ~join-fn ~@args)))

   :left-join  (fn [op left right join-fn & args]
                 `(.. ~left
                      (~op ~right ~join-fn ~@args)))

   :outer-join (fn [op left right join-fn & args]
                 `(.. ~left
                      (~op ~right ~join-fn ~@args)))
   })

(def aggregators
  {
   :group-by     (fn [op stream group-fn & optional]
                   `(.. ~stream
                        (~op ~group-fn ~@optional)))

   :group-by-key (fn [op stream & optional]
                   `(.. ~stream
                        (~op ~@optional)))


   :aggregate    (fn [op stream & args]
                   `(.. ~stream
                        (~op ~@args)))
   :count        (fn [op stream & args]
                   `(.. ~stream
                        (~op ~@args)))
   :reduce       (fn [op stream & args]
                   `(.. ~stream
                        (~op ~@args)))
   })

(def io-syntax
  {:materialized (fn [op init & modifiers]
                   (let [m `(. Materialized ~init)]
                     (if (empty? modifiers)
                       m
                       `(.. ~m ~@modifiers))))

   ;; consumed
   :consumed (fn [op init & modifiers]
               (let [c `(. Consumed ~init)]
                 (if (empty? modifiers)
                   c
                   `(.. ~c ~@modifiers))))
   :grouped (fn [op init & modifiers]
              (let [g `(. Grouped ~init)]
                (if (empty? modifiers)
                  g
                  `(.. ~g ~@modifiers))))

   :repartitioned (fn [op init & modifiers]
                    (let [r `(. Repartitioned ~init)]
                      (if (empty? modifiers)
                        r
                        `(.. ~r ~@modifiers))))

   :with apply-args-to-op
   :as apply-args-to-op
   :numberOfPartitions apply-args-to-op
   :streamPartitioner apply-args-to-op
   :withNumberOfPartitions apply-args-to-op
   :withStreamPartitioner apply-args-to-op
   :withKeySerde apply-args-to-op
   :withValueSerde apply-args-to-op
   :withName apply-args-to-op
   :withTimestampExtractor apply-args-to-op
   :withOffsetResetPolicy apply-args-to-op
   :withCachingDisabled apply-args-to-op
   :withCachingEnabled apply-args-to-op
   :withLoggingDisabled apply-args-to-op
   :withLoggingEnabled apply-args-to-op
   :withRetention apply-args-to-op})

(def serde-syntax
  {:serde (fn [op & args]
            `(.. Serdes ~@args))
   :serde-from (fn [op & args]
                 `(~op ~@args))})

(def store-syntax
  {:stores (fn [op & args]
             `(.. Stores ~@args))
   :inMemoryKeyValueStore apply-args-to-op
   :inMemorySessionStore apply-args-to-op
   :inMemoryWindowStore apply-args-to-op
   :lruMap apply-args-to-op
   :persistentKeyValueStore apply-args-to-op
   :persistentSessionStore apply-args-to-op
   :persistentTimestampedKeyValueStore apply-args-to-op
   :persistentWindowStore apply-args-to-op})

;; ksml syntax rules

(defsyntax application
  (merge builders
         mappers
         joiners
         aggregators
         io-syntax
         serde-syntax
         store-syntax))

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

   :merger               (fn [merge-fn]
                           (reify Merger
                             (apply [_ k agg1 agg2]
                               (merge-fn k agg1 agg2))))

   :reducer              (fn [reduce-fn]
                           (reify Reducer
                             (apply [_ v1 v2]
                               (reduce-fn v1 v2))))

   :partitioner          (fn [partition-fn]
                           (reify StreamPartitioner
                             (partition [this topic k v i]
                               (partition-fn topic k v i))))

   :serializer           (fn serializer
                           [ser-fn]
                           (reify Serializer
                             (close [_])
                             (configure [this cfg is-key?])
                             (serialize [this topic headers data]
                               (ser-fn topic headers data))
                             (serialize [this topic data]
                               (ser-fn topic data))))

   :deserializer         (fn deserializer
                           [deser-fn]
                           (reify Deserializer
                             (close [_])
                             (configure [this cfg is-key?])
                             (deserialize [this topic headers data]
                               (deser-fn topic headers data))
                             (deserialize [this topic data]
                               (deser-fn topic data))))

   :processor-supplier   (fn processor-supplier
                           ([process-fn]
                            (processor-supplier process-fn (constantly nil)))
                           ([process-fn init-fn]
                            (reify ProcessorSupplier
                              (get [_]
                                (let [ctx (atom nil)]
                                  (reify Processor
                                    (init [_ context]
                                      (reset! ctx context)
                                      (when init-fn
                                        (init-fn ctx)))
                                    (process [_ k v]
                                      (process-fn @ctx k v))))))))

   :transformer-supplier (fn transformer-supplier
                           ([transform-fn]
                            (transformer-supplier transform-fn nil))
                           ([transform-fn init-fn]
                            (reify TransformerSupplier
                              (get [_]
                                (let [ctx (atom nil)]
                                  (reify Transformer
                                    (init [_ context]
                                      (reset! ctx context)
                                      (when init-fn
                                        (init-fn ctx)))
                                    (transform [_ k v]
                                      (transform-fn @ctx k v))))))))
   })

;; Super simple evaluator. In ksml, there are only a few types of
;; expression
;;
;;   * self-evaluating
;;     e.g. strings, numbers, functions, raw clojure code
;;
;;   * lambdas
;;     Tend to take a function argument and evaluate to an instance
;;     satisfying a Kafka Streams interface like Predicate, or KeyValueMapper.
;;     When evaluating lambda expressions, we skip evaluating the
;;     the provided function and instead wrap it in the corresponding Kafka
;;     Streams object.
;;
;;   * application
;;     Application expressions consist of an operator (e.g. :stream, :filter,
;;     :branch etc). The evaluator evaluates the arguments and passes the
;;     evaluated expressions to the operator specific function for expansion
;;     into code that can be evaluated by standard Clojure `eval`.

(defn clojure?
  "In ksml, a `clojure?` expression is a list whose first element
  is a symbol. Expressions like this will be passed through the
  evaluator as-is"
  [expr]
  (symbol? (first expr)))

(defn self-evaluating?
  [expr]
  (or
   (string? expr)
   (class? expr)
   (map? expr)
   (instance? Pattern expr)
   (number? expr)
   (and (list? expr)
        (symbol? (first expr)))))

(defn values
  [exprs]
  (map eval exprs))

(defn eval
  [expr]
  (cond
    (self-evaluating? expr)       expr
    (lambda? expr)                (let [[op & functions] expr]
                                    `(apply (lambda ~op)
                                            ~(first functions)
                                            ~(rest functions)))
    (application? expr)           (let [[op & args] expr]
                                    (apply (application op)
                                           (-> op name camelize symbol)
                                           (values args)))

    :else (throw (ex-info "unknown expression: " {:expr expr}))))

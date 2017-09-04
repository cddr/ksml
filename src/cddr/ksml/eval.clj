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
   (org.apache.kafka.streams.kstream
    KStream
    KStreamBuilder
    JoinWindows
    Predicate
    Initializer Aggregator Reducer
    ForeachAction
    ValueMapper KeyValueMapper
    ValueJoiner
    Transformer TransformerSupplier)))

(defn dispatcher [op & args]
  op)

(defmulti eval-op dispatcher)

(def ^:dynamic *builder*)

(defn key-value
  [k v]
  (KeyValue. k v))

(defn self-evaluating?
  [expr]
  (or
   (symbol? expr)
   (string? expr)
   (class? expr)
   (map? expr)
   (instance? java.util.regex.Pattern expr)
   (number? expr)))

(defn ksml?
  [expr]
  (and (coll? expr)
       (keyword? (first expr))))

(declare values)

(defn eval
  [expr]
  (cond
    (self-evaluating? expr) expr
    (ksml? expr)
    (eval-op (first expr)
             (values (rest expr)))
    (ifn? expr) expr
    :else (throw (ex-info "unknown expression: " {:expr expr}))))

(defn values
  [exprs]
  (map eval exprs))

;; primitive ops

(defmethod eval-op :strs
  [_ args]
  `(into-array String (vector ~@args)))

;; builder ;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;

(defmethod eval-op :offset-reset
  [_ args]
  `(. org.apache.kafka.streams.processor.TopologyBuilder$AutoOffsetReset
      ~(first args)))

(defmethod eval-op :timestamp-extractor
  [_ args]
  `(new ~(first args)))

(defmethod eval-op :store
  [_ args]
  (let [store-name (first args)
        store-options (first (rest args))]
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
         (.build))))

(defmethod eval-op :stream
  [_ args]
  `(.. *builder* (stream ~@args)))

(defmethod eval-op :table
  [_ args]
  `(.. *builder* (table ~@args)))

(defmethod eval-op :global-table
  [_ args]
  `(.. *builder* (globalTable ~@args)))

(defmethod eval-op :merge
  [_ args]
  `(.. *builder* (merge (into-array org.apache.kafka.streams.kstream.KStream
                                    (vector ~@args)))))

;; serdes ;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;

(defmulti serde identity)

(defmethod eval-op :serde
  ([_ id]
   (if (keyword? id)
     `(serde id)
     `(.. Serdes ~id)))
  ([_ serializer deserializer]
   `(.. Serdes (serdeFrom ~serializer ~deserializer))))

;; lambdas ;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;

(defn predicate
  [pred-fn]
  (reify Predicate
    (test [_ k v]
      (boolean (pred-fn k v)))))

(defmethod eval-op :predicate
  [_ args]
  (let [[map-fn] args]
    `(predicate ~map-fn)))

(defn key-value-mapper
  [map-fn]
  (reify KeyValueMapper
    (apply [_ k v]
      (apply key-value (map-fn k v)))))

(defmethod eval-op :key-value-mapper
  [_ args]
  (let [[map-fn] args]
    `(key-value-mapper ~map-fn)))

(defn value-mapper
  [map-fn]
  (reify ValueMapper
    (apply [_ v]
      (map-fn v))))

(defmethod eval-op :value-mapper
  [_ args]
  (let [[map-fn] args]
    `(value-mapper ~map-fn)))

(defn value-joiner
  [join-fn]
  (reify ValueJoiner
    (apply [_ left right]
      (join-fn left right))))

(defmethod eval-op :value-joiner
  [_ args]
  (let [[join-fn] args]
    `(value-joiner ~join-fn)))

(defn foreach-action
  [each-fn]
  (reify ForeachAction
     (apply [_ k v]
       (each-fn k v))))

(defmethod eval-op :foreach-action!
  [_ args]
  (let [[each-fn] args]
    `(foreach-action ~each-fn)))

(defn initializer
  [init-fn]
  (reify Initializer
    (apply [_]
      (init-fn))))

(defn aggregator
  [agg-fn]
  (reify Aggregator
    (apply [_ k v agg]
      (agg-fn agg [k v]))))

(defn reducer
  [reducer-fn]
  (reify Reducer
    (apply [_ v1 v2]
      (reducer-fn v1 v2))))

(defn processor-supplier
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

(defmethod eval-op :processor-supplier
  [_ args]
  (let [[process-fn punctuate-fn] args]
    (if punctuate-fn
      `(processor-supplier ~process-fn ~punctuate-fn)
      `(processor-supplier ~process-fn))))

(defn transformer-supplier
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

(defn partitioner
  [partition-fn]
  (reify StreamPartitioner
    (partition [this k v i]
      (partition-fn k v i))))

(defmethod eval-op :partitioner
  [_ args]
  (let [[partition-fn] args]
    `(partitioner ~partition-fn)))

;; kstream/ktable ;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
;; Lots of methods exist on both KStream and KTable classes. Clojure
;; doesn't care which one since it is just invoking the underlying java
;; method anyway.
;;
;; The argument order is not a direct mapping of the kafka streams API. In
;; many cases, this isn't really practical because the java methods are
;; heavily overloaded which is not something that Clojure supports. In
;; choosing the argument order for some operator, we must balance a few
;; (sometimes competing) concerns
;;
;;   * where there is a corresponding clojure function (e.g. map, filter etc),
;;     it should be use the same order
;;
;;   * when indented using standard clojure-modes, the indentation should
;;     "look right" (i.e. not shifted off to the right in weird places)
;;
;;   * I'd rather not get into the business of parsing an individual
;;     operator's arguments in order to decide how to pass them along to
;;     the upstream function. In most cases, I think we can use multiple
;;     arities to cover the main cases, and use optional arguments to
;;     cover the rest

(defmethod eval-op :branch
  [_ args]
  (let [[stream & predicate-fns] args]
  `(.. ~stream
       (branch (into-array Predicate
                           (vector ~@predicate-fns))))))

(defmethod eval-op :filter
  [_ args]
  (let [[predicate-fn stream-or-table & args] args]
    `(.. ~stream-or-table
         (filter ~predicate-fn ~@args))))

(defmethod eval-op :filter-not
  [_ args]
  (let [[predicate-fn stream-or-table & args] args]
    `(.. ~stream-or-table
         (filterNot ~predicate-fn ~@args))))

(defmethod eval-op :flat-map
  [_ args]
  (let [[map-fn stream-or-table] args]
    `(.. ~stream-or-table
         (flatMap ~map-fn))))

(defmethod eval-op :flat-map-values
  [_ args]
  (let [[map-fn stream-or-table] args]
    `(.. ~stream-or-table
         (flatMapValues ~map-fn))))

(defmethod eval-op :foreach
  [_ args]
  (let [[stream each-fn] args]
    `(.. ~stream
         (foreach ~each-fn))))

(defmethod eval-op :group-by
  [_ args]
  (let [[stream group-fn & optional] args]
    `(.. ~stream
         (groupBy ~group-fn ~@optional))))

(defmethod eval-op :group-by-key
  [_ args]
  (let [[stream & optional] args]
    `(.. ~stream
         (groupByKey ~@optional))))

(defmethod eval-op :join
  [_ args]
  (let [[left right join-fn & args] args]
    (assert left)
    (assert right)
    (assert join-fn "Misssing required argument")
    `(.. ~left
         (join ~right
               ~join-fn
               ~@args))))

(defmethod eval-op :join-global
  [_ args]
  (let [[left right map-fn join-fn] args]
    `(.. ~left
         (join ~right
               ~map-fn
               ~join-fn))))

(defmethod eval-op :left-join
  [_ args]
  (let [[left right join-fn & args] args]
    `(.. ~left
         (leftJoin ~right
                   ~join-fn
                   ~@args))))

(defmethod eval-op :left-join-global
  [_ args]
  (let [[left right map-fn join-fn] args]
    `(.. ~left
         (leftJoin ~right
                   ~map-fn
                   ~join-fn))))


(defmethod eval-op :map
  [_ args]
  (let [[map-fn stream] args]
    `(.. ~stream
         (map ~map-fn))))

(defmethod eval-op :map-values
  [_ args]
  (let [[map-fn stream] args]
    `(.. ~stream
         (mapValues ~map-fn))))

(defmethod eval-op :outer-join
  [_ args]
  (let [[left right join-fn & args] args]
    `(.. ~left
         (outerJoin ~right
                    ~join-fn
                    ~@args))))

(defmethod eval-op :peek
  [_ args]
  (let [[each-fn stream] args]
    `(.. ~stream
         (peek ~each-fn))))

(defmethod eval-op :print
  [_ args]
  (let [[stream & args] args]
    `(.. ~stream
         (print ~@args))))

(defmethod eval-op :process
  [_ args]
  (let [[stream & args] args]
    `(.. ~stream
         (process ~@args))))

(defmethod eval-op :select-key
  [_ args]
  (let [[stream map-fn & args] args]
    `(.. ~stream
         (selectKey ~map-fn))))

(defmethod eval-op :through
  [_ args]
  (let [[stream & args] args]
    `(.. ~stream
         (through ~@args))))

(defmethod eval-op :to!
  [_ args]
  (let [[stream & args] args]
    `(.. ~stream
         (to ~@args))))

(defmethod eval-op :to-stream
  [_ args]
  (let [[table & args] args]
    `(.. ~table
         (toStream ~@args))))

(defmethod eval-op :transform
  [_ args]
  (let [[stream & args] args]
    `(.. ~stream
         (transform ~@args))))

  ;; ([_ stream state-stores transform-fn]
  ;;  `(.. ~(eval stream)
  ;;       (transform `(transformer-supplier ~transform)
  ;;                  (into-array String state-stores))))

  ;; ([_ stream state-stores transform-fn punctuate-fn]
  ;;  `(.. ~(eval stream)
  ;;       (transform `(transformer-supplier ~transform-fn ~punctuate-fn)
  ;;                  (into-array String state-stores)))))

;; grouped streams/tables

(defmethod eval-op :aggregate
  [_ args]
  (let [[stream & args] args]
    `(.. ~stream
         (aggregate ~@args))))

(defmethod eval-op :count
  [_ args]
  (let [[stream & args] args]
    `(.. ~stream
         (count ~@args))))

(defmethod eval-op :reduce
  [_ args]
  (let [[stream & args] args]
    `(.. ~stream
         (reduce ~@args))))

;; windows

;; TODO: this just assumes you want a standard window with until set to
;;       `(inc (* diff-ms 2))` but we should also add support for
;;       before/until/after
(defmethod eval-op :join-window
  [_ args]
  (let [[diff-ms & args] args]
    `(.. (.. JoinWindows (of ~diff-ms))
         (until ~(inc (* diff-ms 2))))))

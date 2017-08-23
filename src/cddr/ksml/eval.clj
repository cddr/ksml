(ns cddr.ksml.eval
  ""
  (:refer-clojure :exclude [eval reducer])
  (:require
   [clojure.string :as str])
  (:import
   (org.apache.kafka.common.serialization Serdes)
   (org.apache.kafka.streams KeyValue)
   (org.apache.kafka.streams.processor Processor ProcessorSupplier)
   (org.apache.kafka.streams.kstream
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

(defn eval
  [expr]
  (if (coll? expr)
    (apply eval-op (first expr) (rest expr))
    expr))

;; builder ;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;

(defmethod eval-op :stream
  [_ & args]
  `(.. *builder* (stream ~@(map eval args))))

(defmethod eval-op :table
  [_ & args]
  `(.. *builder* (table ~@(map eval args))))

(defmethod eval-op :global-table
  [_ & args]
  `(.. *builder* (globalTable ~@(map eval args))))

(defmethod eval-op :merge
  [_ & args]
  `(.. *builder* (merge (into-array KStream (vector ~@(map eval args))))))

;; serdes ;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;

(defmethod eval-op :serde
  ([_ id]
   `(.. Serdes (~id)))
  ([_ serializer deserializer]
   `(.. Serdes (serdeFrom ~serializer ~deserializer))))

;; lambdas ;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;

(defn predicate
  [pred-fn]
  (reify Predicate
    (test [_ k v]
      (boolean (pred-fn k v)))))

(defn key-value-mapper
  [map-fn]
  (reify KeyValueMapper
    (apply [_ k v]
      (apply key-value (map-fn k v)))))

(defn value-mapper
  [map-fn]
  (reify ValueMapper
    (apply [_ v]
      (map-fn v))))

(defn value-joiner
  [join-fn]
  (reify ValueJoiner
    (apply [_ left right]
      (join-fn left right))))

(defn foreach-action
  [each-fn]
  (reify ForeachAction
    (apply [_ k v]
      (each-fn k v))))

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
  [_ stream & predicate-fns]
  `(.. ~(eval stream)
       (branch (into-array Predicate
                           (vector ~@(for [p-fn# predicate-fns]
                                       `(predicate p-fn#)))))))

(defmethod eval-op :filter
  [_ predicate-fn stream-or-table]
  `(.. ~(eval stream-or-table)
       (filter (predicate predicate-fn))))

(defmethod eval-op :filter-not
  [_ predicate-fn stream-or-table]
  `(.. ~(eval stream-or-table)
       (filterNot (predicate predicate-fn))))

(defmethod eval-op :flat-map
  [_ map-fn stream-or-table]
  `(.. ~(eval stream-or-table)
       (flatMap (key-value-mapper map-fn))))

(defmethod eval-op :flat-map-values
  [_ map-fn stream-or-table]
  `(.. ~(eval stream-or-table)
       (flatMapValues (value-mapper ~map-fn))))

(defmethod eval-op :foreach
  [_ stream each-fn]
  `(.. ~(eval stream)
       (foreach (foreach-action ~each-fn))))

(defmethod eval-op :group-by
  [_ stream group-fn & args]
  `(.. ~(eval stream)
       ~(remove nil? (apply list 'groupBy
                            `(key-value-map-fn ~group-fn)
                            (map eval args)))))

(defmethod eval-op :group-by-key
  [_ stream & args]
  `(.. ~(eval stream)
       ~(remove nil? (apply list 'groupByKey
                            (map eval args)))))

(defmethod eval-op :join
  [_ left right join-fn & args]
  `(.. ~(eval left)
       ~(remove nil? (apply list 'join
                            (eval right)
                            `(value-joiner ~join-fn)
                            (map eval args)))))

(defmethod eval-op :join-global
  [_ left right join-fn map-fn]
  `(.. ~(eval left)
       (join ~(eval right)
             `(key-value-map-fn ~map-fn)
             `(value-joiner ~join-fn))))

(defmethod eval-op :left-join
  [_ left right join-fn & args]
  `(.. ~(eval left)
       ~(remove nil? (apply list 'leftJoin
                            (eval right)
                            `(value-joiner ~join-fn)
                            (map eval args)))))

(defmethod eval-op :left-join-global
  [_ left right join-fn map-fn]
  `(.. ~(eval left)
       (join ~(eval right)
             `(key-value-mapper ~map-fn)
             `(value-joiner ~join-fn))))

(defmethod eval-op :map
  [_ map-fn stream]
  `(.. ~(eval stream)
       (map (key-value-mapper ~map-fn))))

(defmethod eval-op :map-values
  [_ map-fn stream]
  `(.. ~(eval stream)
       (mapValues (value-mapper ~map-fn))))

(defmethod eval-op :outer-join
  [_ left right join-fn & args]
  `(.. ~(eval left)
       ~(remove nil? (apply list 'outerJoin
                            (eval right)
                            `(value-joiner ~join-fn)
                            (map eval args)))))
(defmethod eval-op :peek
  [_ each-fn stream]
  `(.. ~(eval stream)
       (peek (foreach-action ~each-fn))))

(defmethod eval-op :print
  [_ stream & args]
  `(.. ~(eval stream)
       ~(remove nil? (apply list 'print
                            (map eval args)))))

(defmethod eval-op :process
  ([_ stream process-fn]
   `(.. ~(eval stream)
        (process (processor-supplier ~process-fn))))
  ([_ stream process-fn punctuate-fn]
   `(.. ~(eval stream)
        (process (processor-supplier ~process-fn ~punctuate-fn)))))
                         
(defmethod eval-op :select-key
  [_ stream map-fn]
  `(.. ~(eval stream)
       (selectKey (key-value-mapper ~map-fn))))

(defmethod eval-op :through
  [_ stream & args]
  `(.. ~(eval stream)
       ~(remove nil? (apply list
                            'through
                            (map eval args)))))

(defmethod eval-op :to!
  [_ stream & args]
  `(.. ~(eval stream)
       ~(remove nil? (apply list
                            'to
                            (map eval args)))))

(defmethod eval-op :to-kstream
  [_ table]
  `(.. ~(eval table)
       (toStream)))

(defmethod eval-op :transform
  ([_ stream state-stores transform-fn]
   `(.. ~(eval stream)
        (transform `(transformer-supplier ~transform)
                   (into-array String state-stores))))
  
  ([_ stream state-stores transform-fn punctuate-fn]
   `(.. ~(eval stream)
        (transform `(transformer-supplier ~transform-fn ~punctuate-fn)
                   (into-array String state-stores)))))

;; grouped streams/tables

(defmethod eval-op :aggregate
  [_ stream init-fn agg-fn & args]
  `(.. ~(eval stream)
       ~(remove nil? (apply list 'aggregate
                            `(initializer ~init-fn)
                            `(aggregator ~agg-fn)
                            (map eval args)))))
  
(defmethod eval-op :count
  [_ stream & args]
  `(.. ~(eval stream)
       ~(remove nil? (apply list
                            'count
                            (map 'eval args)))))

(defmethod eval-op :reduce
  [_ stream reducer-fn & args]
  `(.. ~(eval stream)
       ~(remove nil? (apply list 'reduce
                            `(reducer ~reducer-fn)
                            (map eval args)))))

;; windows

;; TODO: this just assumes you want a standard window with until set to
;;       `(inc (* diff-ms 2))` but we should also add support for
;;       before/until/after
(defmethod eval-op :join-window
  ([_ diff-ms]
   `(.. (.. JoinWindows (of ~diff-ms))
        (until ~(inc (* diff-ms 2))))))


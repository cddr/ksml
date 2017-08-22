(ns cddr.ksml.eval
  ""
  (:refer-clojure :exclude [eval])
  (:require
   [clojure.string :as str])
  (:import
   (org.apache.kafka.common.serialization Serdes)
   (org.apache.kafka.streams KeyValue)
   (org.apache.kafka.streams.processor Processor ProcessorSupplier)
   (org.apache.kafka.streams.kstream
    Predicate
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
  (apply eval-op (first expr) (rest expr)))

;; builder ;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;

(defmethod eval-op :stream
  [_ & args]
  `(.. *builder* (stream ~@args)))

(defmethod eval-op :table
  [_ & args]
  `(.. *builder* (table ~@args)))

(defmethod eval-op :global-table
  [_ & args]
  `(.. *builder* (globalTable ~@args)))

(defmethod eval-op :merge
  [_ & args]
  `(.. *builder* (merge (into-array KStream (vector ~@args)))))

;; serdes ;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;

(defmethod eval-op :serde
  ([_ id]
   (if (instance? Serde id)
     `(.. Serdes (~id))))
  ([_ serializer deserializer]
   `(.. Serdes (serdeFrom ~serializer ~deserializer))))

;; lamdas ;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
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
  [_ each-fn stream]
  `(.. ~(eval stream)
       (foreach (foreach-action ~each-fn))))

(defmethod eval-op :group-by
  [_ group-fn stream & args]
  `(.. ~(eval stream)
       ~(remove nil? (list 'groupBy
                           `(key-value-map-fn ~group-fn)
                           args))))

(defmethod eval-op :group-by-key
  [_ group-fn stream & args]
  `(.. ~(eval stream)
       ~(remove nil? (list 'groupByKey
                            `(key-value-map-fn ~group-fn)
                            args))))

(defmethod eval-op :join
  [_ join-fn left right & args]
  `(.. ~(eval left)
       ~(remove nil? (list 'join
                           (eval right)
                           `(value-joiner ~join-fn)
                           args))))

(defmethod eval-op :join-global
  [_ join-fn map-fn left right]
  `(.. ~(eval left)
       (join ~(eval right)
             `(key-value-map-fn ~map-fn)
             `(value-joiner ~join-fn))))

(defmethod eval-op :left-join
  [_ join-fn left right & args]
  `(.. ~(eval left)
       ~(remove nil? (apply list 'leftJoin
                            (eval right)
                            `(value-joiner ~join-fn)
                            args))))

(defmethod eval-op :left-join-global
  [_ join-fn map-fn left right]
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
  [_ join-fn left right & args]
  `(.. ~(eval left)
       ~(remove nil? (apply list 'outerJoin
                            (eval right)
                            `(value-joiner ~join-fn)
                            args))))

(defmethod eval-op :peek
  [_ each-fn stream]
  `(.. ~(eval stream)
       (peek (foreach-action ~each-fn))))

(defmethod eval-op :print
  [_ stream & args]
  `(.. ~(eval stream)
       ~(remove nil? (apply list 'print
                            args))))

(defmethod eval-op :process
  ([_ process-fn stream]
   `(.. ~(eval stream)
        (process (processor-supplier ~process-fn))))
  ([_ process-fn punctuate-fn stream]
   `(.. ~(eval stream)
        (process (processor-supplier ~process-fn ~punctuate-fn)))))
                         
(defmethod eval-op :select-key
  [_ map-fn stream]
  `(.. ~(eval stream)
       (selectKey (key-value-mapper ~map-fn))))

(defmethod eval-op :through
  [_ stream & args]
  `(.. ~(eval stream)
       ~(remove nil? (apply list 'through args))))

(defmethod eval-op :to
  [_ stream & args]
  `(.. ~(eval stream)
       ~(remove nil? (apply list 'to args))))

(defmethod eval-op :transform
  ([_ transform-fn stream state-stores]
   `(.. ~(eval stream)
        (transform `(transformer-supplier ~transform)
                   (into-array String state-stores))))
  
  ([_ transform-fn punctuate-fn stream state-stores]
   `(.. ~(eval stream)
        (transform `(transformer-supplier ~transform-fn ~punctuate-fn)
                   (into-array String state-stores)))))

(defmethod eval-op :to!
  [_ stream topic]
  `(.. ~(eval stream)
       (to ~topic)))


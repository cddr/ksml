(ns cddr.ksml.eval
  ""
  (:refer-clojure :exclude [eval])
  (:require
   [clojure.string :as str])
  (:import
   (org.apache.kafka.streams KeyValue)
   (org.apache.kafka.streams.kstream Predicate
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
       (foreach (reify ForeachAction
                  (apply [_ k# v#]
                    (~each-fn k# v#))))))

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
  [_ foreach-fn stream]
  `(.. ~(eval stream)
       (peek (reify ForeachAction
               (apply [_ k# v#]
                 (~foreach-fn k# v#))))))

(defmethod eval-op :print
  [_ stream & args]
  `(.. ~(eval stream)
       ~(remove nil? (apply list 'print
                            args))))

(defmethod eval-op :process
  ([_ process-fn stream]
   `(.. ~(eval stream)
        (process (reify ProcessorSupplier
                   (get [_]
                     (let [ctx# (atom nil)]
                       (reify Processor
                         (init [_ context#]
                           (reset! ctx# context#))
                         (process [_ k# v#]
                           (~process-fn k# v#)))))))))
  ([_ process-fn punctuate-fn stream]
   `(.. ~(eval stream)
        (process (reify ProcessorSupplier
                   (get [_]
                     (let [ctx# (atom nil)]
                       (reify Processor
                         (init [_ context#]
                           (reset! ctx# context#))
                         (punctuate [_ ts#]
                           (~punctuate-fn @ctx# ts#))
                         (process [_ k# v#]
                           (~process-fn @ctx# k# v#))))))))))
                         
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
        (transform `(reify TransformerSupplier
                      (get [this]
                        (let [ctx# (atom nil)]
                          (reify TransformerSupplier
                            (init [this context]
                              (reset! ctx# context))
                            (transform [this k v]
                              (~transform-fn @ctx# k v))))))
                   (into-array String state-stores))))
  
  ([_ transform-fn punctuate-fn stream state-stores]
   `(.. ~(eval stream)
        (transform `(reify TransformerSupplier
                      (get [this]
                        (let [ctx# (atom nil)]
                          (reify TransformerSupplier
                            (init [this context]
                              (reset! ctx# context))
                            (punctuate [this timestamp]
                              (~punctuate-fn @ctx# timestamp))
                            (transform [this k v]
                              (~transform-fn @ctx# k v))))))
                   (into-array String state-stores)))))

(defmethod eval-op :to!
  [_ stream topic]
  `(.. ~(eval stream)
       (to ~topic)))


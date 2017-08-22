(ns cddr.ksml.macros
  (:require
   [cddr.ksml.eval :as ksml.eval])
  (:import
   (org.apache.kafka.streams.kstream KStreamBuilder)))

(defmacro ksml
  [expr]
  `(binding [ksml.eval/*builder* (KStreamBuilder.)]
     ~(ksml.eval/eval expr)))

(comment

  ;; basic stream with default serdes
  (ksml [:stream #"foo.*"])
  
  ;; table/table join
  (ksml [:join (fn [left right]
                 {:left left
                  :right right})
         [:table "foo"]
         [:table "bar"]])

  ;; complex nested topology returning a pair of streams
  (ksml [:branch [:filter (fn [k v]
                            true)
                  [:map (fn [k v]
                          [k (.toUpperCase v)])
                   [:stream #"nonsense"]]]
         (fn [k v]
           (= (.startsWith "foo")))
         (fn [k v]
           (= (.startsWith "bar")))])

  (ksml [:serde ByteArray])
         
  )

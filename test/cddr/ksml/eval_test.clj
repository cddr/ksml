(ns cddr.ksml.eval-test
  (:require
   [clojure.test :refer :all]
   [cddr.ksml.eval :as k]
   [cddr.ksml.core :refer [ksml* v->]])
  (:import
   (java.io ByteArrayInputStream)
   (java.time Duration)
   (org.apache.kafka.common.serialization Serde Serdes Serializer Deserializer)
   (org.apache.kafka.streams StreamsBuilder)
   (org.apache.kafka.streams.kstream KStream Consumed)
   (org.apache.kafka.streams.processor FailOnInvalidTimestamp)))

(def topics
  [:strs "a" "b" "c"])

(defn builder
  []
  (StreamsBuilder.))

(defn keval
  [expr]
  (cddr.ksml.eval/eval expr))

(defn kpprint
  [expr]
  (clojure.pprint/pprint (keval expr)))



(def extractor
  {:fail FailOnInvalidTimestamp})

(def keySerde [:serde '(ByteArray)])
(def valSerde [:serde '(ByteArray)])
(def topicPattern #"p")
(def topic "foo")
(def topics ["foo" "bar"])
(def offset [:offset-reset "EARLIEST"])
(def join-window [:join-window 1000])
(def timestampExtractor [:timestamp-extractor (extractor :fail)])

(def state-store
  [:store "log" {:with-keys keySerde
                 :with-values valSerde
                 :factory '.inMemory
                 :logging-disabled? true}])
(def state-store-name "foo-store")
(def queryable-store-name "foo-store")
(def partitioner [:partitioner (fn [k v i]
                                 0)])
(def grouped-stream
  (v-> [:stream topicPattern]
       [:group-by-key]))

(defn consumes-pattern?
  [b]
  (= (str topicPattern)
     (str (.sourceTopicPattern b))))

(defn consumes-topics?
  [b]
  (let [topic-group (get (.topicGroups b) (int 0))]
    (= #{"foo" "bar"}
       (.sourceTopics topic-group))))

(defn serde?
  [expr]
  (instance? Serde (eval
                    (k/eval expr))))

(defn valid-ksml?
  [expr]
  (-> expr keval eval))

(deftest test-eval-serde
  (is (valid-ksml? [:serde '(Integer)]))
  (is (valid-ksml? [:serde
                    [:serde-from Integer]]))
  (is (valid-ksml? [:serde
                    [:serde-from [:serializer (fn [])]
                     [:deserializer (fn [])]]])))

(deftest test-eval-stream
  (testing "from pattern"
    (let [ksb (ksml* [:stream topicPattern])]
      (is (instance? StreamsBuilder ksb))))

  (testing "from pattern with serdes"
    (let [ksb (ksml* [:stream topicPattern
                      [:consumed [:with keySerde valSerde]]])]
      (is (instance? StreamsBuilder ksb))))

  (testing "from topics with serdes"
    (let [ksb (ksml* `[:stream [:topics ~@topics]
                       [:consumed [:with ~keySerde ~valSerde]]])]
      (is (instance? StreamsBuilder ksb))))

  (testing "from topics"
    (let [ksb (ksml* `[:stream [:topics ~@topics]])]
      (is (instance? StreamsBuilder ksb))))

  (testing "from pattern with serdes and timestamp extractor"
    (let [ksb (ksml* `[:stream ~topicPattern
                       [:consumed [:with ~keySerde ~valSerde]
                        [:withTimestampExtractor ~timestampExtractor]]])]
      (is (instance? StreamsBuilder ksb))))

  (testing "from topics with serdes and timestamp extractor"
    (let [ksb (ksml* `[:stream [:topics ~@topics]
                       [:consumed [:with ~keySerde ~valSerde]
                        [:withTimestampExtractor ~timestampExtractor]]])]
      (is (instance? StreamsBuilder ksb))))

  (testing "from pattern and offset"
    (is (ksml* `[:stream ~topicPattern
                 [:consumed [:with ~offset]]])))

  (testing "from pattern with serdes and offset"
    (is (ksml* `[:stream ~topicPattern
                 [:consumed [:with ~keySerde ~valSerde]
                  [:withOffsetResetPolicy ~offset]]])))

  (testing "from topics with serde and offset"
    (is (ksml* `[:stream [:topics ~@topics]
                 [:consumed [:with ~keySerde ~valSerde]
                  [:withOffsetResetPolicy ~offset]]])))

  (testing "from topics with offset"
    (is (ksml* `[:stream [:topics ~@topics]
                 [:consumed [:with ~offset]]])))

  (testing "from pattern with offset and timestamp-extractor and serdes"
    (is (ksml* `[:stream ~topicPattern
                 [:consumed
                  [:with ~keySerde ~valSerde ~timestampExtractor ~offset]]])))

  (testing "from topics with offset and timestamp-extractor and serdes"
    (is (ksml* `[:stream [:topics ~@topics]
                 [:consumed
                  [:with ~keySerde ~valSerde ~timestampExtractor ~offset]]]))))

(deftest test-eval-stores
  (is (valid-ksml? [:stores [:inMemoryKeyValueStore "yolo"]]))
  (is (valid-ksml? [:stores [:inMemorySessionStore "yolo"
                             [:duration "PT1H"]]])))

(deftest test-eval-materialized
  (is (valid-ksml? [:materialized [:as "foo"]
                    [:withCachingDisabled]]))
  (is (valid-ksml? [:materialized [:as "foo"]
                    [:withCachingEnabled]]))
  (is (valid-ksml? [:materialized [:as "foo"]
                    [:withLoggingDisabled]]))
  (is (valid-ksml? [:materialized [:as "foo"]
                    [:withLoggingEnabled {"foo" "bar"}]]))
  (is (valid-ksml? [:materialized [:as "foo"]
                    [:withRetention [:duration "PT24H"]]]))
  (is (valid-ksml? [:materialized [:with keySerde valSerde]])))

(deftest test-eval-table
  (testing "from topic with serdes"
    (is (ksml* `[:table ~topic
                 [:consumed
                  [:with ~keySerde ~valSerde]]])))

  (testing "from topic with serdes, and materialized"
    (is (ksml* `[:table ~topic
                 [:consumed
                  [:with ~keySerde ~valSerde]]
                 [:materialized [:with ~keySerde ~valSerde]]])))

  (testing "from topic with serdes and state-store name"
    (is (ksml* `[:table ~topic
                 [:consumed
                  [:with ~keySerde ~valSerde]]
                 [:materialized [:as "store-name"]]])))

  (testing "from topic"
    (is (ksml* [:table topic])))

  (testing "from topic with state store supplier"
    (is (ksml* [:table topic
                [:materialized [:with keySerde valSerde]]])))

  (testing "from topic with state-store name"
    (is (ksml* [:table topic
                 [:materialized [:as state-store-name]]])))

  (testing "from topic with timestamp extractor and serdes and state-store name"
    (is (ksml* `[:table ~topic
                   [:consumed [:with ~keySerde ~valSerde]
                    [:withTimestampExtractor ~timestampExtractor]]
                   [:materialized [:as ~state-store-name]]])))

  (testing "from topic with timestamp extractor and state-store name"
    (is (ksml* `[:table ~topic
                 [:consumed [:with ~timestampExtractor]]
                 [:materialized [:as ~state-store-name]]])))

  (testing "from topic with offset and serdes"
    (is (ksml* `[:table ~topic
                 [:consumed [:with ~keySerde ~valSerde]
                  [:withOffsetResetPolicy ~offset]]])))

  (testing "from topic with offset and state-store name"
    (is (ksml* `[:table ~topic
                 [:consumed [:with ~offset]]
                 [:materialized [:as ~state-store-name]]])))

  (testing "from topic with offset"
    (is (ksml* `[:table ~topic
                 [:consumed [:with ~offset]]])))

  (testing "from topic with offset and state-store supplier"
    (is (ksml* `[:table ~topic
                 [:consumed [:with ~offset]]
                 [:materialized
                  [:as [:stores [:inMemoryKeyValueStore "yolo"]]]]])))

  (testing "from topic with offset and timestamp extractor and serdes"
    (is (ksml* [:table topic
                [:consumed [:with keySerde valSerde]
                 [:withOffsetResetPolicy offset]
                 [:withTimestampExtractor timestampExtractor]]
                [:materialized
                 [:as [:stores [:inMemoryKeyValueStore "yolo"]]]]])))

  (testing "from topic with offset and timestamp extractor and serdes and state store-name"
    (is (ksml* [:table topic
                [:consumed [:with keySerde valSerde]
                 [:withOffsetResetPolicy offset]
                 [:withTimestampExtractor timestampExtractor]]
                [:materialized [:as state-store-name]]])))

  (testing "from topic with offset and timestamp extractor and state-store-name"
    (is (ksml* [:table topic
                [:consumed [:with offset]
                 [:withTimestampExtractor timestampExtractor]]
                [:materialized [:as state-store-name]]]))))

(deftest test-global-stream
  (testing "from topic with serdes"
    (is (ksml* [:global-table topic
                [:consumed [:with keySerde valSerde]]])))

  (testing "from topic with serdes and state store supplier"
    (is (ksml* [:global-table topic
                [:consumed [:with keySerde valSerde]]
                [:materialized
                 [:as [:stores [:inMemoryKeyValueStore "yolo"]]]]])))

  (testing "from topic with serde and queryableStoreName"
    (is (ksml* [:global-table topic
                [:consumed [:with keySerde valSerde]]
                [:materialized [:as queryable-store-name]]])))

  (testing "from topic with serde and timestamp extractor and queryable-store-name"
    (is (ksml* [:global-table topic
                [:consumed [:with keySerde valSerde]
                 [:withTimestampExtractor timestampExtractor]]
                [:materialized [:as queryable-store-name]]])))

  (testing "from topic"
    (is (ksml* [:global-table topic])))

  (testing "from topic with queryable-store-name"
    (is (ksml* [:global-table topic
                [:materialized [:as queryable-store-name]]]))))

(def allow-all [:predicate (fn [k v] true)])
(def allow-none [:predicate (fn [k v] false)])
(def kv-map [:key-value-mapper (fn [k v]
                                 [k v])])
(def vmap [:value-mapper (fn [v] v)])
(def side-effect! [:foreach-action (fn [k1 v1])])
(def xform [:transformer (fn [k v] [k v])])
(def group-fn [:key-value-mapper (fn [v]
                                   (:part-id v))])

;; (def edn-serde
;;   [:serde
;;    [:serializer (fn [this topic data]
;;                   (when data
;;                     (.getBytes (pr-str data))))]
;;    [:deserializer (fn [this topic data]
;;                      (when data
;;                        (clojure.edn/read (ByteArrayInputStream. data))))]])

(deftest test-ktable-ops
  (let [this-table [:table "left"]
        other-stream [:stream #"right"]
        other-global-table [:global-table keySerde valSerde "lookup"]
        other-table [:table "right"]
        join-fn [:value-joiner (fn [l r]
                                 (= (:id l) (:id r)))]]

    (testing "filter"
      (is (ksml* [:filter [:table topic] allow-all]))
      (is (ksml* [:filter [:table topic] allow-all
                  [:named "filtered-topic"]]))
      (is (ksml* [:filter [:table topic] allow-all
                  [:materialized
                   [:as [:stores [:inMemoryKeyValueStore "yolo"]]]]])))

    (testing "filter-not"
      (is (ksml* [:filter-not [:table topic] allow-none]))
      (is (ksml* [:filter-not [:table topic] allow-all
                  [:named "filtered-topic"]]))
      (is (ksml* [:filter-not [:table topic] allow-all
                  [:materialized
                   [:as [:stores [:inMemoryKeyValueStore "yolo"]]]]])))

    (testing "group-by"
      (is (ksml* [:group-by [:table topic]
                  kv-map])))

    (testing "group by, grouped"
      (is (ksml* [:group-by [:table topic] kv-map
                  [:grouped [:with keySerde valSerde]]])))

    (testing "join"
      (is (ksml* [:join this-table other-table join-fn]))
      (is (ksml* [:join this-table other-table join-fn
                  [:named "join-store"]
                  [:materialized [:with keySerde valSerde]]]))
      (is (ksml* [:join this-table other-table join-fn
                  [:materialized
                   [:as [:stores [:inMemoryKeyValueStore "yolo"]]]]])))

    (testing "left join"
      (is (ksml* [:left-join this-table other-table join-fn]))
      (is (ksml* [:left-join this-table other-table join-fn
                  [:named "join-store"]
                  [:materialized [:with keySerde valSerde]]]))
      (is (ksml* [:left-join this-table other-table join-fn
                  [:materialized
                   [:as [:stores [:inMemoryKeyValueStore "yolo"]]]]])))

    (testing "map values"
      (is (ksml* [:map-values this-table vmap]))
      (is (ksml* [:map-values this-table vmap
                  [:named "yolo"]]))
      (is (ksml* [:map-values this-table vmap
                  [:materialized
                   [:as [:stores [:inMemoryKeyValueStore "yolo"]]]]])))

    (testing "outer join"
      (is (ksml* [:outer-join this-table other-table join-fn]))
      (is (ksml* [:outer-join this-table other-table join-fn
                  [:named "outer-join-store"]
                  [:materialized [:with keySerde valSerde]]]))
      (is (ksml* [:outer-join this-table other-table join-fn
                  [:materialized
                   [:as [:stores [:inMemoryKeyValueStore "yolo"]]]]])))

    (testing "to-stream"
      (is (ksml* [:to-stream this-table]))
      (is (ksml* [:to-stream this-table
                  [:key-value-mapper (fn [k v]
                                       [k v])]]))
      (is (ksml* [:to-stream this-table
                  [:key-value-mapper (fn [k v]
                                       [k v])]
                  [:named "yolo"]]))
      (is (ksml* [:to-stream this-table
                  [:named "yolo"]])))))


(deftest test-kstream-ops
  (testing "branch"
    (is (ksml* [:branch [:stream topicPattern]
                 allow-all
                 allow-none]))
    (is (ksml* [:branch [:stream topicPattern]
                [:named "yolo"]
                allow-all
                allow-none]))
  (testing "filter"
    (is (ksml* [:filter [:stream topicPattern] allow-all]))
    (is (ksml* [:filter [:stream topicPattern] allow-all
                [:named "yolo"]])))

  (testing "filter-not"
    (is (ksml* [:filter-not [:stream topicPattern] allow-none]))
    (is (ksml* [:filter-not [:stream topicPattern] allow-none
                [:named "yolo"]])))

  (testing "flat-map"
    (is (ksml* [:flat-map [:stream #"foos"] kv-map]))
    (is (ksml* [:flat-map [:stream #"foos"] kv-map
                [:named "yolo"]])))

  (testing "flat-map-values"
    (is (ksml* [:flat-map-values [:stream #"foos"] vmap]))
    (is (ksml* [:flat-map-values [:stream #"foos"] vmap
                [:named "yolo"]])))

  (testing "foreach"
    (is (ksml* [:foreach [:stream topicPattern] side-effect!]))
    (is (ksml* [:foreach [:stream topicPattern] side-effect!
                [:named "yolo"]])))

  (testing "group-by"
    (is (ksml* [:group-by [:stream topicPattern] group-fn]))
    (is (ksml* [:group-by [:stream topicPattern] group-fn
                [:grouped [:as "foo"]]])))))

;;     (is (ksml* [:group-by [:stream topicPattern]
;;                 group-fn
;;                 keySerde
;;                 valSerde])))

;;   (let [this-stream [:stream #"left"]
;;         other-stream [:stream #"right"]
;;         other-global-table [:global-table keySerde valSerde "lookup"]
;;         other-table [:table "right"]
;;         join-fn [:value-joiner (fn [l r]
;;                                  (= (:id l) (:id r)))]]

;;     (testing "process"
;;       (is (ksml* [:process! this-stream
;;                   [:processor-supplier (fn [context k v]
;;                                          v)]
;;                   [:strs]])))


;;     (testing "join global table"
;;       (is (ksml* [:join-global this-stream other-global-table
;;                   kv-map
;;                   join-fn])))

;;     (testing "join stream with window"
;;       (is (ksml* [:join this-stream other-stream
;;                   join-fn
;;                   join-window])))

;;     (testing "join stream with window and serdes"
;;       (is (ksml* [:join this-stream other-stream
;;                   join-fn
;;                   join-window
;;                   keySerde
;;                   valSerde
;;                   valSerde])))

;;     (testing "join table"
;;       (is (ksml* [:join this-stream other-table join-fn])))

;;     (testing "join table with serdes"
;;       (is (ksml* [:join this-stream other-table join-fn keySerde valSerde])))

;;     (testing "left join global table"
;;       (is (ksml* [:left-join-global this-stream other-global-table
;;                   kv-map
;;                   join-fn])))

;;     (testing "left join stream with window"
;;       (is (ksml* [:left-join this-stream other-stream
;;                   join-fn
;;                   join-window])))


;;     (testing "left join stream with window and serdes"
;;       (is (ksml* [:left-join this-stream other-stream
;;                   join-fn
;;                   join-window
;;                   keySerde
;;                   valSerde
;;                   valSerde])))

;;     (testing "left join table"
;;       (is (ksml* [:left-join this-stream other-table
;;                   join-fn])))

;;     (testing "left join table with serdes"
;;       (is (ksml* [:left-join this-stream other-table
;;                   join-fn
;;                   keySerde
;;                   valSerde])))

;;     (testing "map"
;;       (is (ksml* [:map [:stream #"words"] kv-map])))

;;     (testing "map values"
;;       (is (ksml* [:map-values [:stream topicPattern] vmap])))

;;     (testing "outer join"
;;       (is (ksml* [:outer-join this-stream other-stream
;;                   join-fn
;;                   join-window]))
;;       (is (ksml* [:outer-join this-stream other-stream
;;                   join-fn join-window
;;                   keySerde valSerde valSerde])))

;;     (testing "peek"
;;       (is (ksml* [:peek! this-stream
;;                   [:foreach-action (fn [k v]
;;                                      "yolo")]])))

;;     (testing "print!"
;;       (is (ksml* [:print! [:stream #"foo"]]))
;;       (is (ksml* [:print! this-stream keySerde valSerde]))
;;       (is (ksml* [:print! this-stream keySerde valSerde "stream-name"]))
;;       (is (ksml* [:print! this-stream "stream-name"])))))

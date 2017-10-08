(ns cddr.ksml.eval-test
  (:require
   [clojure.test :refer :all]
   [cddr.ksml.eval :as k]
   [cddr.ksml.core :refer [ksml* v->]])
  (:import
   (java.io ByteArrayInputStream)
   (org.apache.kafka.common.serialization Serde Serializer Deserializer)
   (org.apache.kafka.streams.kstream KStream KStreamBuilder)
   (org.apache.kafka.streams.processor TopologyBuilder
                                       FailOnInvalidTimestamp)))

(def topics
  [:strs "a" "b" "c"])

(defn builder
  []
  (KStreamBuilder.))

(defn keval
  [expr]
  (cddr.ksml.eval/eval expr))

(def extractor
  {:fail FailOnInvalidTimestamp})

(def keySerde [:serde :byte-array])
(def valSerde [:serde :byte-array])
(def topicPattern #"p")
(def topic "foo")
(def topics [:strs "foo" "bar"])
(def offset [:offset-reset 'EARLIEST])
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

(deftest test-serde
  (testing "builtin"
    (is (ksml* [:serde :byte-array]))
    (is (ksml* [:serde :string]))))

(deftest test-eval-stream
  (testing "from pattern"
    (let [ksb (ksml* [:stream topicPattern])]
      (is (instance? KStreamBuilder ksb))
      (is (consumes-pattern? ksb))))

  (testing "from pattern with serdes"
    (let [ksb (ksml* [:stream keySerde valSerde topicPattern])]
      (is (instance? KStreamBuilder ksb))
      (is (consumes-pattern? ksb))))

  (testing "from topics with serdes"
    (let [ksb (ksml* [:stream keySerde valSerde topics])]
      (is (instance? KStreamBuilder ksb))
      (is (consumes-topics? ksb))))

  (testing "from topics"
    (let [ksb (ksml* [:stream topics])]
      (is (instance? KStreamBuilder ksb))
      (is (consumes-topics? ksb))))

  (testing "from pattern with serdes and timestamp extractor"
    (is (ksml* [:stream timestampExtractor
                keySerde
                valSerde
                topicPattern])))

  (testing "from topics with serdes and timestamp extractor"
    (is (ksml* [:stream timestampExtractor
                keySerde
                valSerde
                topics])))

  (testing "from pattern and offset"
    (is (ksml* [:stream offset topicPattern])))

  (testing "from pattern with serdes and offset"
    (is (ksml* [:stream offset keySerde valSerde topicPattern])))

  (testing "from topics with serde and offset"
    (is (ksml* [:stream offset keySerde valSerde topics])))

  (testing "from topics with offset"
    (is (ksml* [:stream offset topics])))

  (testing "from pattern with offset and timestamp-extractor and serdes"
    (is (ksml* [:stream offset timestampExtractor keySerde valSerde topicPattern])))

  (testing "from topics with offset and timestamp-extractor and serdes"
    (is (ksml* [:stream offset timestampExtractor keySerde valSerde topics]))))

(deftest test-eval-table
  (testing "from topic with serdes"
    (is (ksml* [:table keySerde valSerde topic])))

  (testing "from topic with serdes and state store supplier"
    (is (ksml* [:table keySerde valSerde topic state-store])))

  (testing "from topic with serdes and state-store name"
    (is (ksml* [:table keySerde valSerde topic "store-name"])))

  (testing "from topic"
    (is (ksml* [:table topic])))

  (testing "from topic with state store supplier"
    (is (ksml* [:table topic state-store])))

  (testing "from topic with state-store name"
    (is (ksml* [:table topic state-store-name])))

  (testing "from topic with timestamp extractor and serdes and state-store name"
    (is (ksml* [:table timestampExtractor keySerde valSerde topic state-store-name])))

  (testing "from topic with timestamp extractor and state-store name"
    (is (ksml* [:table timestampExtractor topic state-store-name])))

  (testing "from topic with offset and serdes"
    (is (ksml* [:table offset keySerde valSerde topic])))

  (testing "from topic with offset and state-store name"
    (is (ksml* [:table offset topic state-store-name])))

  (testing "from topic with offset"
    (is (ksml* [:table offset topic])))

  (testing "from topic with offset and state-store supplier"
    (is (ksml* [:table offset topic state-store])))

  (testing "from topic with offset and timestamp extractor and serdes"
    (is (ksml* [:table offset timestampExtractor
                keySerde valSerde
                topic state-store])))

  (testing "from topic with offset and timestamp extractor and serdes and state store-name"
    (is (ksml* [:table offset timestampExtractor
                keySerde valSerde
                topic
                state-store-name])))

  (testing "from topic with offset and timestamp extractor and state-store-name"
    (is (ksml* [:table offset timestampExtractor
                topic state-store-name]))))

(deftest test-global-stream
  (testing "from topic with serdes"
    (is (ksml* [:global-table keySerde valSerde topic])))

  (testing "from topic with serdes and state store supplier"
    (is (ksml* [:global-table keySerde valSerde topic state-store])))

  (testing "from topic with serde and queryableStoreName"
    (is (ksml* [:global-table keySerde valSerde topic queryable-store-name])))

  (testing "from topic with serde and timestamp extractor and queryable-store-name"
    (is (ksml* [:global-table keySerde valSerde timestampExtractor topic queryable-store-name])))

  (testing "from topic"
    (is (ksml* [:global-table topic])))

  (testing "from topic with queryable-store-name"
    (is (ksml* [:global-table topic queryable-store-name]))))

(deftest test-merge
  (let [foo [:stream #"foos"]
        bar [:stream #"bar"]]

    (testing "merge streams"
      (ksml* [:merge foo bar]))))

(def allow-all [:predicate (fn [k v] true)])
(def allow-none [:predicate (fn [k v] false)])
(def kv-map [:key-value-mapper (fn [k v]
                                 [k v])])
(def vmap [:value-mapper (fn [v] v)])
(def side-effect! [:foreach-action (fn [k1 v1])])
(def xform [:transformer (fn [k v] [k v])])
(def group-fn [:key-value-mapper (fn [v]
                                   (:part-id v))])

(def edn-serde
  [:serde
   [:serializer (fn [this topic data]
                  (when data
                    (.getBytes (pr-str data))))]
   [:deserializer (fn [this topic data]
                     (when data
                       (clojure.edn/read (ByteArrayInputStream. data))))]])

(deftest test-ktable
  (let [this-table [:table "left"]
        other-stream [:stream #"right"]
        other-global-table [:global-table keySerde valSerde "lookup"]
        other-table [:table "right"]
        join-fn [:value-joiner (fn [l r]
                                 (= (:id l) (:id r)))]]

    (testing "filter"
      (is (ksml* [:filter [:table topic] allow-all]))
      (is (ksml* [:filter [:table topic] allow-all "filtered-topic"]))
      (is (ksml* [:filter [:table topic] allow-all state-store])))

    (testing "filter-not"
      (is (ksml* [:filter-not [:table topic] allow-none]))
      (is (ksml* [:filter-not [:table topic] allow-all "filtered-topic"]))
      (is (ksml* [:filter-not [:table topic] allow-all state-store])))

    (testing "group-by"
      (is (ksml* [:group-by [:table topic]
                  kv-map]))

      (is (ksml* [:group-by [:table topic]
                  kv-map
                  keySerde
                  valSerde])))

    (testing "join"
      (is (ksml* [:join this-table other-table join-fn]))
      (is (ksml* [:join this-table other-table join-fn valSerde "join-store"]))
      (is (ksml* [:join this-table other-table join-fn state-store])))

    (testing "left join"
      (is (ksml* [:left-join this-table other-table join-fn]))
      (is (ksml* [:left-join this-table other-table join-fn valSerde "join-store"]))
      (is (ksml* [:left-join this-table other-table join-fn state-store])))

    (testing "map values"
      (is (ksml* [:map-values this-table vmap]))
      (is (ksml* [:map-values this-table vmap "map-store"]))
      (is (ksml* [:map-values this-table vmap state-store])))

    (testing "outer join"
      (is (ksml* [:outer-join this-table other-table
                  join-fn]))
      (is (ksml* [:outer-join this-table other-table
                  join-fn
                  valSerde
                  "outer-join-store"]))
      (is (ksml* [:outer-join this-table other-table
                  join-fn
                  state-store])))

    (testing "through"
      (is (ksml* [:through! this-table keySerde valSerde "through-topic"]))
      (is (ksml* [:through! this-table keySerde valSerde "through-topic" "through-topic-state"]))
      (is (ksml* [:through! this-table "through-topic" state-store]))
      (is (ksml* [:through! this-table keySerde valSerde partitioner
                  "through-topic" state-store]))
      (is (ksml* [:through! this-table keySerde valSerde partitioner
                  "through-topic" "through-topic-state"]))
      (is (ksml* [:through! this-table keySerde valSerde "through-topic"]))
      (is (ksml* [:through! this-table keySerde valSerde "through-topic" state-store]))
      (is (ksml* [:through! this-table keySerde valSerde "through-topic" "through-topic-state"]))
      (is (ksml* [:through! this-table partitioner "through-topic"]))
      (is (ksml* [:through! this-table partitioner "through-topic" state-store]))
      (is (ksml* [:through! this-table partitioner "through-topic" "through-topic-state"]))
      (is (ksml* [:through! this-table "through-topic"]))
      (is (ksml* [:through! this-table "through-topic" state-store]))
      (is (ksml* [:through! this-table "through-topic" "through-topic-state"])))

    (testing "to!"
      (is (ksml* [:to! this-table keySerde valSerde partitioner "through-topic"]))
      (is (ksml* [:to! this-table keySerde valSerde "through-topic"]))
      (is (ksml* [:to! this-table partitioner "through-topic"]))
      (is (ksml* [:to! this-table "through-topic"])))

    (testing "to-stream"
      (is (ksml* [:to-stream this-table]))
      (is (ksml* [:to-stream this-table kv-map])))))


(deftest test-kstream
  (testing "branch"
    (is (ksml* [:branch [:stream topicPattern]
                allow-all
                allow-none])))

  (testing "filter"
    (is (ksml* [:filter [:stream topicPattern] allow-all]))
    (is (ksml* [:filter [:stream topicPattern] allow-all])))

  (testing "filter-not"
    (is (ksml* [:filter-not [:stream topicPattern] allow-none])))

  (testing "flat-map"
    (is (ksml* [:flat-map [:stream #"foos"] kv-map])))

  (testing "flat-map-values"
    (is (ksml* [:flat-map-values [:stream #"foos"] vmap])))

  (testing "foreach"
    (is (ksml* [:foreach [:stream topicPattern] side-effect!])))

  (testing "group-by"
    (is (ksml* [:group-by [:stream topicPattern]
                group-fn]))

    (is (ksml* [:group-by [:stream topicPattern]
                group-fn
                keySerde
                valSerde])))

  (let [this-stream [:stream #"left"]
        other-stream [:stream #"right"]
        other-global-table [:global-table keySerde valSerde "lookup"]
        other-table [:table "right"]
        join-fn [:value-joiner (fn [l r]
                                 (= (:id l) (:id r)))]]

    (testing "process"
      (is (ksml* [:process! this-stream
                  [:processor-supplier (fn [context k v]
                                         v)]
                  [:strs]])))


    (testing "join global table"
      (is (ksml* [:join-global this-stream other-global-table
                  kv-map
                  join-fn])))

    (testing "join stream with window"
      (is (ksml* [:join this-stream other-stream
                  join-fn
                  join-window])))

    (testing "join stream with window and serdes"
      (is (ksml* [:join this-stream other-stream
                  join-fn
                  join-window
                  keySerde
                  valSerde
                  valSerde])))

    (testing "join table"
      (is (ksml* [:join this-stream other-table join-fn])))

    (testing "join table with serdes"
      (is (ksml* [:join this-stream other-table join-fn keySerde valSerde])))

    (testing "left join global table"
      (is (ksml* [:left-join-global this-stream other-global-table
                  kv-map
                  join-fn])))

    (testing "left join stream with window"
      (is (ksml* [:left-join this-stream other-stream
                  join-fn
                  join-window])))


    (testing "left join stream with window and serdes"
      (is (ksml* [:left-join this-stream other-stream
                  join-fn
                  join-window
                  keySerde
                  valSerde
                  valSerde])))

    (testing "left join table"
      (is (ksml* [:left-join this-stream other-table
                  join-fn])))

    (testing "left join table with serdes"
      (is (ksml* [:left-join this-stream other-table
                  join-fn
                  keySerde
                  valSerde])))

    (testing "map"
      (is (ksml* [:map [:stream #"words"] kv-map])))

    (testing "map values"
      (is (ksml* [:map-values [:stream topicPattern] vmap])))

    (testing "outer join"
      (is (ksml* [:outer-join this-stream other-stream
                  join-fn
                  join-window]))
      (is (ksml* [:outer-join this-stream other-stream
                  join-fn join-window
                  keySerde valSerde valSerde])))

    (testing "peek"
      (is (ksml* [:peek! this-stream
                  [:foreach-action (fn [k v]
                                     "yolo")]])))

    (testing "print!"
      (is (ksml* [:print! [:stream #"foo"]]))
      (is (ksml* [:print! this-stream keySerde valSerde]))
      (is (ksml* [:print! this-stream keySerde valSerde "stream-name"]))
      (is (ksml* [:print! this-stream "stream-name"])))))

(ns cddr.ksml.eval-test
  (:require
   [clojure.test :refer :all]
   [cddr.ksml.eval :as k]
   [cddr.ksml.core :refer [ksml*]]
   [clojure.spec.alpha :as s]
   [cddr.ksml.specs :as specs]
   [cddr.ksml.core :refer [ksml*]])
  (:import
   (org.apache.kafka.streams.kstream KStream KStreamBuilder )
   (org.apache.kafka.streams.processor TopologyBuilder
                                       FailOnInvalidTimestamp)))

(def serdes
  {:byte-array [:serde 'ByteArray]})

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

(def keySerde (serdes :byte-array))
(def valSerde (serdes :byte-array))
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

(defn consumes-pattern?
  [b]
  (= (str topicPattern)
     (str (.sourceTopicPattern b))))

(defn consumes-topics?
  [b]
  (let [topic-group (get (.topicGroups b) (int 0))]
    (= #{"foo" "bar"}
       (.sourceTopics topic-group))))

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

(def allow-all (fn [k v] true))
(def allow-none (fn [k v] false))
(def kv-map (fn [k v]
              [k v]))
(def vmap (fn [v] v))
(def side-effect! (fn [k1 v1]))
(def xform (fn [k v] [k v]))
(def group-fn (fn [k v]
                (:part-id v)))

(deftest test-kstream
  (testing "branch"
    (is (ksml* [:branch [:stream topicPattern]
                allow-all
                allow-none])))

  (testing "filter"
    (is (ksml* [:filter allow-all [:stream topicPattern]])))

  (testing "filter-not"
    (is (ksml* [:filter-not allow-none [:stream topicPattern]])))

  (testing "flat-map"
    (is (ksml* [:flat-map xform [:stream #"foos"]])))

  (testing "flat-map-values"
    (is (ksml* [:flat-map vmap [:stream #"foos"]])))

  (testing "foreach"
    (is (ksml* [:foreach [:stream topicPattern]
                side-effect!])))

  (testing "group-by"
    (is (ksml* [:group-by [:stream topicPattern]
                group-fn]))

    (is (ksml* [:group-by [:stream topicPattern]
                group-fn
                keySerde
                valSerde])))

  (testing "group-by-key"
    (is (ksml* [:group-by-key [:stream topicPattern]]))
    (is (ksml* [:group-by-key [:stream topicPattern]
                keySerde
                valSerde])))

  (let [this-stream [:stream #"left"]
        other-stream [:stream #"right"]
        other-global-table [:global-table keySerde valSerde "lookup"]
        other-table [:table "right"]
        join-fn (fn [l r]
                  (= (:id l) (:id r)))]

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
      (is (ksml* [:join this-stream other-table])))
           
    (testing "join table with serdes"
      (is (ksml* [:join this-stream other-table keySerde valSerde valSerde])))
    
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
                  valSerde])))))
              


  
  

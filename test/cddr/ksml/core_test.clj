(ns cddr.ksml.core-test
  (:require
   [clojure.test :refer :all]
   [cddr.ksml.core :refer [ksml* ksml v->]]))

(deftest test-v->
  (is [:bar [:baz [:foo]]]
      (v-> [:foo]
           [:bar]
           [:baz])))

(deftest test-ksml
  (is (ksml [:stream #"foo"])

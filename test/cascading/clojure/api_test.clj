(ns cascading.clojure.api-test
  (:use clojure.test
        (clojure.contrib [def :only (defvar-)]))
  (:import (cascading.tuple Fields Tuple TupleEntry TupleEntryCollector)
           (cascading.pipe Pipe)
           (cascading.operation ConcreteCall FunctionCall)
           (cascading.flow FlowProcess)
           (cascading.clojure ClojureFilter ClojureMap ClojureMapcat
                              ClojureAggregator Util)
           (clojure.lang IPersistentCollection))
  (:require (cascading.clojure [api :as c])))

(deftest test-ns-fn-name-pair
  (let [[ns-name fn-name] (c/ns-fn-name-pair #'str)]
    (is (= "clojure.core" ns-name))
    (is (= "str" fn-name))))

(deftest test-1-field
  (let [f1 (c/fields "foo")]
    (is (instance? Fields f1))
    (is (= '("foo") (seq f1)))))

(deftest test-n-fields
  (let [f2 (c/fields ["foo" "bar"])]
    (is (instance? Fields f2))
    (is (= `("foo" "bar") (seq f2)))))

(deftest test-uuid-pipe
  (let [up (c/pipe)]
    (is (instance? Pipe up))
    (is (= 36 (.length (.getName up))))))

(deftest test-named-pipe
  (let [np (c/pipe "foo")]
    (is (instance? Pipe np))
    (is (= "foo" (.getName np)))))

(defn inc1 [in]
  [(+ in 1)])

(defn incn [n]
  (fn [in] [(+ in n)]))

(def obj-array-class (class (into-array Object [])))

(deftest test-fn-spec-simple
  (let [fs (c/fn-spec #'inc1)]
    (is (instance? obj-array-class fs))
    (is (= '("cascading.clojure.api-test" "inc1") (seq fs)))))

(deftest test-fn-spec-hof
  (let [fs (c/fn-spec [#'incn 3])]
    (is (instance? obj-array-class fs))
    (is (= `("cascading.clojure.api-test" "incn" 3) (seq fs)))))

(deftest test-boot-fn-simple
  (let [spec (into-array Object `("cascading.clojure.api-test" "inc1"))
        f    (Util/bootFn spec)]
    (is (= [2] (f 1)))))

(deftest test-boot-fn-hof
  (let [spec (into-array Object '("cascading.clojure.api-test" "incn" 3))
        f    (Util/bootFn spec)]
  (is (= [4] (f 1)))))

(defn- roundtrip [obj]
  (cascading.util.Util/deserializeBase64
    (cascading.util.Util/serializeBase64 obj)))

(defn- invoke-filter [fil coll]
  (let [fil     (roundtrip fil)
        op-call (ConcreteCall.)
        fp-null FlowProcess/NULL]
    (.setArguments op-call (TupleEntry. (Util/coerceToTuple coll)))
    (.prepare fil fp-null op-call)
    (let [rem (.isRemove fil fp-null op-call)]
      (.cleanup fil fp-null op-call)
      rem)))

(deftest test-clojure-filter
  (let [fil (ClojureFilter. (c/fn-spec #'odd?))]
    (is (= false (invoke-filter fil [1])))
    (is (= true  (invoke-filter fil [2])))))

(defn- output-collector [out-atom]
  (proxy [TupleEntryCollector] []
    (add [tuple]
      (swap! out-atom conj (Util/coerceFromTuple tuple)))))

(defn- function-call [arg-coll]
  (let [out-atom (atom [])]
    (proxy [FunctionCall IPersistentCollection] []
      (getArguments []
        (TupleEntry. (Util/coerceToTuple arg-coll)))
      (getOutputCollector []
        (output-collector out-atom))
      (seq []
        (seq @out-atom)))))

(defn- op-call-results [func-call]
  (.seq func-call))

(defn- invoke-map [m coll]
  (let [m         (roundtrip m)
        func-call (function-call coll)
        fp-null   FlowProcess/NULL]
    (.prepare m fp-null func-call)
    (.operate m fp-null func-call)
    (.cleanup m fp-null func-call)
    (op-call-results func-call)))

(deftest test-clojure-map
  (let [m (ClojureMap. (c/fields "num") (c/fn-spec #'inc))]
    (is (= [[2]] (invoke-map m [1])))))

(ns cascading.clojure.api-test
  (:use clojure.test)
  (:require (cascading.clojure [api :as c]))
  (:import (cascading.tuple Fields Tuple TupleEntry TupleEntryCollector)
           (cascading.pipe Pipe)
           (cascading.operation ConcreteCall)
           (cascading.flow FlowProcess)
           (cascading.clojure ClojureFilter ClojureMap ClojureMapcat
                              ClojureAggregator Util)
           (clojure.lang IPersistentCollection)))

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

(defn- output-collector [out-atom]
  (proxy [TupleEntryCollector] []
    (add [tuple]
      (swap! out-atom conj (Util/coerceFromTuple tuple)))))

(defn- op-call []
  (let [args-atom    (atom nil)
        out-atom     (atom [])
        context-atom (atom nil)]
    (proxy [ConcreteCall IPersistentCollection] []
      (setArguments [tuple]
        (swap! args-atom (constantly tuple)))
      (getArguments []
         @args-atom)
      (getOutputCollector []
        (output-collector out-atom))
      (setContext [context]
        (swap! context-atom (constantly context)))
      (getContext []
        @context-atom)
      (seq []
        (seq @out-atom)))))

(defn- op-call-results [func-call]
  (.seq func-call))

(defn- invoke-function [m coll]
  (let [m         (roundtrip m)
        func-call (op-call)
        fp-null   FlowProcess/NULL]
    (.setArguments func-call (TupleEntry. (Util/coerceToTuple coll)))
    (.prepare m fp-null func-call)
    (.operate m fp-null func-call)
    (.cleanup m fp-null func-call)
    (op-call-results func-call)))

(defn- invoke-aggregator [a colls]
  (let [a       (roundtrip a)
        ag-call (op-call)
        fp-null FlowProcess/NULL]
    (.prepare a fp-null ag-call)
    (.start a fp-null ag-call)
    (doseq [coll colls]
      (.setArguments ag-call (TupleEntry. (Util/coerceToTuple coll)))
      (.aggregate a fp-null ag-call))
    (.complete a fp-null ag-call)
    (.cleanup  a fp-null ag-call)
    (op-call-results ag-call)))

(deftest test-clojure-filter
  (let [fil (ClojureFilter. (c/fn-spec #'odd?))]
    (is (= false (invoke-filter fil [1])))
    (is (= true  (invoke-filter fil [2])))))

(defn inc-wrapped [num]
  [(inc num)])

(defn inc-both [num1 num2]
  [(inc num1) (inc num2)])

(deftest test-clojure-map-one-field
  (let [m1 (ClojureMap. (c/fields "num") (c/fn-spec #'inc-wrapped))
        m2 (ClojureMap. (c/fields "num") (c/fn-spec #'inc))]
    (are [m] (= [[2]] (invoke-function m [1])) m1 m2)))

(deftest test-clojure-map-multiple-fields
  (let [m (ClojureMap. (c/fields ["num1" "num2"]) (c/fn-spec #'inc-both))]
    (is (= [[2 3]] (invoke-function m [1 2])))))

(defn iterate-inc-wrapped [num]
  (list [(+ num 1)] [(+ num 2)] [(+ num 3)]))

(defn iterate-inc [num]
  (list (+ num 1) (+ num 2) (+ num 3)))

(deftest test-clojure-mapcat-one-field
  (let [m1 (ClojureMapcat. (c/fields "num") (c/fn-spec #'iterate-inc-wrapped))
        m2 (ClojureMapcat. (c/fields "num") (c/fn-spec #'iterate-inc))]
    (are [m] (= [[2] [3] [4]] (invoke-function m [1])) m1 m2)))

(defn sum-start []
  0)

(defn sum-aggregate [mem v]
  (+ mem v))

(defn sum-complete [mem]
  [mem])

(deftest test-clojure-aggregator
  (let [a (ClojureAggregator. (c/fields "sum")
            (c/fn-spec #'sum-start)
            (c/fn-spec #'sum-aggregate)
            (c/fn-spec #'sum-complete))]
    (is (= [[6]] (invoke-aggregator a [[1] [2] [3]])))))

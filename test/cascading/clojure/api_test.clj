(ns cascading.clojure.api-test
  (:use clojure.test)
  (:require (cascading.clojure [api :as c]
			       [testing :as t]
			       [parse :as p]))
  (:import (cascading.tuple Fields)
           (cascading.pipe Pipe)
           (cascading.clojure ClojureFilter ClojureMap ClojureMapcat
                              ClojureAggregator ClojureBuffer Util)))

(deftest test-ns-fn-name-pair
  (let [[ns-name fn-name] (p/ns-fn-name-pair #'str)]
    (is (= "clojure.core" ns-name))
    (is (= "str" fn-name))))

(def obj-array-class (class (into-array Object [])))

(defn inc1 [in]
  [(+ in 1)])

(defn incn [n]
  (fn [in] [(+ in n)]))

(defn inc-wrapped [num]
  [(inc num)])

(defn inc-both [num1 num2]
  [(inc num1) (inc num2)])

(deftest test-fn-spec-simple
  (let [fs (p/fn-spec #'inc1)]
    (is (instance? obj-array-class fs))
    (is (= '("cascading.clojure.api-test" "inc1") (seq fs)))))

(deftest test-fn-spec-hof
  (let [fs (p/fn-spec [#'incn 3])]
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

(deftest test-1-field
  (let [f1 (p/fields "foo")]
    (is (instance? Fields f1))
    (is (= '("foo") (seq f1)))))

(deftest test-n-fields
  (let [f2 (p/fields ["foo" "bar"])]
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

(deftest test-clojure-filter
  (let [fil (ClojureFilter. (p/fn-spec #'odd?))]
    (is (= false (t/invoke-filter fil [1])))
    (is (= true  (t/invoke-filter fil [2])))))

(deftest test-clojure-map-one-field
  (let [m1 (ClojureMap. (p/fields "num") (p/fn-spec #'inc-wrapped))
        m2 (ClojureMap. (p/fields "num") (p/fn-spec #'inc))]
    (are [m] (= [[2]] (t/invoke-function m [1])) m1 m2)))

(deftest test-clojure-map-multiple-fields
  (let [m (ClojureMap. (p/fields ["num1" "num2"]) (p/fn-spec #'inc-both))]
    (is (= [[2 3]] (t/invoke-function m [1 2])))))

(defn iterate-inc-wrapped [num]
  (list [(+ num 1)] [(+ num 2)] [(+ num 3)]))

(defn iterate-inc [num]
  (list (+ num 1) (+ num 2) (+ num 3)))

(deftest test-clojure-mapcat-one-field
  (let [m1 (ClojureMapcat. (p/fields "num") (p/fn-spec #'iterate-inc-wrapped))
        m2 (ClojureMapcat. (p/fields "num") (p/fn-spec #'iterate-inc))]
    (are [m] (= [[2] [3] [4]] (t/invoke-function m [1])) m1 m2)))

(def sum (c/agg + 0))

(deftest test-clojure-aggregator
  (let [a (ClojureAggregator. (p/fields "sum") (p/fn-spec #'sum))]
    (is (= [[6]] (t/invoke-aggregator a [[1] [2] [3]])))))

(defn buff [it]
  (for [x it]
    [(apply + 1 x)]))

; TODO: notice Buffer exects a fn that takes an iterator and returns a seq of
; tuples. if we want to return only a single tuple, then we need to wrap the
; tuple in a seq.
(deftest test-clojure-buffer
  (let [a (ClojureBuffer. (p/fields "sum") (p/fn-spec #'buff))]
    (is (= [[2][3][4]] (t/invoke-buffer a [[1] [2] [3]])))))

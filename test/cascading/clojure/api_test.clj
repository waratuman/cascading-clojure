(ns cascading.clojure.api-test
  (:use clojure.test)
  (:import (cascading.tuple Fields)
           (cascading.pipe Pipe)
           (cascading.clojure Util ClojureMap))
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

(deftest test-named-pipe
  (let [np (c/named-pipe "foo")]
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

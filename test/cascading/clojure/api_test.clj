(ns cascading.clojure.api-test
  (:use clojure.test)
  (:import (cascading.tuple Fields)
            (cascading.pipe Pipe))
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

(deftest named-pipe
  (let [np (c/named-pipe "foo")]
    (is (instance? Pipe np))
    (is (= "foo" (.getName np)))))




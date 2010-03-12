(ns cascading.clojure.core-test
  (:use clojure.test)
  (:require (cascading.clojure [core :as c]))
  (:import (cascading.tuple Fields)))

(deftest fields-test
  (is (= (Fields. (into-array ["a" "b"]))
         (c/fields "a" "b"))))

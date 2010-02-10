(ns cascading.clojure.flow-test-new
  (:use clojure.test
        clojure.contrib.java-utils
        cascading.clojure.testing
        cascading.clojure.io)
  (:import (cascading.tuple Fields)
           (cascading.pipe Pipe)
           (cascading.clojure Util ClojureMap))
    (:require (cascading.clojure [api :as c])))

(defn uppercase
  {:fields "upword"}
  [word]
  (.toUpperCase word))

(deftest map-test
  (test-flow
   (in-pipes "word")
   (in-tuples [["foo"] ["bar"]])
   (fn [in] (-> in (c/map #'uppercase)))
   [["FOO"] ["BAR"]]))

(defn extract-key
  {:fields "key"}
  [val]
  (second (re-find #".*\((.*)\).*" val)))

(deftest extract-test
  (test-flow
   (in-pipes ["val" "num"])
   (in-tuples [["foo(bar)bat" 1] ["biz(ban)hat" 2]])
   (fn [in] (-> in (c/map "val" #'extract-key ["key" "num"])))
   [["bar" 1] ["ban" 2]]))

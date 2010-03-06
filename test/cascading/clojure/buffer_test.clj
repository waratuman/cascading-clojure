(ns cascading.clojure.buffer-test
  (:use clojure.test
        cascading.clojure.testing)
  (:import (cascading.tuple Fields)
           (cascading.pipe Pipe)
           (cascading.clojure Util ClojureMap))
  (:require (clj-json [core :as json]))
  (:require [clojure.contrib.duck-streams :as ds])
  (:require [clojure.contrib.java-utils :as ju])
  (:require (cascading.clojure [api :as c])))

(defn- max-by [keyfn coll]
  (let [maxer (fn [max-elem next-elem]
                (if (> (keyfn max-elem) (keyfn next-elem))
                  max-elem
                  next-elem))]
    (reduce maxer coll)))

(defn maxbuff [it]
  (list (max-by second it)))

(deftest buffer-max-for-each-group
  (test-flow
    (in-pipes ["word" "subcount"])
    (in-tuples [["bar" 1] ["bat" 7] ["bar" 3] ["bar" 2] ["bat" 4]])
    (fn [in] (-> in
               (c/group-by "word")
               (c/buffer #'maxbuff)))
    [["bar" 3] ["bat" 7]]))

;;Note that you can not walk the tuple iterator more than once
;;but you can hold on to the seq and walk that more than once.
(defn maxpairs [it]
  (let [biggest (max-by second it)]
    (map #(concat % biggest) (remove #(= % biggest) it))))

(deftest buffer-max-and-pair
  (test-flow
    (in-pipes ["word" "subcount"])
    (in-tuples [["bar" 1] ["bat" 7] ["bar" 3] ["bar" 2] ["bat" 4]])
    (fn [in] (-> in
               (c/group-by "word")
               (c/buffer [["word" "subcount" "maxword" "maxsubcount"]
                          #'maxpairs])))
    [["bar" 1 "bar" 3]
     ["bar" 2 "bar" 3]
     ["bat" 4 "bat" 7]]))

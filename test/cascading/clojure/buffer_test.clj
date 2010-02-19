(ns cascading.clojure.buffer-test
  (:use clojure.test
        clojure.contrib.java-utils
        clojure.contrib.seq-utils
        cascading.clojure.testing
        cascading.clojure.io)
  (:import (cascading.tuple Fields)
           (cascading.pipe Pipe)
           (cascading.clojure Util ClojureMap))
  (:require [clj-json :as json])
  (:require [clojure.contrib.duck-streams :as ds])
  (:require [clojure.contrib.java-utils :as ju])
  (:require (cascading.clojure [api :as c])))

(defn maxbuff [it]
  (letfn [(maxer [max-tuple next-tuple]
		     (if (> (second max-tuple) (second next-tuple))
			    max-tuple
			    next-tuple))]
  [(reduce maxer (c/tuple-seq it))]))

(deftest buffer-test
  (test-flow
    (in-pipes ["word" "subcount"])
    (in-tuples [["bar" 1] ["bat" 7] ["bar" 3] ["bar" 2] ["bat" 4]])
    (fn [in] (-> in
               (c/group-by "word")
	       
               (c/buffer ["word" "subcount"] [["word1" "subcount1"] #'maxbuff]
			 ["word1" "subcount1"])))
    [["bar" 3] ["bat" 7]]))

;;Note that you can not walk the tuple iterator more than once
;;but you can hold on to the seq and walk that more than once.
(defn maxpairs [it]
  (let [maxer (fn [max-tuple next-tuple]
		(if (> (second max-tuple) (second next-tuple))
		  max-tuple
		  next-tuple))
	tuples (c/tuple-seq it)
	biggest (reduce maxer tuples)
	pairs (for [x tuples
		    :when (not (= x biggest))]
		    (flatten (cons x biggest)))]
	pairs))

(deftest buffer-test
  (test-flow
    (in-pipes ["word" "subcount"])
    (in-tuples [["bar" 1] ["bat" 7] ["bar" 3] ["bar" 2] ["bat" 4]])
    (fn [in] (-> in
               (c/group-by "word")
	       
               (c/buffer [["word1" "subcount1" "maxword" "maxsubcount"]
			  #'maxpairs])))
    [["bar" 1 "bar" 3] ["bar" 2 "bar" 3]
     ["bat" 4 "bat" 7]]))
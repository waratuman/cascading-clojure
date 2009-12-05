(ns cascading.clojure.join-bootstrap
  (:require [cascading.clojure clj_iterator])
  (:import [cascading.pipe.cogroup GroupClosure Joiner]
           [cascading.tuple Tuple]
	   [cascading.clojure CljIterator]))

(gen-class
 :name cascading.clojure.JoinBootstrap
 :implements [cascading.pipe.cogroup.Joiner]
 :constructors {[clojure.lang.IFn clojure.lang.IFn clojure.lang.IFn clojure.lang.IFn String Integer]
		[]}
 :init init
 :state state)

(defn -init [reader writer join-fn callback ns-name num-pipe-fields]
  [[] {"reader" reader "writer" writer "join-fn" join-fn "callback" callback
       "ns-name" ns-name "num-pipe-fields" num-pipe-fields}])

(defn -getIterator [this group-closure]
  (let [state (.state this)]
    (CljIterator. group-closure 
		       (state "reader")
		       (state "writer")
		       (state "join-fn")
		       (state "callback")
		       (state "num-pipe-fields"))))

(defn -numJoins 
  "unlimited number of joins"
  [this] -1)




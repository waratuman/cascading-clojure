(ns cascading.clojure.clj-iterator
  (:import [cascading.pipe.cogroup CoGroupClosure GroupClosure]
           [cascading.tuple Tuple]
	   [java.util Collections]
	   [org.apache.log4j Logger])
  (:use [cascading.clojure utils]
	[clojure.contrib seq-utils combinatorics]))

(gen-class
 :name cascading.clojure.CljIterator
 :implements [java.util.Iterator java.io.Serializable]
 :constructors {[cascading.pipe.cogroup.GroupClosure 
		 clojure.lang.IFn clojure.lang.IFn 
		 clojure.lang.IFn clojure.lang.IFn Integer]
		[]}
 :init init
 :state state)

(defn outer?
  [cogroup-closure i]
  (and (= i (- (.size cogroup-closure) 1))
       (= 0 (.. cogroup-closure (getGroup i) size))))

(defn make-lists
  [cogroup-closure num-fields]
  (for [i (range (.size cogroup-closure))]
    (if (outer? cogroup-closure i)
      [(Tuple/size num-fields)]
      nil)))

(defn init-iters 
  [cogroup-closure singleton-lists]
  (for [[idx l] (indexed singleton-lists)]
    (if (nil? l)
      (.getIterator cogroup-closure idx)
      (.iterator l))))

(defn cartesian-product-iterator
  [cogroup-closure lists]
  (.iterator 
   (apply cartesian-product 
	  (map iterator-seq 
	       (init-iters cogroup-closure lists)))))

(defn -init [cogroup-closure reader writer join-fn callback num-fields]
  (let [lists 
	(make-lists cogroup-closure num-fields)
	cartesian-prod-iter 
	(cartesian-product-iterator cogroup-closure lists)]
	[[] {"reader" reader 
	     "cogroup-closure" cogroup-closure 
	     "writer" writer
	     "join-fn" join-fn 
	     "callback" callback 
	     "cartesian-prod-iter" cartesian-prod-iter}]))

(defn -hasNext [this]
  (let [state (.state this)
	cartesian-prod-iter (state "cartesian-prod-iter")]
    (.hasNext cartesian-prod-iter)))

(defn -next [this]
  (let [state (.state this)
	cartesian-prod-iter (state "cartesian-prod-iter")
	next-vals (apply concat 
			 (map #(iterator-seq (.iterator %)) 
			      (.next cartesian-prod-iter)))]
      (Tuple. (into-array 
	       Comparable 
	       ((state "callback") 
		(state "reader") 
		(state "writer") 
		(state "join-fn") next-vals)))))

(defn -remove [this]
  (throw (RuntimeException. "remove not implemented")))

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
 :constructors {[cascading.pipe.cogroup.GroupClosure clojure.lang.IFn clojure.lang.IFn clojure.lang.IFn clojure.lang.IFn Integer]
		[]}
 :init init
 :state state)

(defn -init [group-closure reader writer join-fn callback num-fields]
  (letfn [(is-outer [i]
		    (and (= i (- (.size group-closure) 1))
			 (= 0 (.. group-closure (getGroup i) size))))
	  (singleton-lists []
			   (for [i (range (.size group-closure))]
			     (if (is-outer i)
			       (Collections/singletonList (Tuple/size num-fields))
			       nil)))
	  (init-iters [singleton-lists]
		      (for [[idx l] (indexed singleton-lists)]
			(if (nil? l)
			  (.. group-closure (getIterator idx))
			  (.getIterator l))))]
    (let [singleton-lists (singleton-lists)
	   cartesian-prod-iter (.iterator (apply cartesian-product (map iterator-seq (init-iters singleton-lists))))]
	  [[] {"reader" reader "group-closure" group-closure "writer" writer
	       "join-fn" join-fn "callback" callback "logger" (Logger/getLogger (class *ns*))
	       "cartesian-prod-iter" cartesian-prod-iter}])))

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
      (Tuple. (into-array Comparable ((state "callback")
				    (state "reader") (state "writer") 
				    (state "join-fn") next-vals)))))

(defn -remove [this]
  (throw (RuntimeException. "remove not implemented")))
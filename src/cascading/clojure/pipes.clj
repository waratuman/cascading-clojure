(ns cascading.clojure.pipes
  (:require [cascading.clojure
             function_filter_bootstrap
             function_bootstrap
	     join_bootstrap
             aggregation_bootstrap])
  (:import [cascading.clojure
            FunctionFilterBootstrapInClojure 
	    FunctionBootstrap
            AggregationBootstrap
            JoinBootstrap]
	   [cascading.pipe
            Each Pipe Every GroupBy CoGroup]
	   [cascading.tuple
            Fields Tuple
            TupleEntryCollector TupleEntry])
  (:use [clojure.contrib.monads
         :only (defmonad with-monad m-lift)]))

(defn seqable? [x] (or (seq? x) (string? x)))
(defn nil-or-empty? [coll] 
  (or (nil? coll) 
      (and (seqable? coll) (empty? coll))))

(defmonad maybe-nilempty-m
   [m-zero   nil
    m-result (fn m-result-maybe [v] v)
    m-bind   (fn m-bind-maybe [mv f]
               (if (nil-or-empty? mv) nil (f mv)))
    m-plus   (fn m-plus-maybe [& mvs]
               (first (drop-while nil-or-empty? mvs)))])

(defn single-val-callback 
  [reader writer f x]
  (let [result (apply f (map reader (seq x)))]
    [(writer result)]))

(defn default-clj-callback 
  "expect [[rowitems] [rowitems] [rowitems]]"
  [reader writer f x]
  (for [result (apply f (map reader (seq x)))]
    (map writer result)))

(defn filter-callback [reader f x]
  (apply f (map reader (seq x))))

(defn join-clj-callback 
  [reader writer f x]
  (let [result (apply f (map reader (seq x)))]
    (map writer result)))

(defn everygroup-clj-callback [reader writer f acc-val x]
  (apply f acc-val (map reader x)))

(defn fields [coll] 
  (Fields. (into-array coll)))

(defn group-fields [n on-fields] 
  (into-array Fields (repeat n (fields on-fields))))

(defn pipe-type [prev-or-name wf]
   (:optype wf))

;;TODO: the multimethod is OK for now but at this point is is just as easy to pass the functions for different pipe-types arounda s first class and then apply them whne needed rather than passing keys around to look the functions up by keyword.  This would be essentially the final step away from map based and tward function composition based api.
(defmulti pipe pipe-type)

;;TODO: these are default.
(def common-wf-fields {:using identity 
		       :reader read-string 
		       :writer pr-str 
		       :inputFields ["line"] 
		       :outputFields ["data"]})

(defmethod pipe :each 
  [prev-or-name wf-without-defaults]
  (let [wf (merge common-wf-fields wf-without-defaults)]
     (Each. prev-or-name (fields (:inputFields wf)) 
	    (FunctionBootstrap. (fields (:inputFields wf)) 
				(fields (:outputFields wf)) 
				(:reader wf) 
				(:writer wf) 
				(:using wf) 
				default-clj-callback 
				(:namespace wf)))))

(defmethod pipe :filter 
  [prev-or-name wf-without-defaults]
  (let [wf (merge common-wf-fields 
		  {:using (fn [x] true)}
		  wf-without-defaults)]
     (Each. prev-or-name (fields (:inputFields wf)) 
	    (FunctionFilterBootstrapInClojure. 
	     (fields (:inputFields wf)) 
	     (fields (:outputFields wf)) 
	     (:reader wf) 
	     (:writer wf) 
	     (:using wf) 
	     filter-callback 
	     (:namespace wf)))))

(defmethod pipe :groupBy
  [prev-or-name wf-without-defaults]
  (let [wf (merge common-wf-fields 
		  {:using (fn [x] [1 x])
		   :outputFields ["key", "clojurecode"]}
		  wf-without-defaults)]
     (GroupBy. (Each. prev-or-name (fields (:inputFields wf)) 
		      (FunctionBootstrap. 
		       (fields (:inputFields wf)) 
		       (fields (:outputFields wf)) 
		       (:reader wf) 
		       (:writer wf) 
		       (:using wf) 
		       default-clj-callback 
		       (:namespace wf))) 
	       Fields/FIRST)))

(defmethod pipe :everygroup
  [prev-or-name wf-without-defaults]
  (let [wf (merge common-wf-fields 
		  {:init (fn [] [""]) 
;;note the pattern of getting the first element of the wrapping vector before performing the operation.
		   :using (fn [acc next-line] 
			    [(str (first acc) next-line)])}
		  wf-without-defaults)]
    (Every. prev-or-name (fields (:inputFields wf)) 
	    (AggregationBootstrap. 
	     (fields (:inputFields wf)) 
	     (fields (:outputFields wf)) 
	     (:reader wf) 
	     (:writer wf) 
	     (:using wf) 
	     (:init wf) 
	     everygroup-clj-callback 
	     (:namespace wf)))))

(defmethod pipe :join
  [prev-or-name wf-without-defaults]
  (let [join-wf (merge common-wf-fields 
		  {:groupFields [] 
		  :wfs [] 
		  :wftype :join}
		  wf-without-defaults)
	[wf1 wf2] (:pipes join-wf)
	[grp-fields1 grp-fields2] (:groupFields join-wf)]
    (CoGroup. prev-or-name
              wf1
	      (fields grp-fields1)
	      wf2
	      (fields grp-fields2)
	      (fields (:outputFields join-wf)) 
	      (JoinBootstrap. (:reader join-wf) 
				(:writer join-wf) 
				(:using join-wf) 
				join-clj-callback 
				(:namespace join-wf) 
				(count (:outputFields join-wf))))))

(defn uuid [] (.toString (java.util.UUID/randomUUID)))

(defn mk-pipe 
  ([pipeline-ns fns] 
     (mk-pipe (uuid) pipeline-ns fns))
  ([prev-or-name pipeline-ns fns]
     (if-let [[op-type operations] (first fns)]
       (mk-pipe 
	(pipe 
	 prev-or-name 
	 (merge operations 
	   {:namespace pipeline-ns
	    :optype op-type})) 
	pipeline-ns 
	(rest fns))
       prev-or-name)))

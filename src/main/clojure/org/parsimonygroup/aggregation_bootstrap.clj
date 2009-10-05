(ns org.parsimonygroup.aggregation-bootstrap
  (:import [cascading.flow FlowProcess]
           [cascading.operation BaseOperation Aggregator AggregatorCall]
           [cascading.tuple Fields Tuple TupleEntryCollector TupleEntry])
  (:use org.parsimonygroup.utils))


(gen-class
 :name org.parsimonygroup.AggregationBootstrap
 :extends cascading.operation.BaseOperation
 :implements [cascading.operation.Aggregator]
 :constructors {[cascading.tuple.Fields cascading.tuple.Fields
                 clojure.lang.IFn clojure.lang.IFn
                 clojure.lang.IFn clojure.lang.IFn clojure.lang.IFn String]
		[cascading.tuple.Fields]}
 :init init
 :state state)

(defn -init [in-fields out-fields reader writer function init-fn
	     clj-callback fn-ns-name]
  [[out-fields] {"reader" reader "writer" writer "function" function
                 "init-fn" init-fn "clj-callback" clj-callback
                 "fn-ns-name" fn-ns-name}])

(defn -prepare [this _ _]
  (boot-clojure (.state this)))

(defn -start [this flow-process aggregator-call]
  (let [state (.state this)
	acc (.. aggregator-call getContext)]
    (if (nil? acc) 
      (.setContext aggregator-call (into-array Object [((state "init-fn"))]))
      (aset (.. aggregator-call getContext) 0
	    ((state "init-fn"))))))

(defn -aggregate [this flow-process aggregator-call]
  (let [state (.state this)]
    (aset (.. aggregator-call getContext) 0 
	  ((state "clj-callback") 
	   (state "reader") (state "writer")
	   (state "function") 
	   (aget (.. aggregator-call getContext) 0)
	   (iterator-seq (.. aggregator-call getArguments getTuple iterator))))))

(defn -complete [this flow-process aggregator-call]
  (let [state (.state this)]
    (.. aggregator-call getOutputCollector (add (Tuple. (into-array Comparable (map (state "writer") (aget (.. aggregator-call getContext) 0))))))))




(ns org.parsimonygroup.function-bootstrap
  (:import [org.parsimonygroup ClojureCascadingHelper]
           [cascading.flow FlowProcess]
           [cascading.operation BaseOperation Function FunctionCall]
           [cascading.tuple Fields Tuple TupleEntryCollector TupleEntry]))


(gen-class
 :name org.parsimonygroup.FunctionBootstrapInClojure
 :extends cascading.operation.BaseOperation
 :implements [cascading.operation.Function]
 :constructors {[cascading.tuple.Fields cascading.tuple.Fields
                 clojure.lang.IFn clojure.lang.IFn
                 clojure.lang.IFn clojure.lang.IFn String]
                [cascading.tuple.Fields]}
 :init init
 :state state)


;; Oddly Cascading gags when trying to serialize
;; Clojure keywords so we can't use them here:
(defn -init [in-fields out-fields reader writer function
             clj-callback fn-ns-name]
  [[out-fields] {"reader" reader "writer" writer "function" function
                 "clj-callback" clj-callback
                 "fn-ns-name" fn-ns-name}])

(defn -prepare [this _ _]
  (println "gbj prepare")
  (require 'clojure.main)
  (require 'org.parsimonygroup.cascading)
  (require (symbol ((.state this) "fn-ns-name"))))


(defn process-data [this arguments collector]
   (let [state (.state this)
         result-tuples
	 ((state "clj-callback" ) 
	  (state "reader" ) (state "writer" )
	  (state "function" )  (iterator-seq (.. arguments getTuple iterator)))
	 result-tuples (map #(Tuple. (into-array Comparable %)) result-tuples)]
     (doseq [tuple result-tuples]
       (.add collector tuple))))

(defn -operate [this flow-process function-call]
  (println "gbj operate FunctionBootstrapInClojure")
  (let [output-collector (.getOutputCollector function-call)
        arguments (.getArguments function-call)]
    (process-data this arguments output-collector)))



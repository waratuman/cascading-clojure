(ns org.parsimonygroup.function-filter-bootstrap
  (:import [org.parsimonygroup ClojureCascadingHelper]
           [cascading.flow FlowProcess]
           [cascading.operation BaseOperation Function FunctionCall]
           [cascading.tuple Fields Tuple TupleEntryCollector TupleEntry])
  (:use org.parsimonygroup.utils))

(gen-class
 :name org.parsimonygroup.FunctionFilterBootstrapInClojure
 :extends cascading.operation.BaseOperation
 :implements [cascading.operation.Function]
 :constructors {[cascading.tuple.Fields cascading.tuple.Fields
                 clojure.lang.IFn clojure.lang.IFn
                 clojure.lang.IFn clojure.lang.IFn String]
                [int cascading.tuple.Fields]}
 :init init
 :state state)


;; Oddly Cascading gags when trying to serialize
;; Clojure keywords so we can't use them here:
(defn -init [in-fields out-fields reader writer function
             clj-callback fn-ns-name]
  [[(.size in-fields) out-fields]
   {"reader" reader "writer" writer "function" function
    "clj-callback" clj-callback "fn-ns-name" fn-ns-name}])

(defn -prepare [this _ _]
  (boot-clojure (.state this)))

(defn process-data [this arguments]
   (let [state (.state this)]
     (if ((state "clj-callback" )
          (state "reader" )
          (state "function" )  (iterator-seq (.. arguments getTuple iterator)))
      (.. arguments getTuple))))


(defn -operate [this flow-process function-call]
  (let [arguments (.getArguments function-call)
        result (process-data this arguments)
        output-collector (.getOutputCollector function-call)]
    (if (not (nil? result)) (.add output-collector result))))


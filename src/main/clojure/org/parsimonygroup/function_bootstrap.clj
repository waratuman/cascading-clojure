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
                 "clojure-helper"
                 (ClojureCascadingHelper. fn-ns-name)}])

(defn process-data [this arguments collector]
  (try
   (let [state (.state this)
         result-tuples
         (.callClojure (state "clojure-helper" ) arguments
                       (state "function" ) (state "clj-callback" )
                       (state "reader" ) (state "writer" ))]
     (doseq [tuple result-tuples]
       (.add collector tuple)))
   (catch Exception e
         (throw (RuntimeException. "error prcessing Data" e)))))

(defn -operate [this flow-process function-call]
  (println "gbj operate FunctionBootstrapInClojure")
  (try
   (.bootClojure ((.state this) "clojure-helper"))
   (catch Exception e
     (.printStackTrace e)))
  (let [output-collector (.getOutputCollector function-call)
        arguments (.getArguments function-call)]
    (process-data this arguments output-collector)))



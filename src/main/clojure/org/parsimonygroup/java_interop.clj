(ns org.parsimonygroup.java-interop
  (:import [org.parsimonygroup FunctionBootstrap FunctionFilterBootstrap GroupByFunctionBootstrap GroupByFilterBootstrap AggregationOperationBootstrap ClojureCascadingHelper RollingWindowScheme GroupByMultipleEachOutputsFunctionBootstrap JoinerBootstrap GroupByFunctionBootstrap2]
	   [cascading.pipe Each Pipe Every GroupBy CoGroup]
	   [cascading.tuple Fields Tuple TupleEntryCollector TupleEntry])
  (:use [clojure.contrib.monads :only (defmonad with-monad m-lift)]))

(defn seqable? [x] (or (seq? x) (string? x)))
(defn nil-or-empty? [coll] (or (nil? coll) (and (seqable? coll) (empty? coll))))
(defmonad maybe-nilempty-m 
   [m-zero   nil
    m-result (fn m-result-maybe [v] v)
    m-bind   (fn m-bind-maybe [mv f]
               (if (nil-or-empty? mv) nil (f mv)))
    m-plus   (fn m-plus-maybe [& mvs]
	       (first (drop-while nil-or-empty? mvs)))])

(defn default-clj-callback [reader writer f x] 
  (with-monad maybe-nilempty-m
     (def reader-m (m-lift 1 reader))
     (def f-m (m-lift 1 f)))
     (writer (f-m (reader-m x))))

(defn groupby-clj-callback [reader writer f out-collector x] 
  (letfn [(collect-grps [kv-map] 
	    (for [[k v] kv-map] (. out-collector add (Tuple. (into-array String (writer k) (writer v))))))
	  (write-out [kv-map]
	    (into {} (for [[k v] kv-map] [(writer k) (writer v)])))]
    (with-monad maybe-nilempty-m
		(def reader-m (m-lift 1 reader))
		(def f-m (m-lift 1 f)))
        (write-out (f-m (reader-m x)))))

(defn everygroup-clj-callback [reader writer f acc-val x] (f acc-val (reader x)))
(defn join-clj-callback [reader writer joinFn args]
  (writer (apply joinFn (map reader (seq args)))))


;; multimethods instead?
(defn each-j [prev wf]
  (Each. prev (FunctionBootstrap. (:reader wf) (:writer wf) (:using wf) default-clj-callback (:namespace wf))))

(defn c-filter-j [prev wf]
  (Each. prev (FunctionFilterBootstrap. (:reader wf) (:writer wf) (:using wf) default-clj-callback (:namespace wf))))

(defn groupBy-j [prev wf]
  (GroupBy. (Each. prev (GroupByFunctionBootstrap. (:reader wf) (:writer wf) (:using wf) (:groupby wf) default-clj-callback (:namespace wf))) Fields/FIRST))

(defn groupBy2-j [prev wf]
  (GroupBy. (Each. prev (GroupByFunctionBootstrap2. (:reader wf) (:writer wf) (:using wf) groupby-clj-callback (:namespace wf))) Fields/FIRST))

(defn everyGroup-j [prev wf]
  (Every. prev (AggregationOperationBootstrap. (:reader wf) (:writer wf) (:using wf) (:init wf) everygroup-clj-callback (:namespace wf))))

(defn transformation-j [prev wf]  
  (GroupBy. 
   (Each. prev 
	  (GroupByMultipleEachOutputsFunctionBootstrap. 
	   (:reader wf) (:writer wf) (:using wf) 
	   default-clj-callback (:namespace wf))) Fields/FIRST))

(defn group-fields [n k] (into-array Fields (repeat n Fields/FIRST)))

(defn join-j [fnNs joinWf pipes outSize]
 (CoGroup. (into-array Pipe pipes) 
	   (group-fields 2 (:on joinWf)) 
	   (Fields/size outSize) 
	   (JoinerBootstrap. (:reader joinWf) 
			     (:writer joinWf) 
			     (:using joinWf) 
			     join-clj-callback fnNs outSize)))


;; (defn present? [x] ((complement nil?) x))

;; (defn hadoop-filter [reader writer pred x] (writer (pred (reader x))))
;; (defn cascading-filter [reader writer pred fnNsName] (FunctionFilterBootstrap. reader writer pred fnNsName))
;; (defn groupby-filter [reader writer pred groupby fnNsName] (GroupByFilterBootstrap. reader writer pred groupby fnNsName))

;; (defn hadoop-function [reader writer f x] (writer (f (reader x))))
;; (defn cascading-function [reader writer f fnNsName] (FunctionBootstrap. reader writer f fnNsName))
;; (defn groupby-function [reader writer f groupby fnNsName] (GroupByFunctionBootstrap. reader writer f groupby fnNsName))

;; (defn json-function [reader writer f x] (writer (f (reader x))))
                           
;; (defn aggregate-dataconverter [reader writer f acc-val x] (f acc-val (reader x)))
;; (defn aggregate-function [reader writer f init fnNsName] (AggregationOperationBootstrap. reader writer f init fnNsName))

;; (defn cascading-each [op reader writer f prev fnNsName] (Each. prev (op reader writer f fnNsName)))
;; (defn groupby-each [op reader writer f groupby prev fnNsName] (Each. prev (op reader writer f groupby fnNsName)))
;; (defn cascading-every [op reader writer f init prev fnNsName] (Every. prev (op reader writer f init fnNsName)))

;; (def optype-functiontable {:each (partial cascading-each cascading-function)
;;                            :filter (partial cascading-each cascading-filter)
;;                            :everygroup (partial cascading-every aggregate-function) })
;; (def groupby-functiontable {:each (partial groupby-each groupby-function)
;;                             :filter (partial groupby-each groupby-filter) })

;; (defn cascading-op-maker[prev f fnNsName]
;;   (let [[opType options] f]
;;     (cond
;;       (present? (:groupby options)) (GroupBy. ((get groupby-functiontable opType) (:reader options) (:writer options) (:using options) (:groupby options) prev fnNsName) Fields/FIRST)
;;       (= :everygroup opType) ((get optype-functiontable opType) (:reader options) (:writer options) (:using options) (:init options) prev fnNsName)
;;       (present? (:transformation options)) (GroupBy. (Each. prev (GroupByMultipleEachOutputsFunctionBootstrap. (:reader options) (:writer options) (:using options) fnNsName)) Fields/FIRST)
;;       :else ((get optype-functiontable opType) (:reader options) (:writer options) (:using options) prev fnNsName))))
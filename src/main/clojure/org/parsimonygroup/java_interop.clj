(ns org.parsimonygroup.java-interop
  (:import [org.parsimonygroup FunctionFilterBootstrap GroupByFunctionBootstrap AggregationOperationBootstrap ClojureCascadingHelper  JoinerBootstrap]
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
  (apply (partial f acc-val) (map reader (seq x))))


(defn mk-fields [coll] (Fields. (into-array String coll)))
;; multimethods instead?
(defn each-j 
  ([prev-or-name wf]
     (Each. prev-or-name (mk-fields (:inputFields wf)) (org.parsimonygroup.FunctionBootstrap. (mk-fields (:inputFields wf)) (mk-fields (:outputFields wf)) (:reader wf) (:writer wf) (:using wf) default-clj-callback (:namespace wf)))))

(defn c-filter-j 
  [prev-or-name wf]
     (Each. prev-or-name (mk-fields (:inputFields wf)) (org.parsimonygroup.FunctionFilterBootstrapInClojure. (mk-fields (:inputFields wf)) (mk-fields (:outputFields wf)) (:reader wf) (:writer wf) (:using wf) filter-callback (:namespace wf))))

(defn groupBy-j
  [prev-or-name wf]
     (GroupBy. (Each. prev-or-name (mk-fields (:inputFields wf)) (GroupByFunctionBootstrap. (mk-fields (:inputFields wf)) (mk-fields (:outputFields wf)) (:reader wf) (:writer wf) (:using wf) (:groupby wf) default-clj-callback (:namespace wf))) Fields/FIRST))

(defn everyGroup-j 
  [prev-or-name wf]
     (Every. prev-or-name (mk-fields (:inputFields wf)) (AggregationOperationBootstrap. (mk-fields (:inputFields wf)) (mk-fields (:outputFields wf)) (:reader wf) (:writer wf) (:using wf) (:init wf) everygroup-clj-callback (:namespace wf))))

(defn group-fields [n on-fields] (into-array Fields (repeat n (mk-fields on-fields))))

(defn join-j [prev-or-name join-wf]
  (let [[wf1 wf2] (:pipes join-wf)
	[grp-fields1 grp-fields2] (:groupFields join-wf)]
    (CoGroup. prev-or-name
              wf1
	      (mk-fields grp-fields1)
	      wf2
	      (mk-fields grp-fields2)
	      (mk-fields (:outputFields join-wf)) 
	      (JoinerBootstrap. (:reader join-wf) 
				(:writer join-wf) 
				(:using join-wf) 
				join-clj-callback (:namespace join-wf) (count (:outputFields join-wf))))))


;; (defn present? [x] ((complement nil?) x))

;; (defn hadoop-filter [reader writer pred x] (writer (pred (reader x))))
;; (defn cascading-filter [reader writer pred fnNsName] (FunctionFilterBootstrap. reader writer pred fnNsName))

;; (defn hadoop-function [reader writer f x] (writer (f (reader x))))
;; (defn cascading-function [reader writer f fnNsName] (FunctionBootstrap. reader writer f fnNsName))
;; (defn groupby-function [reader writer f groupby fnNsName] (GroupByFunctionBootstrap. reader writer f groupby fnNsName))

;; (defn json-function [reader writer f x] (writer (f (reader x))))
                           
;; (defn aggregate-dataconverter [reader writer f acc-val x] (f acc-val (reader x)))
;; (defn aggregate-function [reader writer f init fnNsName] (AggregationOperationBootstrap. reader writer f init fnNsName))

;; (defn cascading-each [op reader writer f prev-or-name fnNsName] (Each. prev-or-name (op reader writer f fnNsName)))
;; (defn groupby-each [op reader writer f groupby prev-or-name fnNsName] (Each. prev-or-name (op reader writer f groupby fnNsName)))
;; (defn cascading-every [op reader writer f init prev-or-name fnNsName] (Every. prev-or-name (op reader writer f init fnNsName)))

;; (def optype-functiontable {:each (partial cascading-each cascading-function)
;;                            :filter (partial cascading-each cascading-filter)
;;                            :everygroup (partial cascading-every aggregate-function) })
;; (def groupby-functiontable {:each (partial groupby-each groupby-function)
;;                             :filter (partial groupby-each groupby-filter) })

;; (defn cascading-op-maker[prev-or-name f fnNsName]
;;   (let [[opType options] f]
;;     (cond
;;       (present? (:groupby options)) (GroupBy. ((get groupby-functiontable opType) (:reader options) (:writer options) (:using options) (:groupby options) prev-or-name fnNsName) Fields/FIRST)
;;       (= :everygroup opType) ((get optype-functiontable opType) (:reader options) (:writer options) (:using options) (:init options) prev-or-name fnNsName)
;;       (present? (:transformation options)) (GroupBy. (Each. prev-or-name (GroupByMultipleEachOutputsFunctionBootstrap. (:reader options) (:writer options) (:using options) fnNsName)) Fields/FIRST)
;;       :else ((get optype-functiontable opType) (:reader options) (:writer options) (:using options) prev-or-name fnNsName))))
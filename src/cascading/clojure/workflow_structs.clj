(ns cascading.clojure.workflow-structs
  (:use cascading.clojure.java-interop)
  (:import [cascading.scheme TextLine]
	   [cascading.tap Hfs]))

;for structs, you must call merge with the struct arg before the map arg

; workflows
(defstruct executable-wf :pipe :source :sink)

(defstruct workflow :using :reader :writer :namespace :javahelper :inputFields :outputFields)

; add tag for multimethods?
; these are default workflows
(def common-wf-fields {:using identity :reader read-string :writer pr-str :inputFields ["line"] :outputFields ["data"]})
(def each (merge common-wf-fields {:javahelper each-j}))
(def groupBy (merge common-wf-fields {:groupby (fn [x] 1) :javahelper groupBy-j :outputFields ["key", "clojurecode"]}))
(def everyGroup (merge common-wf-fields {:init (fn [] [""]) :using (fn [acc next-line] [(str (first acc) next-line)]) :javahelper everyGroup-j}))
(def c-filter (merge common-wf-fields {:using (fn [x] true) :javahelper c-filter-j}))
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
; joins
;(defstruct join-s :join-wfs :on :join-fn :to :javahelper :reader :writer :numOutFields)
(def join (merge common-wf-fields {:javahelper join-j :groupFields [] :wfs [] :wftype :join}))

(def op-lookup {:each each :groupBy groupBy :everygroup everyGroup :filter c-filter :join join})

(defn cascading-ize [prev-or-name step fnNsName]
  (let [[opType operations] step
	wf-struct (merge (op-lookup opType) (assoc operations :namespace fnNsName))] 
    ((:javahelper wf-struct) prev-or-name wf-struct)))








(ns org.parsimonygroup.workflow-structs
  (:use org.parsimonygroup.java-interop)
  (:import [cascading.scheme TextLine]
	   [cascading.tap Hfs]))

; note for structs
;    I can call merge, except struct has to be before the map
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
; workflows
(defstruct executable-wf :pipe :tap :sink)

(defstruct workflow :using :reader :writer :namespace :javahelper :inputFields :outputFields)

; add tag for multimethods?
; these are default workflows
(def common-wf-fields {:using identity :reader read-string :writer pr-str :inputFields ["line"] :outputFields ["data"]})
(def each (merge common-wf-fields {:javahelper each-j}))
(def groupBy (merge common-wf-fields {:groupby (fn [x] 1) :javahelper groupBy-j :outputFields ["key", "clojurecode"]}))
(def everyGroup (merge common-wf-fields {:init (fn [] [""]) :using (fn [acc next-line] [(str (first acc) next-line)]) :javahelper everyGroup-j}))
(def c-filter (merge common-wf-fields {:using (fn [x] true) :javahelper c-filter-j}))



(def op-lookup {:each each :groupBy groupBy :everygroup everyGroup :filter c-filter})

(defn cascading-ize [prev step fnNsName]
  (let [[opType operations] step
	wf-struct (merge (op-lookup opType) (assoc operations :namespace fnNsName))] 
    ((:javahelper wf-struct) prev wf-struct)))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
; config shit

(defstruct wf-config :tap :sink :name)

(defn default-tap [path] (Hfs. (TextLine.) path))
(def default-config (struct-map wf-config :tap default-tap :sink default-tap))

(defn mk-config [pline]
    (merge default-config (:config pline)))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
; joins
;(defstruct join-s :join-wfs :on :join-fn :to :javahelper :reader :writer :numOutFields)
(def join-default (struct-map workflow :using (fn [x y] x) :reader read-string :writer pr-str :javahelper join-j :to default-tap :numOutFields 4))




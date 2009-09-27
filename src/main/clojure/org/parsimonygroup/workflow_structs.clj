(ns org.parsimonygroup.workflow-structs
  (:use org.parsimonygroup.java-interop)
  (:import [cascading.scheme TextLine]
	   [cascading.tap Hfs]))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
; config shit

;use java's random generator
(def random (java.util.Random.))
;define characters list to use to generate string
(def chars (map char (concat (range 48 58) (range 66 92) (range 97 123))))
;generates 1 random character
(defn random-char [] (nth chars (.nextInt random (count chars))))
; generates random string of length characters
(defn random-string [length] (apply str (take length (repeatedly random-char))))

(defstruct wf-config :tap :sink :name)

(defn default-tap [path] (Hfs. (TextLine.) path))
(def default-config (struct-map wf-config :tap default-tap :sink default-tap :name #(random-string %)))

(defn mk-config [pline]
    (merge default-config (:config pline)))


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
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
; joins
;(defstruct join-s :join-wfs :on :join-fn :to :javahelper :reader :writer :numOutFields)
(def join (merge common-wf-fields {:javahelper join-j :groupFields [] :wfs [] :wftype :join}))

(def op-lookup {:each each :groupBy groupBy :everygroup everyGroup :filter c-filter :join join})

(defn cascading-ize [prev-or-name step fnNsName]
  (let [[opType operations] step
	wf-struct (merge (op-lookup opType) (assoc operations :namespace fnNsName))] 
    ((:javahelper wf-struct) prev-or-name wf-struct)))








(ns org.parsimonygroup.workflow-structs
  (:use org.parsimonygroup.java-interop)
  (:import [cascading.scheme TextLine]
	   [cascading.tap Hfs]))

; note for structs
;    I can call merge, except struct has to be before the map
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
; workflows
(defstruct executable-wf :pipe :tap :sink)

(defstruct workflow :using :reader :writer :namespace :javahelper)

; add tag for multimethods?
; these are default workflows
(def each (struct-map workflow :using identity :reader read-string :writer pr-str :javahelper each-j))
(def groupBy (struct-map workflow :using identity :reader read-string :writer pr-str :groupby (fn [x] 1) :javahelper groupBy-j))
(def groupBy2 (struct-map workflow :using identity :reader read-string :writer pr-str :javahelper groupBy2-j))
(def everyGroup (struct-map workflow :using identity :reader read-string :writer pr-str :init (fn [] {}) :javahelper everyGroup-j))
(def c-filter (struct-map workflow :using (fn [x] true) :reader read-string :writer pr-str :javahelper c-filter-j))
(def transformation (struct-map workflow :using identity :reader read-string :writer pr-str :javahelper transformation-j))


(def op-lookup {:each each :groupBy groupBy :groupBy2 groupBy2 :everygroup everyGroup :filter c-filter :transformation transformation})

(defn cascading-ize [prev step fnNsName]
  (let [[opType operations] step
	wf-struct (merge (op-lookup opType) (assoc operations :namespace fnNsName))] 
    ((:javahelper wf-struct) prev wf-struct)))

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

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
; joins
;(defstruct join-s :join-wfs :on :join-fn :to :javahelper :reader :writer :numOutFields)
(def join-default (struct-map workflow :using (fn [x y] x) :reader read-string :writer pr-str :javahelper join-j :to default-tap :numOutFields 4))




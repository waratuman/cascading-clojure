(ns org.parsimonygroup.cascading
  (:import 
   [cascading.cascade Cascade CascadeConnector]
   [cascading.flow Flow FlowConnector FlowProcess MultiMapReducePlanner]
   [cascading.pipe Pipe]
   [cascading.tap Tap]
   [org.apache.hadoop.mapred JobConf]
   [java.util Map Properties])
  (:use org.danlarkin.json)
  (:use [org.parsimonygroup.workflow-structs :only (default-tap executable-wf cascading-ize mk-config)]))

(defn mk-pipe [prev fnNsName fns]
  (let [f (first fns)]
    (if (nil? f)
      prev
      (mk-pipe (cascading-ize prev f fnNsName) fnNsName (rest fns)))))

(defn -retrieveFn [namespace sym]
  (let [nsSym (symbol namespace)]
    (apply use :reloadall [nsSym])
    ((ns-resolve nsSym (symbol sym)))))

(defn configure-properties [mainCls]
  (let [prop (Properties.)
        jobConf (JobConf.)]
    (. prop load (.. (class *ns*) getClassLoader (getResourceAsStream "config.properties")))
    (Flow/setStopJobsOnExit prop false)
    (FlowConnector/setApplicationJarClass prop mainCls)
    (. jobConf set "mapred.task.timeout" "600000000")
					; (. jobConf set "mapred.child.java.opts" "-Xmx768m")
					; (. jobConf set "mapred.tasktracker.map.tasks.maximum" "1")
					; (. jobConf set "mapred.tasktracker.reduce.tasks.maximum" "1")
					; (Flow/setStopJobsOnExit prop false)(. jobConf set "fs.default.name" "file:///")(. jobConf set "mapred.compress.map.output" "true")
    (MultiMapReducePlanner/setJobConf prop jobConf)
    prop))

(defn uuid [] (.toString (java.util.UUID/randomUUID)))

(defn flow
  ([source-tap sink-tap pipe]
     (.connect (FlowConnector.) source-tap sink-tap pipe))
  ([properties source-tap sink-tap pipe]
     (.connect (FlowConnector. properties) source-tap sink-tap pipe)))

(defn copy-flow
  "uses random flow name that cascading creates because: all flow names must be unique, found duplicate: copy"
  [source-tap sink-tap]
  (flow source-tap sink-tap (Pipe. (uuid))))

(defn cascade 
  "note the into-array trickery to call the java variadic method"
  [& flows]
  (.connect (CascadeConnector.) (into-array Flow flows)))

(defn mk-workflow 
  "this makes a single workflow, with keys of :pipe :sink :tap"
  [fnNs inPath outPath pline]
  (let [steps (:operations pline) 
        config (mk-config pline)
	unique-name (uuid)]
    (struct-map executable-wf 
      :pipe (mk-pipe (Pipe. unique-name) fnNs steps) 
      :tap ((:tap config) inPath) 
      :sink ((:sink config) outPath) 
      :name unique-name)))

(defn execute 
  "executes a flow or cascade and blocks until completion."
  [x] (doto x .start .complete) x)

(defn cascading [{:keys [input output mainCls pipeline fnNsName]}]
  (let [prop (configure-properties mainCls)
	wf (mk-workflow fnNsName input output (-retrieveFn fnNsName pipeline))]
    (execute (flow prop (:tap wf) (:sink wf) (:pipe wf)))))

;; refactor this to multimethods
(defn mk-join-workflow 
  "takes in a join-s struct, inputs (which should match number of wfs to join), output loc"
  [fnNs input output join-pline]
					; validate?
  (let [mk-single-wf (partial mk-workflow fnNs)
	in-out-pipe-triples (partition 3 (interleave input (repeat output) (:join-wfs join-pline)))
	wfs (map #(apply mk-single-wf %) in-out-pipe-triples)
	pipes (map #(:pipe %) wfs)
	taps (into {} (map (fn [x] [(:name x) (:tap x)]) wfs)) ; I need <name, tap>
	join-pipe ((:javahelper join-pline) fnNs join-pline pipes (:numOutFields join-pline))]
    (struct-map executable-wf :pipe join-pipe :tap taps :sink ((:to join-pline) output))))

(defn cascading-join [{:keys [input output mainCls pipeline fnNsName]}]
  (cond (not (seq? input)) (throw (IllegalArgumentException. "need at least 2 inputs to join, this should match the number of workflows in your join definition"))
	:else (let [join-pipeline (-retrieveFn fnNsName pipeline)
		    prop (configure-properties mainCls)
		    wf (mk-join-workflow fnNsName input output join-pipeline)]
		(execute (flow prop (:tap wf) (:sink wf) (:pipe wf))))))

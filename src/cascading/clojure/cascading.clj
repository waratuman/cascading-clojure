(ns cascading.clojure.cascading
  (:import 
   [cascading.cascade Cascade CascadeConnector Cascades]
   [cascading.flow Flow FlowConnector FlowListener 
    FlowProcess MultiMapReducePlanner]
   [cascading.pipe Pipe]
   [cascading.tap Tap]
   [org.apache.hadoop.mapred JobConf]
   [java.util Map Properties])
  (:use cascading.clojure.taps)
  (:use cascading.clojure.pipes))

;;TODO: we may want to split the flow and cascade metaphors and dsl stuff from some of the hadoop/cascading plumbing like retrieve-fn, configure-properties, etc.


;;TODO: we may want to change to set jar path so we don't need to crete main class.
(defn configure-properties 
"http://www.cascading.org/javadoc/cascading/flow/FlowConnector.html

Most applications will need to call setApplicationJarClass(java.util.Map, Class) or setApplicationJarPath(java.util.Map, String)  so that the correct application jar file is passed through to all child processes. The Class or path must reference the custom application jar, not a Cascading library class or jar. The easiest thing to do is give setApplicationJarClass the Class with your static main function and let Cascading figure out which jar to use." 

[main-class]
  (let [prop (Properties.)]
    (when-let [config (.. (class *ns*) (getClassLoader)
                          (getResourceAsStream "config.properties"))]
      (.load prop config))
    (FlowConnector/setApplicationJarClass prop main-class)
    (MultiMapReducePlanner/setJobConf prop (JobConf.)) prop))

(defn flow
  ([source-tap sink-tap pipe]
     (.connect (FlowConnector.) source-tap sink-tap pipe))
  ([properties source-tap sink-tap pipe]
     (.connect (FlowConnector. properties) source-tap sink-tap pipe)))

(defn execute 
  "executes a flow or cascade and blocks until completion. writes dot file on planner exception."
  [x] 
  (try
     (doto x .start .complete)
   (catch cascading.flow.PlannerException e 
     (do 
       (.writeDOT e "exception.dot")
       (throw (RuntimeException. "see exception.dot file for visualization of plan" e))))))

(defn copy-flow
  "uses random flow name that cascading creates because: all flow names must be unique, found duplicate: copy"
  [source-tap sink-tap]
  (flow source-tap sink-tap (Pipe. (uuid))))

(defn cascade
  "note the into-array trickery to call the java variadic method"
  [& flows]
  (.connect (CascadeConnector.) (into-array Flow flows)))

(defn wf-type [props pipeline-ns make-tap input output pipeline]
   (:wftype pipeline))

(defmulti mk-workflow wf-type)

(defmethod mk-workflow :join
 [props pipeline-ns make-tap input output join-pipeline]
  (let [clj-wfs (:wfs join-pipeline)]
    (cond (not (= 2 (count clj-wfs))) 
	  (throw (IllegalArgumentException. 
		  "can only take 2 wfs for join for now"))
	  (not (= (count clj-wfs) (count input))) 
	  (throw (IllegalArgumentException. 
		  (str "there are " (count clj-wfs) " workflows and " 
		       (count input) " inputs, these counts needs to match")))
	  :otherwise
	  (let [pipes (map #(mk-pipe pipeline-ns %) clj-wfs)
		taps (taps-map pipes (map make-tap input))
		join-pipe (mk-pipe "join-wf" pipeline-ns 
				   {:join (merge join-pipeline
						 {:pipes pipes})})]
	    (flow props taps (make-tap output) join-pipe)))))

(defmethod mk-workflow :default
  [props pipeline-ns make-tap in-path out-path pipeline]
    (flow props 
	  (make-tap in-path) 
	  (make-tap out-path) 
	  (mk-pipe pipeline-ns pipeline)))

(defn var-symbols 
  "get the namespace and name symbols from the var metadata:

   http://clojure.org/special_forms
   http://clojure.org/cheatsheet
   http://stackoverflow.com/questions/1175920/explain-clojure-symbols"
  [x]
  (let [m (meta x)]
    [(symbol (str (:ns m))) (symbol (str (:name m)))]))

(defn retrieve-fn 
  "get a fn instance from namespace and name symbols.  note that we must reloadall in order to be have the namespace available for interning in processes where the ns has not been loaded yet."
  [namespace sym]
  (apply use :reloadall [namespace])
  @(intern namespace sym))

;;TODO: workflow can be merged with mk-workflow, into a single coherent workflow creation system.

;;TODO: 
;-need to add the ability to pass main-class via code.
;-the first fns expect pipeline as a var and the second map fn signature expects pipeline as a fully qualified string.
(defn workflow 
  "in or out can be local lifesystem, hdfs, or s3.  if you do not specify the dir as relative to s3n, or local filesystem, the path is assumed to be on hdfs."
  ([in-path out-path #^Var pipeline]
   (let [[f-ns f-name] (var-symbols pipeline)]
     (mk-workflow 
      (Properties.) (str f-ns) default-tap in-path out-path (retrieve-fn f-ns f-name))))
 ([make-tap in-path out-path #^Var pipeline]
   (let [[f-ns f-name] (var-symbols pipeline)]
     (mk-workflow 
      (Properties.) (str f-ns) make-tap in-path out-path (retrieve-fn f-ns f-name))))
  ([{:keys [input output main-class pipeline]}]
     (let [[pipeline-ns pipeline-sym] (.split pipeline "/")
	   props (configure-properties main-class)]
       (mk-workflow props pipeline-ns default-tap input output 
;;TODO: note that we apply the fn right after retrieving it. this is the "wrap in a fn to avoid serialization issue" fix.	
	    ((retrieve-fn (symbol pipeline-ns) (symbol pipeline-sym)))))))

;;TODO: api for comp'ing workflows with copy workflow into cascades.

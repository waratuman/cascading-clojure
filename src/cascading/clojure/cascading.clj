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

(defn retrieve-fn [namespace sym]
  (let [ns-sym (symbol namespace)]
    (apply use :reloadall [ns-sym])
    ((ns-resolve ns-sym (symbol sym)))))

(defn configure-properties [main-class]
  (let [prop (Properties.)]
    (when-let [config (.. (class *ns*) (getClassLoader)
                          (getResourceAsStream "config.properties"))]
      (.load prop config))
    ;; (Flow/setStopJobsOnExit prop false)
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

;;TODO: workflow can be merged with mk-workflow, into a single coherent workflow creation system.
(defn workflow 
  ([pipeline-ns in-path out-path pipeline]
     (mk-workflow 
      (Properties.) pipeline-ns default-tap in-path out-path pipeline))
 ([pipeline-ns make-tap in-path out-path pipeline]
     (mk-workflow 
      (Properties.) pipeline-ns make-tap in-path out-path pipeline))
  ([{:keys [input output main-class pipeline]}]
     (let [[pipeline-ns pipeline-sym] (.split pipeline "/")
	   props (configure-properties main-class)]
       (mk-workflow props pipeline-ns default-tap input output 
		    (retrieve-fn pipeline-ns pipeline-sym))))) 
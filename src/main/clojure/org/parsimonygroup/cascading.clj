(ns org.parsimonygroup.cascading
  (:import 
   [cascading.cascade Cascade CascadeConnector Cascades]
   [cascading.flow Flow FlowConnector FlowProcess MultiMapReducePlanner]
   [cascading.pipe Pipe]
   [cascading.tap Tap]
   [org.apache.hadoop.mapred JobConf]
   [java.util Map Properties])
  (:use [org.parsimonygroup.workflow-structs :only (executable-wf cascading-ize mk-config)]))

(defn mk-pipe [prev-or-name pipeline-ns fns]
  (if-let [f (first fns)]
    (mk-pipe (cascading-ize prev-or-name f pipeline-ns) pipeline-ns (rest fns))
    prev-or-name))

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

(defn uuid [] (.toString (java.util.UUID/randomUUID)))

(defn copy-flow
  "uses random flow name that cascading creates because: all flow names must be unique, found duplicate: copy"
  [source-tap sink-tap]
  (.connect (FlowConnector.) source-tap sink-tap (Pipe. (uuid))))

(defn mk-cascade 
  "note the into-array trickery to call the java variadic method"
  [& flows]
  (. (CascadeConnector.) connect (into-array Flow flows)))

(defn wf-type [pipeline-ns input output pipeline]
  (:wftype pipeline))
(defmulti mk-workflow wf-type)

(defmethod mk-workflow :join
  [pipeline-ns input output join-pipeline]
  (let [clj-wfs (:wfs join-pipeline)]
    (cond (not (= 2 (count clj-wfs))) (throw (IllegalArgumentException. "can only take 2 wfs for join for now"))
	  (not (= (count clj-wfs) (count input))) (throw (IllegalArgumentException. (str "there are " (count clj-wfs) " workflows and " (count input) " inputs, these counts needs to match")))

	  :otherwise
	  (let [mk-single-wf (partial mk-workflow pipeline-ns)
		in-out-pipe-triples (partition 3 (interleave input (repeat output)
							     clj-wfs))
		wfs (map #(apply mk-single-wf %) in-out-pipe-triples)
		pipes (map :pipe wfs)
		pipes-arr (into-array Pipe pipes)
		taps (Cascades/tapsMap pipes-arr (into-array Tap (map :tap wfs)))
		config (mk-config join-pipeline)
		join-pipe (mk-pipe "join-wf"
				   pipeline-ns {:join (merge join-pipeline {:pipes pipes})})]
	    (struct-map executable-wf :pipe join-pipe :tap taps
			:sink ((:sink config) output))))))

(defmethod mk-workflow :default
  [pipeline-ns in-path out-path pipeline]
  (let [steps (:operations pipeline)
        config (mk-config pipeline)
	gen-name ((:name config) 6)]
    (struct-map executable-wf 
      :pipe (mk-pipe gen-name pipeline-ns steps)
      :name gen-name
      :tap ((:tap config) in-path) :sink ((:sink config) out-path))))

(defn run-workflow [wf main-class]
  (try
   (let [prop (configure-properties main-class)
	 flowConnector (FlowConnector. prop)
	 flow (.. flowConnector (connect (:tap wf) (:sink wf) (:pipe wf)))]
     (.. flow complete))
   (catch cascading.flow.PlannerException e 
     (do 
       (.writeDOT e "exception.dot")
       (throw (RuntimeException. "see exception.dot file for visualization of plan" e))))))

					; pull out fields to read and write?
(defn cascading [{:keys [input output mainCls pipeline fnNsName]}]
	(let [[pipeline-ns pipeline-sym] (.split pipeline "/")]
    (run-workflow (mk-workflow pipeline-ns input output (retrieve-fn pipeline-ns pipeline-sym)) mainCls)))

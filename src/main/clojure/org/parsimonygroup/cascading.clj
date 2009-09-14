(ns org.parsimonygroup.cascading
  (:import [cascading.cascade Cascade CascadeConnector]
    [cascading.flow Flow FlowConnector FlowProcess MultiMapReducePlanner]
    [cascading.pipe Pipe]
    [org.apache.hadoop.mapred JobConf]
    [java.util Map Properties])
  (:use org.danlarkin.json)
  (:use [org.parsimonygroup.workflow-structs :only (executable-wf cascading-ize mk-config)]))

(defn mk-pipe [prev pipeline-ns fns]
    (let [f (first fns)]
      (if (nil? f)
        prev
        (mk-pipe (cascading-ize prev f pipeline-ns) pipeline-ns (rest fns)))))

(defn- retrieve-fn [namespace sym]
    (let [ns-sym (symbol (or namespace (first (.split sym "/"))))]
      (apply use :reloadall [ns-sym])
      ;; TODO: once we store pipelines in vars instead of functions,
      ;; remove one layer of parens here:
      ((ns-resolve ns-sym (symbol sym)))))

(defn pipe-with-name [name] (Pipe. name))

(defn configure-properties [main-class]
  (let [prop (Properties.)
        jobConf (JobConf.)]
    ;; (.load prop (.. (class *ns*) getClassLoader
    ;;                 (getResourceAsStream "config.properties")))
    ;; (Flow/setStopJobsOnExit prop false)
    (FlowConnector/setApplicationJarClass prop main-class)
    (.set jobConf "mapred.task.timeout" "600000000")
    ;; (.set jobConf "mapred.child.java.opts" "-Xmx768m")
    ;; (.set jobConf "mapred.tasktracker.map.tasks.maximum" "1")
    ;; (.set jobConf "mapred.tasktracker.reduce.tasks.maximum" "1")

    ;; (Flow/setStopJobsOnExit prop false)
    ;; (.set jobConf "fs.default.name" "file:///")
    ;; (.set jobConf "mapred.compress.map.output" "true")
    (MultiMapReducePlanner/setJobConf prop jobConf)
    prop))

(defn mk-workflow [pipeline-ns in-path out-path pipeline]
  "this makes a single workflow, with keys of :pipe :sink :tap"
  (let [steps (:operations pipeline)
        config (mk-config pipeline)
        gen-name ((:name config) 6)]
    (struct-map executable-wf :pipe (mk-pipe (pipe-with-name gen-name)
                                             pipeline-ns steps)
                :tap ((:tap config) in-path) :sink ((:sink config) out-path)
                :name gen-name)))

(defn run-workflow [wf main-class]
  (let [prop (configure-properties main-class)
        flowConnector (FlowConnector. prop)]
    (.. flowConnector (connect (:tap wf) (:sink wf) (:pipe wf)) complete)))

; pull out fields to read and write?
(defn cascading [{:keys [input output main-class pipeline pipeline-ns]}]
  (run-workflow (mk-workflow pipeline-ns input output
                             (retrieve-fn pipeline-ns pipeline)) main-class))

;; refactor this to multimethods
(defn mk-join-workflow [pipeline-ns input output join-pipeline]
  "takes in a join-s struct, inputs (which should match number of wfs to join), output loc"
  ; validate?
  (let [mk-single-wf (partial mk-workflow pipeline-ns)
        in-out-pipe-triples (partition 3 (interleave input (repeat output)
                                                     (:join-wfs join-pipeline)))
        wfs (map #(apply mk-single-wf %) in-out-pipe-triples)
        pipes (map #(:pipe %) wfs)
        taps (into {} (map (fn [x] [(:name x) (:tap x)])
                           wfs)) ; I need <name, tap>
        join-pipe ((:javahelper join-pipeline) pipeline-ns
                   join-pipeline pipes (:numOutFields join-pipeline))]
    (struct-map executable-wf :pipe join-pipe :tap taps
                :sink ((:to join-pipeline) output))))

(defn cascading-join [{:keys [input output main-class pipeline pipeline-ns]}]
  (when-not (seq? input)
    (throw (IllegalArgumentException.
            (str "need at least 2 inputs to join, this should match the "
                 "number of workflows in your join definition"))))
  (let [join-pipeline (retrieve-fn pipeline-ns pipeline)]
    (run-workflow (mk-join-workflow pipeline-ns input output join-pipeline)
                  main-class)))

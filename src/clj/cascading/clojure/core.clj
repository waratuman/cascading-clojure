(ns cascading.clojure.core
  (:refer-clojure :exclude (count first filter mapcat map))
  (:import (cascading.tap Lfs)
           (cascading.pipe Pipe)
           (cascading.flow Flow
                           FlowConnector)
           (cascading.tuple Fields)
           (cascading.scheme TextLine)
           (cascading.clojure MapOperation)))


(defn- uuid []
  (str (java.util.UUID/randomUUID)))

(defn fields [& args]
  (Fields. (into-array Comparable args)))

(def all-fields
     Fields/ALL)

(defn text-line-scheme
  ([] (TextLine.))
  ([field1] (TextLine. (fields field1) (fields field1)))
  ([field1 field2] (TextLine. (fields field1 field2) (fields field1 field2))))

(defn lfs-tap [scheme path]
  (Lfs. scheme path))

(defn pipe
  ([] (pipe (uuid)))
  ([name] (Pipe. name)))

(defn map
  ([previous-pipe fn]
     (MapOperation/pipe previous-pipe fn))
  ([previous-pipe in-fields fn]
     (let [flds (cond (= Fields (class in-fields)) in-fields
                      :else (apply fields in-fields))]
       (MapOperation/pipe previous-pipe flds fn)))
  ([previous-pipe in-fields fn out-fields]
     (let [inflds (cond (= Fields (class in-fields)) in-fields
                      :else (apply fields in-fields))
           outflds (cond (= Fields (class out-fields)) out-fields
                      :else (apply fields out-fields))]
       (MapOperation/pipe previous-pipe inflds fn outflds))))

(defn flow [sources sinks pipes]
  (let [prop (java.util.Properties.)]
    (.setProperty prop "mapred.used.genericoptionsparser" "true")
    (.setProperty prop "cascading.flow.job.pollinginterval" "100")
    (let [flow-connector (FlowConnector. prop)]
      (try (.connect flow-connector sources sinks pipes)
            (catch cascading.flow.PlannerException e
              (.writeDOT e "exception.dot")
              (throw (RuntimeException. "Unable to build flow.")))))))

(defn exec [flow]
  (doto flow .start .complete))

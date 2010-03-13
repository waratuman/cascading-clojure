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

(defn text-line-scheme
  ([] (TextLine.))
  ([field1] (TextLine. (fields field1) (fields field1)))
  ([field1 field2] (TextLine. (fields field1 field2) (fields field1 field2))))

(defn lfs-tap [scheme path]
  (Lfs. scheme path))

(defn pipe
  ([] (pipe (uuid)))
  ([name] (Pipe. name)))

(defn map [previous-pipe fn]
  (MapOperation/pipe previous-pipe fn))

(defn flow [sources sinks pipes]
  (let [prop (java.util.Properties.)
        flow-connector (FlowConnector. prop)]
    (try (.connect flow-connector sources sinks pipes)
            (catch cascading.flow.PlannerException e
              (.writeDOT e "exception.dot")
              (throw (RuntimeException. "Unable to build flow."))))))

(defn exec [flow]
  (doto flow .start .complete))

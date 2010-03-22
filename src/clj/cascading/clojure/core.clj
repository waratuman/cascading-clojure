(ns cascading.clojure.core
  (:refer-clojure :exclude (filter mapcat map))
  (:import (cascading.tap Hfs Lfs)
           (cascading.pipe Pipe Each)
           (cascading.operation Identity)
           (cascading.flow Flow
                           FlowConnector)
           (cascading.tuple Fields)
           (cascading.scheme TextLine)
           (cascading.clojure MapOperation
                              FilterOperation)
           (cascading.clojure.scheme JSONMapLineFile)))

(defn- uuid []
  (str (java.util.UUID/randomUUID)))

(defn fields [& args]
  "Returns a Fields object when given a sequence of fields names. The
   field names must be comparable.
   eg.  (fields \"a\" \"b\") ; <Fields 'a', 'b'>
        (fields [\"a\" \"b\" \"c\"]) ; <Fields 'a', 'b', 'c'>
        (fileds all-fields) ; <Fields/ALL>
  "
  (cond (and (= 1 (count args))
             (coll? (first args)))
        (Fields. (into-array Comparable (first args)))
        (and (= 1 (count args))
             (= Fields (class (first args))))
        (first args)
        :else
        (Fields. (into-array Comparable args))))

(def all-fields
     Fields/ALL)

(defn text-line-scheme
  ([] (TextLine.))
  ([field1] (TextLine. (fields field1) (fields field1)))
  ([field1 field2] (TextLine. (fields field1 field2) (fields field1 field2))))

(defn json-map-line-scheme [map-fields]
  "A scheme that wraps a JSON map per line of the input file. For
   example, `(json-map-line-scheme [\"a\" \"b\" \"c\"])` will map to
   the line \"{\"a\":1,\"b\":2,\"c\":3}\".
   eg. (json-map-line-scheme [\"a\" \"b\" \"c\"])"
  (JSONMapLineFile. (fields map-fields)))

(defn tap [scheme path]
  (Hfs. scheme path))

(defn lfs-tap [scheme path]
  (Lfs. scheme path))

(defn pipe
  ([] (pipe (uuid)))
  ([name] (Pipe. name)))

(defn map
  ([previous-pipe fn]
     (MapOperation/pipe previous-pipe fn))
  ([previous-pipe in-fields-or-fn fn-or-arg-fields]
     (if (fn? fn-or-arg-fields)
       (MapOperation/pipe previous-pipe (fields in-fields-or-fn) fn-or-arg-fields)
       (MapOperation/pipe previous-pipe in-fields-or-fn (fields fn-or-arg-fields))))
  ([previous-pipe in-fields fn out-fields]
     (MapOperation/pipe previous-pipe (fields in-fields) fn (fields out-fields))))

(defn select [in-pipe fields-to-keep]
  (map in-pipe fields-to-keep (fn [x] x)))

(defn filter [previous-pipe fn]
  (FilterOperation/pipe previous-pipe fn))

(defn flow [sources sinks pipes]
  (let [prop (java.util.Properties.)]
    (.setProperty prop "mapred.used.genericoptionsparser" "true")
    (.setProperty prop "cascading.flow.job.pollinginterval" "100")
    (let [flow-connector (FlowConnector. prop)]
      (try (.connect flow-connector (uuid) sources sinks pipes)
            (catch cascading.flow.PlannerException e
              (.writeDOT e "exception.dot")
              (throw (RuntimeException. "Unable to build flow.")))))))

(defn exec [flow]
  (doto flow .start .complete))

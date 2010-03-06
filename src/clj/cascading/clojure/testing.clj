(ns cascading.clojure.testing
  (:use clojure.test
        clojure.contrib.java-utils
        cascading.clojure.io)
  (:import (cascading.tuple Fields Tuple TupleEntry TupleEntryCollector)
           (cascading.pipe Pipe)
           (cascading.operation ConcreteCall)
           (cascading.flow FlowProcess)
           (cascading.clojure Util ClojureMap)
           (clojure.lang IPersistentCollection))
  (:require (cascading.clojure [api :as c])))

(defn- roundtrip [obj]
  (cascading.util.Util/deserializeBase64
    (cascading.util.Util/serializeBase64 obj)))

(defn invoke-filter [fil coll]
  (let [fil     (roundtrip fil)
        op-call (ConcreteCall.)
        fp-null FlowProcess/NULL]
    (.setArguments op-call (TupleEntry. (Util/coerceToTuple coll)))
    (.prepare fil fp-null op-call)
    (let [rem (.isRemove fil fp-null op-call)]
      (.cleanup fil fp-null op-call)
      rem)))

(defn- output-collector [out-atom]
  (proxy [TupleEntryCollector] []
    (add [tuple]
      (swap! out-atom conj (Util/coerceFromTuple tuple)))))

(defn- op-call []
  (let [out-atom (atom [])]
    (proxy [ConcreteCall IPersistentCollection] []
      (getOutputCollector []
        (output-collector out-atom))
      (seq []
        (seq @out-atom)))))

(defn- op-call-results [func-call]
  (.seq func-call))

(defn invoke-function [m coll]
  (let [m         (roundtrip m)
        func-call (op-call)
        fp-null   FlowProcess/NULL]
    (.setArguments func-call (TupleEntry. (Util/coerceToTuple coll)))
    (.prepare m fp-null func-call)
    (.operate m fp-null func-call)
    (.cleanup m fp-null func-call)
    (op-call-results func-call)))

(defn invoke-aggregator [a colls]
  (let [a       (roundtrip a)
        ag-call (op-call)
        fp-null FlowProcess/NULL]
    (.prepare a fp-null ag-call)
    (.start a fp-null ag-call)
    (doseq [coll colls]
      (.setArguments ag-call (TupleEntry. (Util/coerceToTuple coll)))
      (.aggregate a fp-null ag-call))
    (.complete a fp-null ag-call)
    (.cleanup  a fp-null ag-call)
    (op-call-results ag-call)))

(defn invoke-buffer [a colls]
  (let [a       (roundtrip a)
        ag-call (op-call)
        fp-null FlowProcess/NULL]
    (.prepare a fp-null ag-call)
    (.setArgumentsIterator ag-call
      (.iterator (map #(TupleEntry. (Util/coerceToTuple %)) colls)))
    (.operate a fp-null ag-call)
    (.cleanup a fp-null ag-call)
    (op-call-results ag-call)))

(defn- deserialize-tuple [line]
  (read-string line))

(defn- serialize-tuple
  {:fields "line"}
  [tuple]
  (pr-str tuple))

(defn- serialize-vals
  {:fields "line"}
  [& vals]
  (pr-str (vec vals)))

(defn- line-sink-seq [tuple-entry-iterator]
  (map #(read-string (first (.getTuple %)))
       (iterator-seq tuple-entry-iterator)))

(defn- mash [f coll]
  (into {} (map f coll)))

(defn in-pipe [in-label in-fields]
  (-> (c/pipe in-label)
      (c/map #'deserialize-tuple :fn> in-fields)))

(defn in-pipes [fields-spec]
  (if (not (map? fields-spec))
    (in-pipe "in" fields-spec)
    (mash (fn [[in-label one-in-fields]]
      [in-label (in-pipe in-label one-in-fields)])
    fields-spec)))

(defn in-tuples [tuples-spec]
  (if (map? tuples-spec)
    tuples-spec
    {"in" tuples-spec}))

(defn test-flow [in-pipes-spec in-tuples-spec assembler expected-out-tuples]
  (with-log-level :warn
    (with-tmp-files [source-dir-path (temp-dir  "source")
                     sink-path       (temp-path "sink")]
      (doseq [[in-label in-tuples] in-tuples-spec]
  (write-lines-in source-dir-path in-label
      (map serialize-tuple in-tuples)))
      (let [assembly   (-> in-pipes-spec
                           assembler
                           (c/map #'serialize-vals))
      source-tap-map (mash (fn [[in-label _]]
           [in-label
            (c/lfs-tap (c/text-line "line")
                 (file source-dir-path in-label))])
         in-tuples-spec)
      sink-tap       (c/lfs-tap (c/text-line "line") sink-path)
      flow           (c/flow source-tap-map sink-tap assembly)
      out-tuples     (line-sink-seq (.openSink (c/exec flow)))]
  (is (= expected-out-tuples out-tuples))))))

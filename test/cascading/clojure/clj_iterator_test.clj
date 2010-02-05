(ns cascading.clojure.clj-iterator-test
  (:require [cascading.clojure clj_iterator])
  (:import java.util.Arrays)
  (:import [cascading.tuple Fields Tuple IndexTuple])
  (:import [cascading.pipe.cogroup GroupClosure CoGroupClosure])
  (:import [cascading.flow FlowProcess FlowSession])
  (:import [cascading.flow.hadoop HadoopFlowProcess])
  (:import [org.apache.hadoop.mapred JobConf])
  (:import [cascading.clojure CljIterator]))

(def fake-group-closure
 (GroupClosure. 
		(into-array Fields [(Fields. (into-array String ["a" "b"]))])
		(into-array Fields [(Fields. (into-array String ["a" "b"]))])
		(Tuple. (into-array String ["foo"]))
		(.iterator ["w" "t" "f"])))

(defn fake-flow-process []
  (HadoopFlowProcess. FlowSession/NULL (JobConf.) true))

(def fake-cogroup-closure
 (CoGroupClosure. 
   (fake-flow-process)
   1
		(into-array Fields [(Fields. (into-array String ["a" "b"]))])
		(into-array Fields [(Fields. (into-array String ["a" "b"]))])
		(Tuple. (into-array String ["foo"]))
		(.iterator ["w" "t" "f"])))

(deftest make-group-closure
 (is (= 1 
	(.size fake-group-closure))))

;;requires CoGroupClosure, which can not be instantiated without a HadoopFlowProcess, which in turn seems very difficult to instantiate.
(deftest clj-iterator-test
  (is (not 
       (.hasNext 
	(CljIterator. fake-cogroup-closure
		      read-string
		      pr-str
		      identity
		      identity
		      1)))))
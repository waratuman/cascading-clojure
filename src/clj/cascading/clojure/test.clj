(ns cascading.clojure.test
  (:use clojure.test
        cascading.clojure.io
        cascading.clojure.test)
  (:require (cascading.clojure [core :as c]))
  (:import (cascading.tap Lfs)
           (cascading.scheme SequenceFile)
           cascading.clojure.Util))

(defn test-flow [sources outputs assembly]
  (log-with-level "info"
    (let [source-path (str (temp-dir))
          sink-path (str (temp-dir) "/out")
          in-fields (apply c/fields (keys (first sources)))
          out-fields (apply c/fields (keys (first outputs)))]
      (let [source-tap (Lfs. (SequenceFile. in-fields) source-path)
            collector (.openForWrite source-tap (org.apache.hadoop.mapred.JobConf.))]
        (doall (map (fn [obj]
                      (.add collector (Util/mapToTupleEntry obj)))
                    sources))
        (.close collector))
      (let [source (Lfs. in-fields (str source-path "/part-00000"))
            sink (Lfs. out-fields sink-path)
            flow (c/flow source sink (assembly (c/pipe)))]
        (c/exec flow))
      (let [sink-tap (Lfs. (SequenceFile. out-fields) sink-path)
            collector (.openForRead sink-tap (org.apache.hadoop.mapred.JobConf.))
            results (doall (map #(Util/tupleEntryToMap %)
                                (iterator-seq collector)))]
        (.close collector)
        (is (= outputs results))))))

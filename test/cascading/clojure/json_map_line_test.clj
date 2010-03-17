(ns cascading.clojure.json-map-line-test
  (:use clojure.test
        cascading.clojure.io
        clojure.contrib.java-utils)
  (:require (cascading.clojure [core :as c]))
  (:import cascading.tap.Lfs
           cascading.clojure.Util
           cascading.clojure.scheme.JSONMapLineFile
           (cascading.tuple Tuple TupleEntry)))

(deftest json-map-line-test
  (log-with-level "warn"
    (let [obj {"a" 1 "b" 2 "c" "3" "d" 4}
        source-path (str (temp-dir))
        in-fields (c/fields "a" "b" "c" "d")
        source-tap (Lfs. (JSONMapLineFile. in-fields in-fields) source-path)
        collector (.openForWrite source-tap (org.apache.hadoop.mapred.JobConf.))]
    (.add collector (Util/mapToTupleEntry obj))
    (.close collector)
    (let [collector (.openForRead source-tap (org.apache.hadoop.mapred.JobConf.))
          results (doall (map (fn [x] (Util/tupleEntryToMap x))
                                (iterator-seq collector)))]
      (is (= [obj]
             results))))))


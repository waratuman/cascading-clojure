(ns cascading.clojure.util-test
  (:use clojure.test
        clojure.contrib.java-utils)
  (:require (cascading.clojure [core :as c]))
  (:import cascading.clojure.Util
           (cascading.tuple Tuple TupleEntry)))

(deftest tuple-entry-to-map-test
  (let [tuple-entry (TupleEntry. (c/fields "name" "age")
                                 (Tuple. (into-array ["james" "23"])))]
    (is (= {"name" "james" "age" "23"}
           (Util/tupleEntryToMap tuple-entry)))))

(deftest map-to-tuple-entry-test
  (let [map {"name" "james" "age" "23"}
        tuple-entry (TupleEntry. (c/fields "name" "age")
                                 (Tuple. (into-array ["james" "23"])))]
    (is (= (Util/tupleEntryToMap tuple-entry) 
           (Util/tupleEntryToMap (Util/mapToTupleEntry map))))))

(deftest coll-to-tuple-entries-test
  (let [coll '({"name" "james" "age" "23"}
              {"name" "ben" "age" "32"})]
    (is (= TupleEntry
           (class (first (Util/collectionToTupleEntries coll)))
           (class (second (Util/collectionToTupleEntries coll)))))
    (is (= [{"name" "james" "age" "23"}
            {"name" "ben" "age" "32"}]
           [(Util/tupleEntryToMap (first (Util/collectionToTupleEntries coll)))
            (Util/tupleEntryToMap (last (Util/collectionToTupleEntries coll)))]))))

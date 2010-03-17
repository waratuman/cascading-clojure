(ns cascading.clojure.core-test
  (:use clojure.test
        clojure.contrib.java-utils
        clojure.contrib.duck-streams
        cascading.clojure.io
        cascading.clojure.test)
  (:require (cascading.clojure [core :as c]))
  (:import (cascading.tap Lfs)
           (cascading.pipe Pipe)
           (cascading.tuple Fields)
           (cascading.scheme TextLine)))

(deftest fields-test
  (is (= (Fields. (into-array ["a" "b"]))
         (c/fields "a" "b"))))
 
(deftest text-line-scheme-test
  (is (= (TextLine.)
         (c/text-line-scheme)))
  (is (= (TextLine. (c/fields "a-line") (c/fields "a-line"))
         (c/text-line-scheme "a-line")))
  (is (= (TextLine. (c/fields "3" "4") (c/fields "3" "4"))
         (c/text-line-scheme "3" "4"))))

(deftest lfs-tap-test
  (let [f (.getPath (temp-file))
        scheme (c/text-line-scheme "line")]
    (is (= (c/lfs-tap scheme f)
           (Lfs. scheme f)))))

(deftest pipe-test
   (is (c/pipe))
   (is (= "test-pipe"
          (.getName (Pipe. "test-pipe")))))

(deftest map-test
  (test-flow [{"age" 1} {"age" 2} {"age" 3}]
             [{"age" 2} {"age" 3} {"age" 4}]
             #(c/map % (fn [x] [{"age" (+ 1 (get x "age"))}])))
  (test-flow [{"name" "james" "age" 23} {"name" "jared" "age" 24}]
             [{"name" "JAMES"} {"name" "JARED"}]
             #(c/map %
                     (c/fields "name")
                     (fn [x] [{"name" (.toUpperCase (get x "name"))}])))
  (test-flow [{"name" "james" "age" 23} {"name" "jared" "age" 24}]
             [{"name" "JAMES"} {"name" "JARED"}]
             #(c/map %
                     ["name"]
                     (fn [x] [{"name" (.toUpperCase (get x "name"))}])))
  (test-flow [{"name" "james" "age" 23} {"name" "jared" "age" 24}]
             [{"upper-name" "JAMES"} {"upper-name" "JARED"}]
             #(c/map %
                     (c/fields "name")
                     (fn [x] [{"upper-name" (.toUpperCase (get x "name"))}])
                     (c/fields "upper-name")))
  (test-flow [{"name" "james" "age" 23} {"name" "jared" "age" 24}]
             [{"name" "james" "age" 23 "upper-name" "JAMES"} {"name" "jared" "age" 24 "upper-name" "JARED"}]
             #(c/map %
                     ["name" "age"]
                     (fn [x] 
                       [(assoc x "upper-name" (.toUpperCase (get x "name")))])
                     ["name" "age" "upper-name"])))

(deftest flow-test
  (let [scheme (c/text-line-scheme "line")
        source (c/lfs-tap scheme (.getPath (temp-file)))
        sink (c/lfs-tap scheme (.getPath (temp-dir)))
        pipe (c/pipe)
        flow (c/flow source sink pipe)]
    (is (= sink (.getSink flow)))
    (is (.contains (.values (.getSources flow)) source))))

(deftest exec-test
  (log-with-level :warn
    (let [scheme (c/text-line-scheme "line")
          source-path (.getPath (temp-file))
          source (c/lfs-tap scheme source-path)
          sink-path (str (.getPath (temp-dir)) "/out")
          sink (c/lfs-tap scheme sink-path)
          pipe (c/pipe)
          flow (c/flow source sink pipe)]
      (write-lines (file source-path) ["This is a line!"])
      (c/exec flow)
      (is (= ["This is a line!"]
             (read-lines (str sink-path "/part-00000")))))))

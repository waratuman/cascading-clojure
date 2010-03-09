(ns cascading.clojure.flow-test
  (:use clojure.test
        clojure.contrib.java-utils
        cascading.clojure.testing
        cascading.clojure.io)
  (:import (cascading.tuple Fields)
           (cascading.pipe Pipe)
           (cascading.clojure Util ClojureMap))
  (:require (clj-json [core :as json]))
  (:require [clojure.contrib.duck-streams :as ds])
  (:require [clojure.contrib.java-utils :as ju])
  (:require (cascading.clojure [api :as c])))

(defn uppercase
  {:fn> "upword"}
  [word]
  (.toUpperCase word))

(deftest map-test
  (test-flow
   (in-pipes "word")
   (in-tuples [["foo"] ["bar"]])
   (fn [in] (-> in (c/map #'uppercase)))
   [["FOO"] ["BAR"]]))

(deftest map-without-defaults
  (test-flow
   (in-pipes ["x" "y" "foo"])
   (in-tuples [[2 3 "blah"] [7 3 "blah"]])
   (fn [in] (-> in (c/map #'+ :< ["x" "y"] :fn> "sum" :> "sum")))
   [[5] [10]]))

(defn extract-key
  {:fn> "key"}
  [val]
  (second (re-find #".*\((.*)\).*" val)))

(deftest extract-test
  (test-flow
   (in-pipes ["val" "num"])
   (in-tuples [["foo(bar)bat" 1] ["biz(ban)hat" 2]])
   (fn [in] (-> in (c/map #'extract-key :< "val" :> ["key" "num"])))
   [["bar" 1] ["ban" 2]]))

(def sum (c/agg + 0))

(deftest aggreate-test
  (test-flow
    (in-pipes ["word" "subcount"])
    (in-tuples [["bar" 1] ["bat" 2] ["bar" 3] ["bar" 2] ["bat" 1]])
    (fn [in] (-> in
               (c/group-by "word")
               (c/aggregate #'sum :< "subcount" :fn> "count" :> ["word" "count"])))
    [["bar" 6] ["bat" 3]]))

(defn transform
  {:fn> ["up-name" "inc-age"]}
  [name age]
  [(.toUpperCase name) (inc age)])


(deftest json-map-line-test
  (with-log-level :warn
    (with-tmp-files [source (temp-dir "source")
                     sink   (temp-path "sink")]
      (let [lines [{"name" "foo" "age" 23} {"name" "bar" "age" 14}]]
        (write-lines-in source "source.data" (map json/generate-string
                                                  lines))
        (let [trans (-> (c/pipe "j")
                        (c/map #'transform :< ["name" "age"]))
              flow (c/flow {"j" (c/lfs-tap (c/json-map-line ["name" "age"]) source)}
                           (c/lfs-tap (c/json-map-line ["up-name" "inc-age"])
                                      sink)
                           trans)]
          (c/exec flow)
          (is (= "{\"inc-age\":24,\"up-name\":\"FOO\"}\n{\"inc-age\":15,\"up-name\":\"BAR\"}\n"
                 (ds/slurp* (ju/file sink "part-00000")))))))))



(ns cascading.clojure.flow-test-new
  (:use clojure.test
        clojure.contrib.java-utils
        cascading.clojure.io)
  (:import (cascading.tuple Fields)
           (cascading.pipe Pipe)
           (cascading.clojure Util ClojureMap))
  (:require (cascading.clojure [api :as c])))

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

(defn- test-flow [in-fields-spec in-tuples-spec assembler expected-out-tuples]
  (with-log-level :warn
    (with-tmp-files [source-dir-path (temp-dir  "source")
                     sink-path       (temp-path "sink")]
      (let [multi-source? (map? in-tuples-spec)
            in-fields-map (if multi-source?
                            in-fields-spec
                            {"in" in-fields-spec})
            in-tuples-map (if multi-source?
                            in-tuples-spec
                            {"in" in-tuples-spec})]
        (doseq [[in-label in-tuples] in-tuples-map]
          (write-lines-in source-dir-path in-label
            (map serialize-tuple in-tuples)))
        (let [in-pipes-map  (mash (fn [[in-label in-tuples]]
                                    [in-label
                                       (-> (c/pipe in-label)
                                         (c/map [(in-fields-map in-label)
                                                 #'deserialize-tuple]))])
                                  in-tuples-map)
              assembly   (-> (if multi-source?
                               in-pipes-map
                               (val (first in-pipes-map)))
                           assembler
                           (c/map #'serialize-vals))
              source-tap-map (mash (fn [[in-label _]]
                                     [in-label
                                      (c/lfs-tap (c/text-line "line")
                                        (file source-dir-path in-label))])
                                   in-tuples-map)
              sink-tap       (c/lfs-tap (c/text-line "line") sink-path)
              flow           (c/flow source-tap-map sink-tap assembly)
              out-tuples     (line-sink-seq (.openSink (c/exec flow)))]
          (is (= expected-out-tuples out-tuples)))))))

(defn uppercase
  {:fields "upword"}
  [word]
  (.toUpperCase word))

(deftest map-test
  (test-flow
    "word"
    [["foo"] ["bar"]]
    (fn [in] (-> in (c/map #'uppercase)))
    [["FOO"] ["BAR"]]))

(defn extract-key
  {:fields "key"}
  [val]
  (second (re-find #".*\((.*)\).*" val)))

(deftest extract-test
  (test-flow
    ["val" "num"]
    [["foo(bar)bat" 1] ["biz(ban)hat" 2]]
    (fn [in] (-> in (c/map "val" #'extract-key ["key" "num"])))
    [["bar" 1] ["ban" 2]]))

(deftest inner-join-test
  (test-flow
    {"lhs" ["name" "num"]
     "rhs" ["name" "num"]}
    {"lhs" [["foo" 5] ["bar" 6]]
     "rhs" [["foo" 1] ["bar" 2]]}
    (fn [{lhs "lhs" rhs "rhs"}]
      (-> [lhs rhs]
        (c/inner-join
          ["name" "name"]
          ["name1" "val1" "nam2" "val2"])
        (c/select ["val1" "val2"])))
    [[6 2] [5 1]]))

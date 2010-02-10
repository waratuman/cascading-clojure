(ns cascading.clojure.testing
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

(defn in-pipe [in-label in-fields]
  (-> (c/pipe in-label)
      (c/map [in-fields
	      #'deserialize-tuple])))

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

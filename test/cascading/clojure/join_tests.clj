(ns cascading.clojure.join-tests
  (:use clojure.test)
  (:use cascading.clojure.io)
  (:import (cascading.tuple Fields)
           (cascading.pipe Pipe)
           (cascading.clojure Util ClojureMap))
  (:require (cascading.clojure [api :as c])))

(def get-name (comp :name read-string))

;;TODO: this is some funky shit - surely there is a better way.
(defn as-vector
  [entry]
  (into []
	(.split
	 (second
	  (iterator-seq
	   (.iterator (.getTuple entry))))
	 "\t")))

(deftest map-test
 (with-tmp-files [source (temp-dir "source")
		  sink (temp-path "sink")]
 (let [lines [{:name "foo" :a 1} {:name "bar" :b 2}]
       _ (write-lines-in source "source.data" lines)
       extract (c/map (c/pipe "m") ["name" #'get-name] ["name" "val"])
       map-flow (c/flow
		  {"m" (c/lfs-tap (c/text-line ["val"]) source)}
		  (c/lfs-tap (c/text-line ["name" "val"]) sink)
		  extract)
       out (.openSink (c/exec map-flow))]
   (is (= ["foo" (pr-str {:name "foo" :a 1})]
	  (as-vector (.next out))))
   (is (= ["bar" (pr-str {:name "bar" :b 2})]
	  (as-vector (.next out))))
   (is (not (.hasNext out))))))

;;taps-map requires named pipes to be the same as key names identifying pipes
;;must be a way to encapsulated all this plumbing.
(deftest inner-join-test
  (with-tmp-files [rhs (temp-dir "rhs")
		   lhs (temp-dir "lhs")
		   sink (temp-path "sink")]
    (let [rhs-lines [{:name "foo" :a 1} {:name "bar" :b 2}]
	  lhs-lines [{:name "foo" :a 5} {:name "bar" :b 6}]
	  _ (write-lines-in rhs "rhs.data" rhs-lines)
	  _ (write-lines-in lhs "lhs.data" lhs-lines)
	  lhs-extract (c/map (c/pipe "lhs") ["name" #'get-name] ["name" "val"])
	  rhs-extract (c/map (c/pipe "rhs") ["name" #'get-name] ["name" "val"])
	  join-pipe (c/inner-join
		     [lhs-extract rhs-extract]
		     [["name"] ["name"]]
		     ["name1" "val1" "name2" "val2"])
	  keep-only (c/select join-pipe ["val1" "val2"])
	  join-flow (c/flow
			    {"rhs" (c/lfs-tap (c/text-line ["val"]) rhs)
			     "lhs" (c/lfs-tap (c/text-line ["val"]) lhs)}
			    (c/lfs-tap (c/text-line ["val1" "val2"]) sink)
			    join-pipe)
	  out (.openSink (c/exec join-flow))]
      (is (= [(pr-str {:name "bar" :b 6}) (pr-str {:name "bar" :b 2})]
	     (as-vector (.next out))))
      (is (= [(pr-str {:name "foo" :a 5}) (pr-str {:name "foo" :a 1})]
	     (as-vector (.next out))))
	     (is (not (.hasNext out))))))

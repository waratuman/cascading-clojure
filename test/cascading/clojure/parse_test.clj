(ns cascading.clojure.parse-test
  (:use clojure.test)
  (:import (cascading.tuple Fields))
  (:use cascading.clojure.parse))

(defn example [x] x)

(def obj-array-class (class (into-array Object [])))

(defn extract-obj-array [x]
  (map (fn [e]
	 (if (instance? obj-array-class e)
	   (seq e)
	   e))
       x))

(deftest parse-everything
  (is (= [(fields ["foo"])
	  (fields ["bar"]) 
	  ["cascading.clojure.parse-test" "example"]
	  (fields ["baz"])]
	 (extract-obj-array
	  (parse-args [#'example "foo" :fn> "bar" :> "baz"])))))

(deftest parse-no-input-selectors
  (is (= [Fields/ALL
	  (fields ["bar"]) 
	  ["cascading.clojure.parse-test" "example"]
	  (fields ["baz"])]
	 (extract-obj-array
	  (parse-args [#'example :fn> "bar" :> "baz"])))))

(deftest parse-no-input-or-output-selectors
  (is (= [Fields/ALL
	  (fields ["bar"]) 
	  ["cascading.clojure.parse-test" "example"]
	  Fields/RESULTS]
	 (extract-obj-array
	  (parse-args [#'example :fn> "bar"])))))

(deftest parse-no-input-or-output-or-fn-selectors
  (is (= [Fields/ALL
	  nil
	  ["cascading.clojure.parse-test" "example"]
	  Fields/RESULTS]
	 (extract-obj-array
	  (parse-args [#'example])))))
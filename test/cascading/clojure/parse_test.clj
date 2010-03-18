(ns cascading.clojure.parse-test
  (:use clojure.test
        (cascading.clojure parse testing))
  (:import cascading.tuple.Fields))

(defn example [x]
  x)

(defn meta-example
  {:fn> "foo"}
  [x]
  x)

(deftest test-fields
  (is (= (Fields. (into-array String ["foo"]))
    (fields "foo")))
  (is (= (Fields. (into-array String ["foo" "bar"]))
    (fields ["foo" "bar"]))))

(deftest test-parse-fn-spec-simple
  (is (= ["cascading.clojure.parse-test" "example"]
    (parse-fn-spec #'example))))

(deftest test-parse-fn-spec-hof
  (is (= ["cascading.clojure.parse-test" "example" 3]
    (parse-fn-spec [#'example 3]))))

(deftest test-parse-fn-spec-invalid
  (is (thrown-with-msg? IllegalArgumentException #"Expected.*"
    (parse-fn-spec example))))

(deftest test-parse-args-everything
  (is (= {:fn-spec ["cascading.clojure.parse-test" "example"]
          :<       (fields "foo")
          :fn>     (fields "bar")
          :>       (fields "baz")}
    (parse-args  [#'example :< "foo" :fn> "bar" :> "baz"]))))

(deftest test-parse-args-everything-multiple-ins
  (is (= {:fn-spec ["cascading.clojure.parse-test" "example"]
          :<       (fields ["foo" "bat"])
          :fn>     (fields "bar")
          :>       (fields "baz")}
    (parse-args [#'example :< ["foo" "bat"] :fn> "bar" :> "baz"]))))

(deftest test-parse-args-no-input-fields
  (is (= {:fn-spec ["cascading.clojure.parse-test" "example"]
          :<       Fields/ALL
	        :fn>     (fields "bar")
	        :>       (fields "baz")}
    (parse-args [#'example :fn> "bar" :> "baz"]))))

(deftest test-parse-args-no-input-or-output-fields
  (is (= {:fn-spec ["cascading.clojure.parse-test" "example"]
          :<       Fields/ALL
	        :fn>     (fields "bar")
	        :>       Fields/RESULTS}
	  (parse-args [#'example :fn> "bar"]))))

(deftest test-parse-args-no-input-or-output-or-fn-fields
  (is (= {:fn-spec ["cascading.clojure.parse-test" "example"]
          :<       Fields/ALL
	        :fn>     Fields/ARGS
	        :>       Fields/RESULTS}
    (parse-args [#'example]))))

(deftest test-parse-fn-spec-meta-fn-fields
  (is (= {:fn-spec ["cascading.clojure.parse-test" "meta-example"]
          :<       Fields/ALL
	        :fn>     (fields "foo")
	        :>       Fields/RESULTS}
    (parse-args [#'meta-example]))))

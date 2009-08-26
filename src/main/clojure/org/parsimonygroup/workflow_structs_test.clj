(ns org.parsimonygroup.workflow-structs-test
  (:use org.parsimonygroup.workflow-structs)
  (:use clojure.contrib.test-is))

(deftest test-cascading-ize
  (is (= ()
	 (cascading-ize ))))
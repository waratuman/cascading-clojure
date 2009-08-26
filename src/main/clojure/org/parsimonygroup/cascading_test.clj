(ns org.parsimonygroup.cascading-test
   (:use org.parsimonygroup.cascading)
   (:use clojure.contrib.test-is))


(deftest attach-fnNs-test
  (is (= {:each {:using 1 :fnNs "fnNs"} :every {:using 2 :groupby "me" :fnNs "fnNs"}}
	 (attach-fnNs {:each {:using 1} :every {:using 2 :groupby "me"}} "fnNs"))))

(ns org.parsimonygroup.makemain-utils-test
   (:use org.parsimonygroup.makemain-utils)
   (:use clojure.contrib.test-is))



(deftest parseArgs-test
  (is (= {:input "input" :output "output" :fnNsName "ns" :pipeline "workflow" :join nil}
	 (parseArgs ["-out=output" "-ns=ns" "-wf=workflow" "-in=input"])))
  (is (= {:input ["input" "input2"] :output "output" :fnNsName "ns" :pipeline "workflow" :join "true"}
	 (parseArgs ["-out=output" "-ns=ns" "-wf=workflow" "-join=true" "-in=input,input2"]))))
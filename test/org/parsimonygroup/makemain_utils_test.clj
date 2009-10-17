(ns org.parsimonygroup.makemain-utils-test
  (:use org.parsimonygroup.makemain-utils)
  (:use clojure.contrib.test-is))

(deftest parseArgs-test
  (is (= {:input "input" :output "output" :pipeline "ns/workflow" :join nil}
         (parse-args ["-out=output" "-wf=ns/workflow" "-in=input"])))
  (is (= {:input ["input" "input2"] :output "output"
          :pipeline "ns/workflow" :join "true"}
         (parse-args ["-out=output" "-wf=ns/workflow"
                     "-join=true" "-in=input,input2"]))))
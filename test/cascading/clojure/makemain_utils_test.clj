(ns cascading.clojure.makemain-utils-test
  (:use cascading.clojure.makemain-utils)
  (:use clojure.contrib.test-is))

(deftest parseArgs-test
  (is (= {:input "input" :output "output" :pipeline "ns/workflow"}
         (parse-args ["-out=output" "-wf=ns/workflow" "-in=input"])))
  (is (= {:input ["input" "input2"] :output "output"
          :pipeline "ns/workflow"}
         (parse-args ["-out=output" "-wf=ns/workflow"
                     "-in=input,input2"]))))
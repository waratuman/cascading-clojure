(ns org.parsimonygroup.join-helper-test
  (:use clojure.contrib.test-is)
  (:use org.parsimonygroup.join-helper)
  (:use org.parsimonygroup.workflow-structs)
  (:require [org.parsimonygroup.java-interop :as j]))

(deftest join-test
  (is (= {:join-wfs ["wf1" "wf2"] :using "join-fn" :on "on" :to "out" :reader "rdr" :writer "w" :javahelper j/join-j :numOutFields 4 :namespace nil}
	 (c-join { :on "on" :to "out" :reader "rdr" :writer "w" :numOutFields 4} "wf1" :with "wf2"))))
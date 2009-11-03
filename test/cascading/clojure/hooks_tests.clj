(ns cascading.clojure.hooks-tests
  (:use cascading.clojure.cascading)
  (:use cascading.clojure.hooks)
  (:use cascading.clojure.io)
  (:use cascading.clojure.testing)
  (:use clojure.test))

(deftest shutdown-hook
  (with-tmp-files [in (temp-dir "source")
		   out (temp-path "sink")]
    (write-lines-in in "some.data" [])))

(deftest listener-test
  (def finished (ref false)) 
  (with-tmp-files [in (temp-dir "source")
		   out (temp-path "sink")]
    (let [fl (listen {:to (copy-flow 
			   (test-tap in) 
			   (test-tap out))
		      :with (flow-listener 
			     {:complete (fn [flow] (dosync (ref-set finished true)))})})]
      (is (.hasListeners fl))
      (is (not (deref finished)))
      (execute fl)
      (is (deref finished)))))
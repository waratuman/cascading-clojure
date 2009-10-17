(ns org.parsimonygroup.copy-tests
  (:import java.io.File)
  (:import cascading.tap.Lfs)
  (:import cascading.scheme.TextLine)
  (:import [cascading.tuple TupleEntry Fields])
  (:use org.parsimonygroup.cascading)
  (:use org.parsimonygroup.io)
  (:use org.parsimonygroup.testing)
  (:use clojure.contrib.duck-streams)
  (:use clojure.contrib.test-is))

;;TODO: factor out structural duplication in with-files test useages.  note the problem with closing file and the iterator mentioned in simple-copy fn
(defn simple-copy 
  ([] (simple-copy []))
  ([lines]
     ;;source-tap creates a dir and sticks fake data in there
	   (with-tmp-files [in (temp-dir "source")
			out (temp-path "sink")]
	  (let [   
	   file (write-lines-in in "some.data" lines)]
	   ;;sink tap creates a tap from a path in tmp that doesn't 
	   ;;exist yet. we can not create a dir or cascading gives 
	   ;;us an output dir already exists exeption.
;;TODO: problem is it deletes file and the tupleiterator is lazy loading.
       (execute (copy-flow (test-tap in) (test-tap out)))))))

(deftest empty-copy
  (with-tmp-files [in (temp-dir "source")
		   out (temp-path "sink")]
     (write-lines-in in "some.data" [])
	  (let [copied (.openSink 
		  (execute 
		   (copy-flow 
		    (test-tap in) 
		    (test-tap out))))]
      (is (not (.hasNext copied))))))

(deftest write-read-clojure
  (with-tmp-files [in (temp-dir "source")
		   out (temp-path "sink")]
    (let [lines [{:a 1 :b 2} [1 2 3]]
	  file (write-lines-in in "some.data" lines)
	  copied (.openSink 
		  (execute 
		   (copy-flow 
		    (test-tap in) 
		    (test-tap out))))]
      (is (= {:a 1 :b 2}
	     (read-tuple (.next copied))))
      (is (= [1 2 3]
	     (read-tuple (.next copied))))
      (is (not (.hasNext copied))))))

(deftest write-read-cascade
  (with-tmp-files [in (temp-dir "source")
		   sink1 (temp-path "sink")
		   sink2 (temp-path "sink2")]
    (let [lines [{:a 1 :b 2} [1 2 3]]
	  file (write-lines-in in "some.data" lines)
	  flow1 (copy-flow (test-tap in) (test-tap sink1))
	  flow2 (copy-flow (test-tap sink1) (test-tap sink2))
	  cascade (execute (cascade flow1 flow2))
	  copied (.openSink flow2)]
      (is (= {:a 1 :b 2}
	     (read-tuple (.next copied))))
      (is (= [1 2 3]
	     (read-tuple (.next copied))))
      (is (not (.hasNext copied))))))

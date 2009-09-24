(ns org.parsimonygroup.copy-test
  (:import java.io.File)
  (:import cascading.tap.Lfs)
  (:import cascading.scheme.TextLine)
  (:import [cascading.tuple TupleEntry Fields])
  (:use org.parsimonygroup.cascading)
  (:use org.parsimonygroup.io)
  (:use org.parsimonygroup.testing)
  (:use clojure.contrib.duck-streams)
  (:use clojure.contrib.test-is))

;;TODO: move to tests specific to io
(deftest building-paths
     (is (= "tmp/foo/bar/data"
	 (path "tmp" "foo" "bar" "data"))))

(defn simple-copy 
  ([] (simple-copy []))
  ([lines]
     ;;source-tap creates a dir and sticks fake data in there
	   (with-tmp-files [in (temp-dir "source")
			out (File. (temp-path "sink"))]
	  (let [   
	   file (write-lines-in in "some.data" lines)]
	   ;;sink tap creates a tap from a path in tmp that doesn't 
	   ;;exist yet. we can not create a dir or cascading gives 
	   ;;us an output dir already exists exeption.
;;TODO: problem is it deletes file and the tupleiterator is lazy loading.
       (execute (copy-flow (test-tap in) (test-tap out)))))))

;;TODO: factor out structural duplication in with-files useages.
(deftest empty-copy
  (with-tmp-files [in (temp-dir "source")
		   out (File. (temp-path "sink"))]
    (let [file (write-lines-in in "some.data" [])
	  copied (.openSink 
		  (execute 
		   (copy-flow 
		    (test-tap in) 
		    (test-tap out))))]
      (is (not (.hasNext copied))))))

(deftest write-read-clojure
  (with-tmp-files [in (temp-dir "source")
		   out (File. (temp-path "sink"))]
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
(ns org.parsimonygroup.copy-test
  (:import java.io.File)
  (:import cascading.tap.Lfs)
  (:import cascading.scheme.TextLine)
  (:import [cascading.tuple TupleEntry])
  (:use org.parsimonygroup.cascading)
  (:use clojure.contrib.duck-streams)
  (:use clojure.contrib.test-is))

(defn delete-file
  "Delete file f. Raise an exception if it fails."
  [f]
  (or (.delete (if (string? f) (File. f) f))
      (throw (java.io.IOException. (str "Couldn't delete " f)))))

(defn delete-file-recursively
  "Delete file f. If it's a directory, recursively delete all its
  contents. Raise an exception if any deletion fails."
  [f]
  (let [file (if (string? f) (File. f) f)]
    (if (.isDirectory file)
      (doseq [child (.listFiles file)]
          (delete-file-recursively child)))
    (delete-file file)))

(defn path [root & rest]
  (reduce #(str %1 File/separator %2)
	  root
	  rest))

(defn temp-path [sub-path]
  (path (System/getProperty "java.io.tmpdir") 
	       sub-path))

;;TODO: trouble with this is that lazy cleanup doesn't work well with the repl. :-)

(defn temp-dir [sub-path]
"1) creates a directory in System.getProperty(\"java.io.tmpdir\") 
 2) calls tempDir.deleteOn Exit() so the file is deleted by the jvm.
 reference: ;http://bugs.sun.com/bugdatabase/view_bug.do?bug_id=4735419"
  (let [tmp-dir  (File. (temp-path sub-path))]
  (if (not (.exists tmp-dir))
    (.mkdir tmp-dir))
  (.deleteOnExit tmp-dir)
  tmp-dir))

(defmacro with-tmp-files [bindings & body]
  `(let ~bindings 
     (try ~@body 
	  (finally 
	    (doall (for [file# (reverse 
			       (map first 
				    (partition 2 ~bindings)))]
			       (delete-file-recursively file#)))))))

(defn write-lines-in [root filename lines]
  (write-lines 
   (File. 
    (path (.getAbsolutePath root) filename)) lines))

(defn test-tap [f]
  "by making it a single field textline (passing in Fields with 1 field), we should be able to get a single rather than a tuple.  this negates the parsing steps in read-tuple."
  (let [path (if (string? f) f (.getAbsolutePath f))]
    (Lfs. (TextLine. Fields/FIRST) path)))

(defn read-tuple [#^TupleEntry t]
  "assumes a single field textline, if we use tuples or multi-filed 
  textlines, we must split as in: (second (.split (.get t 1) \"\t\"))))"
  (read-string  (.get t 0)))

(defn execute [flow]
       (doto flow .start .complete)
       flow)

(defn simple-copy 
  ([] (simple-copy []))
  ([lines]
     ;;source-tap creates a dir and sticks fake data in there
	   (with-files [in (temp-dir "source")
			out (File. (temp-path "sink"))]
	  (let [   
	   file (write-lines-in in "some.data" lines)]
	   ;;sink tap creates a tap from a path in tmp that doesn't 
	   ;;exist yet. we can not create a dir or cascading gives 
	   ;;us an output dir already exists exeption.
;;TODO: problem is it deletes file and the tupleiterator is lazy loading.
       (execute (copy-flow (test-tap in) (test-tap out)))))))

(deftest building-paths
     (is (= "tmp/foo/bar/data"
	 (path "tmp" "foo" "bar" "data"))))


;;TODO: factor out duplication in with-files useages.
(deftest empty-copy
  (with-files [in (temp-dir "source")
	       out (File. (temp-path "sink"))]
    (let [file (write-lines-in in "some.data" [])
	  copied (.openSink 
		  (execute 
		   (copy-flow 
		    (test-tap in) 
		    (test-tap out))))]
   (is (not (.hasNext copied))))))

;;TODO: iterator-seq yielded strange results here
(deftest write-read-clojure
  (with-files [in (temp-dir "source")
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
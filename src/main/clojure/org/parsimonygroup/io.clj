(ns org.parsimonygroup.io
  (:import java.io.File)
  (:use clojure.contrib.duck-streams))

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

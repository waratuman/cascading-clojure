(ns org.parsimonygroup.io
  (:import java.io.File)
  (:use clojure.contrib.java-utils)
  (:use clojure.contrib.duck-streams))

(defn delete-file
  "Delete file f. Raise an exception if it fails."
  [f]
  (let [file (if (string? f) (File. f) f)]
    (if (.exists file)
  (or (.delete file)
      (throw (java.io.IOException. (str "Couldn't delete " f)))))))

(defn delete-file-recursively
  "Delete file f. If it's a directory, recursively delete all its
  contents. Raise an exception if any deletion fails."
  [f]
  (let [file (if (string? f) (File. f) f)]
    (if (.isDirectory file)
      (doseq [child (.listFiles file)]
          (delete-file-recursively child)))
    (delete-file file)))

(defn temp-path [sub-path]
  (.getAbsolutePath 
   (file (System/getProperty "java.io.tmpdir") 
	       sub-path)))

;;TODO:  deleteOnExit is last resort cleanup on jvm exit.  delete-file-recursively is preemptive delete on exitint the code block for repl and tests run in the same process.

(defn temp-dir
"1) creates a directory in System.getProperty(\"java.io.tmpdir\") 
 2) calls tempDir.deleteOn Exit() so the file is deleted by the jvm.
 reference: ;http://bugs.sun.com/bugdatabase/view_bug.do?bug_id=4735419"
 [sub-path]
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

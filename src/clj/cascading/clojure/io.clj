(ns cascading.clojure.io
  (:import java.io.File)
  (:use clojure.contrib.java-utils)
  (:use clojure.contrib.duck-streams))

(defn temp-path [sub-path]
   (file (System/getProperty "java.io.tmpdir") sub-path))

(defn temp-dir
  "1) creates a directory in System.getProperty(\"java.io.tmpdir\")
   2) calls tempDir.deleteOn Exit() so the file is deleted by the jvm.
   reference: ;http://bugs.sun.com/bugdatabase/view_bug.do?bug_id=4735419
   deleteOnExit is last resort cleanup on jvm exit."
  [sub-path]
  (let [tmp-dir (temp-path sub-path)]
    (or (.exists tmp-dir) (.mkdir tmp-dir))
    (.deleteOnExit tmp-dir)
    tmp-dir))

(defn delete-all
  "delete-file-recursively is preemptive delete on exiting the code block for
   repl and tests run in the same process."
  [bindings]
  (doseq [file (reverse (map second (partition 2 bindings)))]
    (if (.exists file)
     (delete-file-recursively file))))

(defmacro with-tmp-files [bindings & body]
  `(let ~bindings
     (try ~@body
       (finally (delete-all ~bindings)))))

(defn write-lines-in [root filename lines]
  (write-lines
    (file (.getAbsolutePath root) filename) lines))

(ns org.parsimonygroup.copy-test
  (:import java.io.File)
  (:use clojure.contrib.test-is))

;;tempdirs
;;http://bugs.sun.com/bugdatabase/view_bug.do?bug_id=4735419
;;As discussed in this RFE and its comments, you could call tempDir.delete() first. 
;;Or you could use System.getProperty("java.io.tmpdir") and create a directory there. 
;;Either way, you should remember to call tempDir.deleteOnExit(), or the file won't be deleted after you're done.

;;another option
;;tempDir = File.createTempFile("install", "dir");
;;tempDir.mkdir();

(defn temp-dir [sub-path]
  (let [tmp-path (System/getProperty "java.io.tmpdir")
	tmp-dir  (File. (str tmp-path File/separator sub-path))]
  (if (not (.exists tmp-dir))
    (.mkDir tmp-dir))
  (.deleteOnExit tmp-dir)
  tmp-dir))

;; (deftest simple-copy
;;   (
;;     String path = "simple";

;;     Flow first = firstFlow( path );
;;     Flow second = secondFlow( first.getSink(), path );
;;     Flow third = thirdFlow( second.getSink(), path );
;;     Flow fourth = fourthFlow( third.getSink(), path );

;;     Cascade cascade = new CascadeConnector().connect( first, second, third, fourth );

;;     cascade.start();

;;     cascade.complete();

;;     validateLength( fourth, 20 );
;;     }

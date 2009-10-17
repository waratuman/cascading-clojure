(ns cascading.clojure.io-tests
  (:import java.io.File)
  (:use cascading.clojure.io)
  (:use cascading.clojure.testing)
  (:use clojure.contrib.duck-streams)
  (:use clojure.contrib.test-is))

(deftest delete-after-tmpfiles-block
  (let [i (temp-dir "source")]
  (with-tmp-files [in i])
     (is (not (.exists i)))))

(ns cascading.clojure.io
  (:import (java.io File)
           (org.apache.log4j Logger Level))
  (:require (cascading.clojure [core :as c]))
  (:use clojure.test
        clojure.contrib.java-utils
        clojure.contrib.duck-streams))

(def temp-path
     (str (System/getProperty "java.io.tmpdir")))

(defn temp-dir
  ([] (temp-dir (str (java.util.UUID/randomUUID))))
  ([sub-path]
     (let [dir (file (str temp-path "/" sub-path))]
       (or (.exists dir) (.mkdir dir))
       (.deleteOnExit dir)
       dir)))

(defn temp-file
  ([] (temp-file nil))
  ([dir]
     (let [f (file (File/createTempFile "tmp" nil dir))]
       (.deleteOnExit f)
       f)))

(defn delete-tmp-files [bindings]
  (doseq [file (reverse (map second (partition 2 bindings)))]
    (if (.exists file) (delete-file-recursively file))))

(defmacro with-tmp-files [bindings & body]
  `(let ~bindings
     (try ~@body
          (finally (delete-tmp-files ~bindings)))))

(defn log-level [level]
  (cond (or (= level "fatal") (= level :fatal)) Level/FATAL
        (or (= level "warn") (= level :warn)) Level/FATAL
        (or (= level "info") (= level :info)) Level/INFO
        (or (= level "debug") (= level :debug)) Level/DEBUG
        (or (= level "off") (= level :off)) Level/OFF))

(defmacro log-with-level [level & body]
  `(let [lev#       (log-level ~level)
        logger#    (Logger/getRootLogger)
        prev-lev#  (.getLevel logger#)]
    (try (.setLevel logger# lev#)
         ~@body
         (finally (.setLevel logger# prev-lev#)))))

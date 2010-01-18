(ns cascading.clojure.logging
  (:use clojure.contrib.logging)
  (:import org.apache.log4j.PropertyConfigurator)
  (:import java.util.Properties)
  (:import java.io.FileInputStream)
  (:import  [org.apache.log4j
             ConsoleAppender WriterAppender
             Logger SimpleLayout Level LogManager]))

(defn load-log-props [path]
  (do
    (PropertyConfigurator/configure
     (.load (Properties.)
            (FileInputStream. path))))
  (LogManager/resetConfiguration))

;;WriterAppender appends log events to a Writer or an OutputStream
(defn test-logger [out]
 (let [logger (Logger/getRootLogger)]
   (doto logger
     (.addAppender (WriterAppender. (SimpleLayout.) out))
     (.addAppender (ConsoleAppender. (SimpleLayout.))))))

(defn hadoop-logger []
  (let [r (Logger/getRootLogger)]
    (doto r (.setLevel Level/DEBUG)
          (.addAppender
           (ConsoleAppender.
            (SimpleLayout.))))))

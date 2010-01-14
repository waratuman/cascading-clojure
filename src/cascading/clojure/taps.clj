(ns
 #^{:doc
"Everything your heart desires for making cascading taps.
 Warning: BEWARE that Hadoop reuses Writeable objects from its iterators so iterator-tap is dangerious and can not be used with iterator-seq!!!
 also be aware that cascading 1.1 also reuses non primitive types inside tuples
"}
  cascading.clojure.taps
  (:import java.io.File
	   [cascading.tap Lfs Hfs]
	   [cascading.tap Tap]
	   [cascading.pipe Pipe])
  (:import [cascading.cascade Cascades])
  (:import cascading.scheme.TextLine)
  (:import [cascading.tuple TupleEntry Fields])
  (:use cascading.clojure.io)
  (:use clojure.contrib.duck-streams))

(defn default-tap
  "the default tap is a Hadoop file system TextLine tap."
  [path] (Hfs. (TextLine.) path))

(defn test-tap [f]
  "by making it a single field textline (passing in Fields with 1 field), we should be able to get a single rather than a tuple.  this negates the parsing steps in read-tuple."
  (let [path (if (string? f) f (.getAbsolutePath f))]
    (Lfs. (TextLine.) path)))

(defn read-tuple [#^TupleEntry t]
"assumes a single field textline, if we use tuples or multi-filed 
textlines, we must split as in: (second (.split (.get t 1) \"\t\"))))"
    (read-string  (.get t 0)))

(defn taps-map
  "creates a taps map from a seq of pipes and a seq of taps.
   http://www.cascading.org/javadoc/cascading/cascade/Cascades.html"
  [pipes taps]
  (Cascades/tapsMap (into-array Pipe pipes) (into-array Tap taps)))

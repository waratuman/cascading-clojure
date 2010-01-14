(ns cascading.clojure.taps
  (:import java.io.File
	   [cascading.tap Lfs Hfs]
	   [cascading.tap Tap]
	   [cascading.pipe Pipe])
  (:import [cascading.cascade Cascades])
  (:import cascading.scheme.TextLine)
  (:import [cascading.tuple TupleEntry Fields])
  (:use cascading.clojure.io)
  (:use clojure.contrib.duck-streams))

;;BEWARE of the iterator-tap and the mutable-reused tuple iterator entries!!!!!

;; <stuartsierra> bradford: careful you don't hang on to the Writable objects from the iterator -- Hadoop reuses them
;; <bradford> yea, that may have had something to do with the interaction with interator-seq <stuartsierra> I ran into this ages ago, which is why iterator-seq was created in the first place.
;; <cwensel> fyi, cascading 1.1 also reuses non primitive types inside tuples as well

(defn default-tap [path] (Hfs. (TextLine.) path))

(defn test-tap [f]
  "by making it a single field textline (passing in Fields with 1 field), we should be able to get a single rather than a tuple.  this negates the parsing steps in read-tuple."
  (let [path (if (string? f) f (.getAbsolutePath f))]
    (Lfs. (TextLine.) path)))

(defn read-tuple [#^TupleEntry t]
  "assumes a single field textline, if we use tuples or multi-filed 
  textlines, we must split as in: (second (.split (.get t 1) \"\t\"))))"
  (read-string  (.get t 0)))

(defn taps-map [pipes taps]
  (Cascades/tapsMap (into-array Pipe pipes) (into-array Tap taps)))

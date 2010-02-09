(ns cascading.clojure.api-example
  (:require (cascading.clojure [api :as c])))

(defn starts-with-b?
  {:fields "word"}
  [word]
  (re-find #"^b.*" word))

(defn split-words
  {:fields "word"}
  [line]
  (re-seq #"\w+" line))

(defn uppercase
  {:fields "word"}
  [word]
  (.toUpperCase word))

(def phrase-reader
  (-> (c/pipe "phrase-reader")
    (c/mapcat "line" #'split-words)
    (c/filter #'starts-with-b?)
    (c/group-by "word")
    (c/count "count")))

(def white-reader
  (-> (c/pipe "white-reader")
    (c/mapcat "line" ["white" #'split-words])))

(def joined
  (-> [phrase-reader white-reader]
    (c/inner-join ["word" "white"])
    (c/select ["word" "count"])
    (c/map ["word"] [["upword"] #'uppercase] ["upword" "count"])))

(defn run-example
  [jar-path dot-path in-phrase-dir-path in-white-dir-path out-dir-path]
  (let [source-scheme  (c/text-line "line")
        sink-scheme    (c/text-line ["upword" "count"])
        phrase-source  (c/hfs-tap source-scheme in-phrase-dir-path)
        white-source   (c/hfs-tap source-scheme in-white-dir-path)
        sink           (c/hfs-tap sink-scheme out-dir-path)
        flow           (c/flow
                         jar-path
                         {}
                         {"phrase-reader" phrase-source
                          "white-reader"  white-source}
                         sink
                         joined)]
    (c/write-dot flow dot-path)
    (c/exec flow)))

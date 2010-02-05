(ns cascading.clojure.api-example
  (:require (cascading.clojure [api :as c])))

(defn starts-with-b? [word]
  (re-find #"^b.*" word))

(defn split-words [line]
  (map list (re-seq #"\w+" line)))

(def phrase-reader
  (-> (c/named-pipe "phrase-reader")
    (c/mapcat ["line"] ["word"] #'split-words)
    (c/filter ["word"] #'starts-with-b?)
    (c/group-by "word")
    (c/count "count")))

(def white-reader
  (-> (c/named-pipe "white-reader")
    (c/word-split "line" "white")))

(def joined
  (-> [phrase-reader white-reader]
    (c/inner-join ["word" "white"])
    (c/select ["word" "count"])))

(let [[in-phrase-dir-path in-white-dir-path out-dir-path dot-path] *command-line-args*
      source-scheme  (c/text-line-scheme ["line"])
      sink-scheme    (c/text-line-scheme ["word" "count"])
      phrase-source  (c/hfs-tap source-scheme in-phrase-dir-path)
      white-source   (c/hfs-tap source-scheme in-white-dir-path)
      sink           (c/hfs-tap sink-scheme out-dir-path)
      flow           (c/flow
                       {"phrase-reader" phrase-source
                        "white-reader"  white-source}
                       sink
                       joined)]
    (c/write-dot flow dot-path)
    (c/complete flow))

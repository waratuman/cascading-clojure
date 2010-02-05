(ns cascading.clojure.raw-example
  (:import
     (java.util Properties)
     (cascading.tuple Fields)
     (cascading.scheme TextLine)
     (cascading.flow Flow FlowConnector)
     (cascading.operation.regex RegexGenerator RegexFilter)
     (cascading.operation.aggregator Count)
     (cascading.pipe Pipe Each Every GroupBy CoGroup)
     (cascading.pipe.cogroup InnerJoin)
     (cascading.tap Hfs Lfs Tap)))

(defn fields
  {:tag Fields}
  [& names]
  (Fields. (into-array names)))

(let [[in-phrase-dir-path in-white-dir-path out-dir-path dot-path] *command-line-args*
      phrase-reader  (-> (Pipe. "phrase-reader")
                       (Each. (fields "line") (RegexGenerator. (fields "word") "(\\w+)"))
                       (Each. (fields "word") (RegexFilter. "^b.*"))
                       (GroupBy. (fields "word"))
                       (Every. (Count. (fields "count"))))
      white-reader   (-> (Pipe. "white-reader")
                       (Each. (fields "line") (RegexGenerator. (fields "white") "(\\w+)")))
      joined         (CoGroup. phrase-reader (fields "word") white-reader (fields "white") (InnerJoin.))
      source-scheme  (TextLine. (fields "line") (fields "line"))
      sink-scheme    (TextLine. (fields "word" "count") (fields "word" "count"))
      phrase-source  (Hfs. source-scheme #^String in-phrase-dir-path)
      white-source   (Hfs. source-scheme #^String in-white-dir-path)
      sink           (Hfs. sink-scheme #^String out-dir-path)
      properties     (Properties.)
      flow-connector (FlowConnector.)
      flow           (.connect flow-connector
                       {"phrase-reader" phrase-source
                        "white-reader"  white-source}
                       sink
                       joined)]
  (.writeDOT flow #^String dot-path)
  (.complete flow))

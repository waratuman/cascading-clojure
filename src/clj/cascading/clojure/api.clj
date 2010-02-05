(ns cascading.clojure.api
  (:refer-clojure :exclude (count filter mapcat map))
  (:import (cascading.tuple Fields)
           (cascading.scheme TextLine)
           (cascading.flow Flow FlowConnector)
           (cascading.operation Identity)
           (cascading.operation.regex RegexGenerator RegexFilter)
           (cascading.operation.aggregator Count)
           (cascading.pipe Pipe Each Every GroupBy CoGroup)
           (cascading.pipe.cogroup InnerJoin)
           (cascading.scheme Scheme)
           (cascading.tap Hfs Lfs Tap)
           (java.util Properties Map)
           (cascading.clojure ClojureFilter ClojureMapcat); ClojureMap)
           (clojure.lang Var)))

(defn- fields
  {:tag Fields}
  [names]
  (Fields. (into-array names)))

(defn- ns-var-pair [v]
  (let [m (meta v)]
    [(str (:ns m)) (str (:name m))]))

(defn named-pipe [#^String name]
  (Pipe. name))

(defn regex-filter [#^Pipe previous name pattern]
  (Each. previous (fields [name]) (RegexFilter. pattern)))

(defn filter [#^Pipe previous in-fields #^Var pred]
  (let [[ns-str var-str] (ns-var-pair pred)]
    (Each. previous (fields in-fields)
      (ClojureFilter. ns-str var-str))))

(defn mapcat [#^Pipe previous in-fields out-fields #^Var f]
  (let [[ns-str var-str] (ns-var-pair f)
        func (ClojureMapcat. (fields out-fields) ns-str var-str)]
    (Each. previous (fields in-fields) func)))

;(defn map [#^Pipe previous in-fields out-fields #^Var f]
;  (let [[ns-str var-str] (ns-var-pair f)
;        func (ClojureMap. (fields out-fields) ns-str var-str)]
;    (Each. previous (fields in-fields) func)))

(defn word-split [#^Pipe previous in-field out-field]
  (let [func (RegexGenerator. (fields [out-field])
               "(?<!\\pL)(?=\\pL)[^ ]*(?<=\\pL)(?!\\pL)")]
    (Each. previous (fields [in-field]) func)))

(defn group-by [#^Pipe previous group-field]
  (GroupBy. previous (fields [group-field])))

(defn count [#^Pipe previous #^String count-field]
  (Every. previous (Count. (fields [count-field]))))

(defn inner-join [[#^Pipe lhs #^Pipe rhs] [lhs-field rhs-field]]
  (CoGroup. lhs (fields [lhs-field]) rhs (fields [rhs-field]) (InnerJoin.)))

(defn select [#^Pipe previous keep]
  (Each. previous (fields keep) (Identity.)))

(defn text-line-scheme [names]
  (TextLine. (fields names) (fields names)))

(defn hfs-tap [#^Scheme scheme #^String path]
  (Hfs. scheme path))

(defn flow [jar-path config #^Map source-map #^Tap sink #^Pipe pipe]
  (let [props (Properties.)]
    (when jar-path
      (FlowConnector/setApplicationJarPath props jar-path))
    (doseq [[k v] config]
      (.setProperty props k v))
    (let [flow-connector (FlowConnector. props)]
      (.connect flow-connector source-map sink pipe))))

(defn write-dot [#^Flow flow #^String path]
  (.writeDOT flow path))

(defn complete [#^Flow flow]
  (.complete flow))


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
           (cascading.clojure ClojureFilter ClojureMapcat ClojureMap
                              ClojureAggregator)
           (clojure.lang Var)))



(defn- ns-fn-name-pair [v]
  (let [m (meta v)]
    [(str (:ns m)) (str (:name m))]))

(defn fields
  {:tag Fields}
  [names]
  (if (string? names)
    (fields [names])
    (Fields. (into-array names))))

(defn named-pipe [#^String name]
  (Pipe. name))

(defn filter [#^Pipe previous in-fields #^Var pred]
  (let [[ns-str var-str] (ns-fn-name-pair pred)]
    (Each. previous (fields in-fields)
      (ClojureFilter. ns-str var-str))))

(defn mapcat [#^Pipe previous in-fields out-fields #^Var f]
  (let [[ns-str var-str] (ns-fn-name-pair f)
        func (ClojureMapcat. (fields out-fields) ns-str var-str)]
    (Each. previous (fields in-fields) func)))

(defn map [#^Pipe previous in-fields out-fields #^Var f]
  (let [[ns-str var-str] (ns-fn-name-pair f)
        func (ClojureMap. (fields out-fields) ns-str var-str (seq [1 2 3]))]
    (Each. previous (fields in-fields) func)))

(defn aggregate [#^Pipe previous in-fields out-fields
                 #^Var start #^Var aggregate #^Var complete]
  (let [[start-ns-str     start-fn-str]     (ns-fn-name-pair start)
        [aggregate-ns-str aggregate-fn-str] (ns-fn-name-pair aggregate)
        [complete-ns-str  complete-fn-str]  (ns-fn-name-pair complete)]
    (Every. previous (fields in-fields)
      (ClojureAggregator. (fields out-fields)
        start-ns-str     start-fn-str
        aggregate-ns-str aggregate-fn-str
        complete-ns-str  complete-fn-str))))

(defn word-split [#^Pipe previous in-field out-field]
  (let [func (RegexGenerator. (fields [out-field])
               "(?<!\\pL)(?=\\pL)[^ ]*(?<=\\pL)(?!\\pL)")]
    (Each. previous (fields [in-field]) func)))

(defn group-by [#^Pipe previous group-field]
  (GroupBy. previous (fields [group-field])))

(defn count [#^Pipe previous #^String count-field]
  (Every. previous (Count. (fields [count-field]))))

(defn inner-join [[#^Pipe lhs #^Pipe rhs] [lhs-fields rhs-fields]]
  (CoGroup. lhs (fields lhs-fields) rhs (fields rhs-fields) (InnerJoin.)))

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

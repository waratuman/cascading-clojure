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

(defn ns-fn-name-pair [v]
  (let [m (meta v)]
    [(str (:ns m)) (str (:name m))]))

(defn fn-spec [v-or-coll]
  "v-or-coll => var or [var & params]
   Returns an Object array that is used to represent a Clojure function.
   If the argument is a var, the array represents that function.
   If the argument is a coll, the array represents the function returned
   by applying the first element, which should be a var, to the rest of the
   elements."
  (cond
    (var? v-or-coll)
      (into-array Object (ns-fn-name-pair v-or-coll))
    (coll? v-or-coll)
      (into-array Object
        (concat
          (ns-fn-name-pair (first v-or-coll))
          (next v-or-coll)))
    :else
      (throw (IllegalArgumentException. (str v-or-coll)))))

(defn fields
  {:tag Fields}
  [names]
  (if (string? names)
    (fields [names])
    (Fields. (into-array names))))

(defn named-pipe [#^String name]
  (Pipe. name))

(defn filter [#^Pipe previous in-fields pred]
  (Each. previous (fields in-fields)
    (ClojureFilter. (fn-spec pred))))

(defn mapcat [#^Pipe previous in-fields out-fields func]
  (Each. previous (fields in-fields)
    (ClojureMapcat. (fields out-fields) (fn-spec func))))

(defn map [#^Pipe previous in-fields out-fields func]
  (Each. previous (fields in-fields)
    (ClojureMap. (fields out-fields) (fn-spec func))))

(defn aggregate [#^Pipe previous in-fields out-fields
                 start aggregate complete]
  (Every. previous (fields in-fields)
    (ClojureAggregator. (fields out-fields)
      (fn-spec start) (fn-spec aggregate) (fn-spec complete))))

(defn group-by [#^Pipe previous group-fields]
  (GroupBy. previous (fields group-fields)))

(defn count [#^Pipe previous #^String count-fields]
  (Every. previous (Count. (fields count-fields))))

(defn inner-join [[#^Pipe lhs #^Pipe rhs] [lhs-fields rhs-fields]]
  (CoGroup. lhs (fields lhs-fields) rhs (fields rhs-fields) (InnerJoin.)))

(defn select [#^Pipe previous keep-fields]
  (Each. previous (fields keep-fields) (Identity.)))

(defn text-line-scheme [field-names]
  (TextLine. (fields field-names) (fields field-names)))

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

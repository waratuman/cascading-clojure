(ns cascading.clojure.api
  (:refer-clojure :exclude (count first filter mapcat map))
  (:use [clojure.contrib.seq-utils :only (find-first indexed)]
        (cascading.clojure parse))
  (:require [clj-json.core :as json])
  (:import (cascading.tuple Tuple TupleEntry Fields)
           (cascading.scheme TextLine)
           (cascading.flow Flow FlowConnector)
           (cascading.operation Identity)
           (cascading.operation.filter Limit)
           (cascading.operation.regex RegexGenerator RegexFilter)
           (cascading.operation.aggregator First Count)
           (cascading.pipe Pipe Each Every GroupBy CoGroup)
           (cascading.pipe.cogroup InnerJoin OuterJoin
                                   LeftJoin RightJoin MixedJoin)
           (cascading.scheme Scheme)
           (cascading.tap Hfs Lfs Tap)
           (org.apache.hadoop.io Text)
           (org.apache.hadoop.mapred TextInputFormat TextOutputFormat
                                     OutputCollector JobConf)
           (java.util Properties Map UUID)
           (cascading.clojure ClojureFilter ClojureMapcat ClojureMap
                              ClojureAggregator ClojureBuffer Util)
           (clojure.lang Var)
           (java.io File)
           (java.lang RuntimeException)))

(defn pipes-array
  [pipes]
  (into-array Pipe pipes))

(defn as-pipes
  [pipe-or-pipes]
  (pipes-array
    (if (instance? Pipe pipe-or-pipes)
      [pipe-or-pipes]
      pipe-or-pipes)))

(defn- uuid []
  (str (UUID/randomUUID)))

(defn pipe
  "Returns a Pipe of the given name, or if one is not supplied with a
   unique random name."
  ([]
   (Pipe. (uuid)))
  ([#^String name]
   (Pipe. name)))

(defn filter [#^Pipe previous & args]
  (let [[#^Fields in-fields _ spec _] (parse-args args)]
    (Each. previous in-fields
      (ClojureFilter. spec))))

(defn mapcat [#^Pipe previous & args]
  (let [[#^Fields in-fields func-fields spec #^Fields out-fields] (parse-args args)]
    (Each. previous in-fields
      (ClojureMapcat. func-fields spec) out-fields)))

(defn map [#^Pipe previous & args]
  (let [[#^Fields in-fields func-fields spec #^Fields out-fields] (parse-args args)]
    (Each. previous in-fields
      (ClojureMap. func-fields spec) out-fields)))

(defn extract [#^Pipe previous & args]
  "A map operation that extracts a new field, thus returning Fields/ALL."
  (let [[#^Fields in-fields func-fields spec #^Fields out-fields] (parse-args args)]
    (Each. previous in-fields
      (ClojureMap. func-fields spec) Fields/ALL)))

(defn agg [f init]
  "A combinator that takes a fn and an init value and returns a reduce aggregator."
  (fn ([] init)
    ([x] [x])
    ([x y] (f x y))))

(defn aggregate [#^Pipe previous & args]
  (let [[#^Fields in-fields func-fields specs #^Fields out-fields] (parse-args args)]
    (Every. previous in-fields
      (ClojureAggregator. func-fields specs) out-fields)))

(defn buffer [#^Pipe previous & args]
  (let [[#^Fields in-fields func-fields specs #^Fields out-fields] (parse-args args)]
    (Every. previous in-fields
      (ClojureBuffer. func-fields specs) out-fields)))

(defn group-by
  ([previous group-fields]
     (GroupBy. (as-pipes previous) (fields group-fields)))
  ([previous group-fields sort-fields]
     (GroupBy. (as-pipes previous) (fields group-fields) (fields sort-fields)))
  ([previous group-fields sort-fields reverse-order]
     (GroupBy. (as-pipes previous) (fields group-fields) (fields sort-fields) reverse-order)))

(defn first [#^Pipe previous]
  (Every. previous (First.)))

(defn count [#^Pipe previous #^String count-fields]
  (Every. previous
    (Count. (fields count-fields))))

(defn co-group
  [pipes-seq fields-seq declared-fields joiner]
  (CoGroup.
    (pipes-array pipes-seq)
    (fields-array fields-seq)
    (fields declared-fields)
    joiner))

; TODO: create join abstractions. http://en.wikipedia.org/wiki/Join_(SQL)

; "join and drop" is called a natural join - inner join, followed by select to
; remove duplicate join keys.

; another kind of join and dop is to drop all the join keys - for example, when
; you have extracted a specil join key jsut for grouping, you typicly want to get
; rid of it after the group operation.

; another kind of "join and drop" is an outer-join followed by dropping the nils

(defn inner-join
  [pipes-seq fields-seq declared-fields]
  (co-group pipes-seq fields-seq declared-fields (InnerJoin.)))

(defn outer-join
  [pipes-seq fields-seq declared-fields]
  (co-group pipes-seq fields-seq declared-fields (OuterJoin.)))

(defn left-join
  [pipes-seq fields-seq declared-fields]
  (co-group pipes-seq fields-seq declared-fields (LeftJoin.)))

(defn right-join
  [pipes-seq fields-seq declared-fields]
  (co-group pipes-seq fields-seq declared-fields (RightJoin.)))

(defn mixed-join
  [pipes-seq fields-seq declared-fields inner-bools]
  (co-group pipes-seq fields-seq declared-fields
      (MixedJoin. (into-array Boolean inner-bools))))

(defn join-into
  "outer-joins all pipes into the leftmost pipe"
  [pipes-seq fields-seq declared-fields]
  (co-group pipes-seq fields-seq declared-fields
      (MixedJoin.
       (boolean-array (cons true
             (repeat (- (clojure.core/count pipes-seq)
            1) false))))))

(defn select [#^Pipe previous keep-fields]
  (Each. previous (fields keep-fields) (Identity.)))

(defn text-line
 ([]
  (TextLine. Fields/FIRST))
 ([field-names]
  (TextLine. (fields field-names) (fields field-names))))

(defn json-map-line
  [json-keys]
  (let [json-keys-arr (into-array json-keys)
        scheme-fields (fields json-keys)]
    (proxy [Scheme] [scheme-fields scheme-fields]
      (sourceInit [tap #^JobConf conf]
        (.setInputFormat conf TextInputFormat))
      (sinkInit [tap #^JobConf conf]
        (doto conf
          (.setOutputKeyClass Text)
          (.setOutputValueClass Text)
          (.setOutputFormat TextOutputFormat)))
      (source [_ #^Text json-text]
        (let [json-map  (json/parse-string (.toString json-text))
              json-vals (clojure.core/map json-map json-keys-arr)]
          (Util/coerceToTuple json-vals)))
      (sink [#^TupleEntry tuple-entry #^OutputCollector output-collector]
        (let [tuple     (.selectTuple tuple-entry scheme-fields)
              json-map  (areduce json-keys-arr i mem {}
                          (assoc mem (aget json-keys-arr i)
                                     (.get tuple i)))
              json-str  (json/generate-string json-map)]
          (.collect output-collector nil (Tuple. json-str)))))))

(defn path
  {:tag String}
  [x]
  (if (string? x) x (.getAbsolutePath #^File x)))

(defn hfs-tap [#^Scheme scheme path-or-file]
  (Hfs. scheme (path path-or-file)))

(defn lfs-tap [#^Scheme scheme path-or-file]
  (Lfs. scheme (path path-or-file)))

(defn flow
  ([#^Map source-map #^Tap sink #^Pipe pipe]
   (flow nil {} source-map sink pipe))
  ([jar-path config #^Map source-map #^Tap sink #^Pipe pipe]
   (let [props (Properties.)]
     (when jar-path
       (FlowConnector/setApplicationJarPath props jar-path))
     (doseq [[k v] config]
       (.setProperty props k v))
     (.setProperty props "mapred.used.genericoptionsparser" "true")
     (let [flow-connector (FlowConnector. props)]
       (try
         (.connect flow-connector source-map sink pipe)
         (catch cascading.flow.PlannerException e
           (.writeDOT e "exception.dot")
           (throw (RuntimeException.
             "see exception.dot to visualize your broken plan." e))))))))

(defn write-dot [#^Flow flow #^String path]
  (.writeDOT flow path))

(defn exec [#^Flow flow]
  (doto flow .start .complete))

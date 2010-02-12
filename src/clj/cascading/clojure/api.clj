(ns cascading.clojure.api
  (:refer-clojure :exclude (count first filter mapcat map))
  (:use [clojure.contrib.seq-utils :only [find-first indexed]])
  (:require [clj-json :as json])
  (:import (cascading.tuple Tuple TupleEntry Fields)
           (cascading.scheme TextLine)
           (cascading.flow Flow FlowConnector)
           (cascading.operation Identity)
           (cascading.operation.regex RegexGenerator RegexFilter)
           (cascading.operation.aggregator First Count)
           (cascading.pipe Pipe Each Every GroupBy CoGroup)
           (cascading.pipe.cogroup
	    InnerJoin OuterJoin LeftJoin RightJoin MixedJoin)
           (cascading.scheme Scheme)
           (cascading.tap Hfs Lfs Tap)
           (org.apache.hadoop.io Text)
           (org.apache.hadoop.mapred TextInputFormat TextOutputFormat
                                     OutputCollector JobConf)
           (java.util Properties Map UUID)
           (cascading.clojure ClojureFilter ClojureMapcat ClojureMap
                              ClojureAggregator Util)
           (clojure.lang Var)
           (java.io File)
           (java.lang RuntimeException)))

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
          (ns-fn-name-pair (clojure.core/first v-or-coll))
          (next v-or-coll)))
    :else
      (throw (IllegalArgumentException. (str v-or-coll)))))

(defn- collectify [obj]
  (if (sequential? obj) obj [obj]))

(defn fields
  {:tag Fields}
  [obj]
  (if (or (nil? obj) (instance? Fields obj))
    obj
    (Fields. (into-array String (collectify obj)))))

(defn fields-array
  [fields-seq]
  (into-array Fields (clojure.core/map fields fields-seq)))

(defn pipes-array
  [pipes]
  (into-array Pipe pipes))

(defn- fields-obj? [obj]
  "Returns true for a Fileds instance, a string, or an array of strings."
  (or
    (instance? Fields obj)
    (string? obj)
    (and (sequential? obj) (every? string? obj))))

(defn- idx-of-first [coll pred]
  (clojure.core/first (find-first #(pred (last %)) (indexed coll))))

; in-fields: subset of fields from incoming pipe that are passed to function
;   defaults to all
; func-fields: fields declared to be returned by the function
;   must be given in meta data or as [override-fields #'func-var]
;   no default, error if missing
; out-fields: subset of (union in-fields func-fields) that flow out of the pipe
;   defaults to func-fields

(defn- parse-func [func-obj]
  "func-obj =>
   #'func
   [#'func]
   [override-fields #'func]
   [#'func & params]
   [override-fields #'func & params]"
  (let [func-obj    (collectify func-obj)
        i           (idx-of-first func-obj var?)]
    (assert (<= i 1))
    (let [spec        (fn-spec (drop i func-obj))
          func-var    (nth func-obj i)
          func-fields (or (and (= i 1) (clojure.core/first func-obj))
                          ((meta func-var) :fields))]
      (when-not func-fields
        (throw (Exception. (str "no fields assocaiated with " func-obj))))
      [(fields func-fields) spec])))

(defn- parse-args
  "arr =>
  [func-obj]
  [in-fields func-obj]
  [in-fields func-obj out-fields]
  [func-obj out-fields]"
  [arr]
  (let [i (idx-of-first arr (complement fields-obj?))]
    (assert (<= i 1))
    (let [in-fields          (if (= i 1)
                               (fields (clojure.core/first arr))
                               Fields/ALL)
          [func-fields spec] (parse-func (nth arr i))
          out-fields         (if (< i (dec (clojure.core/count arr)))
                               (fields (last arr))
                               Fields/RESULTS)]
    [in-fields func-fields spec out-fields])))

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

(defn aggregate [#^Pipe previous & args]
  (let [[#^Fields in-fields func-fields specs #^Fields out-fields] (parse-args args)]
    (Every. previous in-fields
      (ClojureAggregator. func-fields specs) out-fields)))

(defn group-by [#^Pipe previous group-fields]
  (GroupBy. previous (fields group-fields)))

(defn first [#^Pipe previous in-fields]
  (Every. previous (fields in-fields) (First.)))

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

;;TODO create join abstractions. http://en.wikipedia.org/wiki/Join_(SQL)
;;"join and drop" is called a natural join - inner join, followed by select to remove duplicate join keys.

;;another kind of join and dop is to drop all the join keys - for example, when you have extracted a specil join key jsut for grouping, you typicly want to get rid of it after the group operation.

;;another kind of "join and drop" is an outer-join followed by dropping the nils

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
        (let [elems     (Util/coerceArrayFromTuple
                          (.selectTuple tuple-entry scheme-fields))
              json-map  (reduce
                          (fn [m i]
                            (assoc m (aget json-keys-arr i)
                                     (aget elems i)))
                          {}
                          (range (alength json-keys-arr)))
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
     (.setProperty props "cascading.serialization.tokens"
                         "130=cascading.clojure.ClojureWrapper")
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
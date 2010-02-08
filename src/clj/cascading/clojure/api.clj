(ns cascading.clojure.api
  (:refer-clojure :exclude (count first filter mapcat map))
  (:use [clojure.contrib.seq-utils :only [find-first indexed]])
  (:import (cascading.tuple Fields)
           (cascading.scheme TextLine)
           (cascading.flow Flow FlowConnector)
           (cascading.operation Identity)
           (cascading.operation.regex RegexGenerator RegexFilter)
           (cascading.operation.aggregator First Count)
           (cascading.pipe Pipe Each Every GroupBy CoGroup)
           (cascading.pipe.cogroup InnerJoin)
           (cascading.scheme Scheme)
           (cascading.tap Hfs Lfs Tap)
           (java.util Properties Map UUID)
           (cascading.clojure ClojureFilter ClojureMapcat ClojureMap
                              ClojureAggregator)
           (clojure.lang Var)
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
  (let [[in-fields _ spec _] (parse-args args)]
    (Each. previous in-fields
      (ClojureFilter. spec))))

(defn mapcat [#^Pipe previous & args]
  (let [[in-fields func-fields spec out-fields] (parse-args args)]
    (Each. previous in-fields
      (ClojureMapcat. func-fields spec) out-fields)))

(defn map [#^Pipe previous & args]
  (let [[in-fields func-fields spec out-fields] (parse-args args)]
    (Each. previous in-fields
      (ClojureMap. func-fields spec) out-fields)))

(defn aggregate [#^Pipe previous in-fields out-fields
                 start aggregate complete]
  (Every. previous (fields in-fields)
    (ClojureAggregator. (fields out-fields)
      (fn-spec start) (fn-spec aggregate) (fn-spec complete))))

(defn group-by [#^Pipe previous group-fields]
  (GroupBy. previous (fields group-fields)))

(defn first [#^Pipe previous]
  (Every. previous (First.)))

(defn count [#^Pipe previous #^String count-fields]
  (Every. previous
    (Count. (fields count-fields))))

(defn inner-join
  ([[#^Pipe lhs #^Pipe rhs] [lhs-fields rhs-fields]]
   (CoGroup. lhs (fields lhs-fields) rhs (fields rhs-fields)
     (InnerJoin.)))
  ([pipes fields declared-fields]
   (CoGroup.
	     (fields-array fields) 
	     (pipes-array pipes)
	     (fields declared-fields) 
	     (InnerJoin.))))

(defn select [#^Pipe previous keep-fields]
  (Each. previous (fields keep-fields) (Identity.)))

(defn text-line
 ([]
  (TextLine. Fields/FIRST))
 ([field-names]
  (TextLine. (fields field-names) (fields field-names))))

(defn path [x]
  (if (string? x) x (.getAbsolutePath x)))

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

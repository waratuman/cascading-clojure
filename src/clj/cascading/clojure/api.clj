(ns cascading.clojure.api
  (:refer-clojure :exclude (count filter mapcat map))
  (:use [clojure.contrib.seq-utils :only [find-first indexed]])
  (:import (cascading.tuple Fields)
           (cascading.scheme TextLine)
           (cascading.flow Flow FlowConnector)
           (cascading.cascade Cascades)
           (cascading.operation Identity)
           (cascading.operation.regex RegexGenerator RegexFilter)
           (cascading.operation.aggregator Count First)
           (cascading.pipe Pipe Each Every GroupBy CoGroup)
           (cascading.pipe.cogroup InnerJoin)
           (cascading.scheme Scheme)
           (cascading.tap Hfs Lfs Tap)
           (java.util Properties Map)
           (cascading.clojure ClojureFilter ClojureMapcat ClojureMap
                              ClojureAggregator)
           (clojure.lang Var)
           (java.util UUID)))

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

(defn- collectify [obj]
  (if (sequential? obj) obj [obj]))

(defn fields
  {:tag Fields}
  [obj]
  (if (or (nil? obj) (instance? Fields obj))
      obj
      (Fields. (into-array String (collectify obj)))))

(defn- fields-obj? [obj]
  "True is string, array of strings, or a fields obj"
  (or
    (instance? Fields obj)
    (string? obj)
    (and (sequential? obj) (every? string? obj))))

(defn- idx-of-first [aseq pred]
  (first (find-first #(pred (last %)) (indexed aseq))))

(defn- parse-func [obj]
  "
  #'func
  [#'func]
  [overridefields #'func]
  [#'func params...]
  [overridefields #'func params...]
  "
  (let
    [obj (collectify obj)
     i (idx-of-first obj var?)
     spec (fn-spec (drop i obj))
     funcvar (nth obj i)
     func-fields (fields (if (> i 0) (first obj) ((meta funcvar) :fields)))]
    [func-fields spec]))

(defn- parse-args
  ([arr] (parse-args arr Fields/RESULTS))
  ([arr defaultout]
  (let
    [i (idx-of-first arr #(not (fields-obj? %)))
     infields (if (> i 0) (fields (first arr)) Fields/ALL)
     [func-fields spec] (parse-func (nth arr i))
     outfields (if (< i (dec (clojure.core/count arr)))
                        (fields (last arr)) defaultout)]
    [infields func-fields spec outfields] )))

(defn- uuid []
  (str (UUID/randomUUID)))

(defn pipe
  "Returns a Pipe of the given name, or if one is not supplied with a
   unique random name."
  ([]
   (Pipe. (uuid)))
  ([#^String name]
   (Pipe. name)))

(defn filter [& args]
  (fn [previous]
    (let [[in-fields _ spec _] (parse-args args)]
      (Each. previous in-fields
        (ClojureFilter. spec)))))

(defn mapcat [& args]
  (fn [previous]
    (let [[in-fields func-fields spec out-fields] (parse-args args)]
    (Each. previous in-fields
      (ClojureMapcat. func-fields spec) out-fields))))

(defn map [& args]
  (fn [previous]
    (let [[in-fields func-fields spec out-fields] (parse-args args)]
    (Each. previous in-fields
      (ClojureMap. func-fields spec) out-fields))))

(defn aggregate [in-fields out-fields
                 start aggregate complete]
  (fn [previous]
    (Every. previous (fields in-fields)
      (ClojureAggregator. (fields out-fields)
        (fn-spec start) (fn-spec aggregate) (fn-spec complete)))))

(defn group-by [group-fields]
  (fn [previous]
    (GroupBy. previous (fields group-fields))))

(defn count [#^String count-fields]
  (fn [previous]
    (Every. previous (Count. (fields count-fields)))))

(defn c-first []
  (fn [previous]
    (Every. previous (First.))))

(defn select [keep-fields]
  (fn [previous]
    (Each. previous (fields keep-fields) (Identity.))))

(defn inner-join [lhs-fields rhs-fields]
  (fn [lhs rhs]
    (CoGroup. lhs (fields lhs-fields) rhs (fields rhs-fields) (InnerJoin.))))


(defn assemble
  ([x] x)
  ([x form] (apply form (collectify x)))
  ([x form & more] (apply assemble (assemble x form) more)))

(defmacro assembly
  ([args return]
    (assembly args [] return))
  ([args bindings return]
    (let [pipify (fn [forms] (if (or (not (sequential? forms))
                                     (vector? forms))
                              forms
                              (cons 'cascading.clojure.api/assemble forms)))
          return (pipify return)
          bindings (vec (clojure.core/map #(%1 %2) (cycle [identity pipify]) bindings))]
      `(fn ~args
          (let ~bindings
            ~return)))))

(defmacro defassembly
  ([name args return]
    (defassembly name args [] return))
  ([name args bindings return]
    `(def ~name (cascading.clojure.api/assembly ~args ~bindings ~return))))

(defn text-line-scheme [field-names]
  (TextLine. (fields field-names) (fields field-names)))

(defn hfs-tap [#^Scheme scheme #^String path]
  (Hfs. scheme path))

(defn taps-map [pipes taps]
  (Cascades/tapsMap (into-array Pipe pipes) (into-array Tap taps)))

(defn mk-flow [sources sinks assembly]
  (let
    [sources (collectify sources)
     sinks (collectify sinks)
     source-pipes (clojure.core/map #(Pipe. (str "spipe" %2)) sources (iterate inc 0))
     tail-pipes (clojure.core/map #(Pipe. (str "tpipe" %2) %1)
                    (collectify (apply assembly source-pipes)) (iterate inc 0))]
     (.connect (FlowConnector.)
        (taps-map source-pipes sources)
        (taps-map tail-pipes sinks)
        (into-array Pipe tail-pipes))))

;; need this anymore?
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

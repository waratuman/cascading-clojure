(ns cascading.clojure.parse
  (:import (cascading.tuple Tuple TupleEntry Fields))
  (:use [clojure.contrib.seq-utils :only [find-first indexed]]))

(defn- collectify [obj]
  (if (sequential? obj) obj [obj]))

(defn- map-vals [f m]
  (into {} (map (fn [[k v]] [k (f v)]) m)))

(defn fields
  {:tag Fields}
  [obj]
  (if (or (nil? obj) (instance? Fields obj))
    obj
    (Fields. (into-array String (collectify obj)))))

(defn- ns-fn-name-pair [fn-var]
  (let [m (meta fn-var)]
    [(str (:ns m)) (str (:name m))]))

(defn- parse-fn-spec* [v-or-c]
  (cond
    (var? v-or-c)
      [v-or-c (ns-fn-name-pair v-or-c)]
    (coll? v-or-c)
      [(first v-or-c)
       (concat
         (ns-fn-name-pair (first v-or-c))
         (next v-or-c))]
    :else
      (throw (IllegalArgumentException. (str v-or-c)))))

(defn parse-fn-spec
   "fn-var-or-coll => var or [var & params]
   Returns a coll of Objects used to serializably describe the function.
   If the argument is a Var, this coll represents that function directly.
   If the argument is a coll, this coll represents the function returned
   by applying its first element, which should be a Var, to the rest of its
   elements."
   [fn-var-or-coll]
   (second (parse-fn-spec* fn-var-or-coll)))

(def opt-defaults
  {:<   Fields/ALL
   :fn> Fields/ARGS
   :>   Fields/RESULTS})

(defn parse-args
  "arr => [fn-var-or-coll (:< in-fields)? (:fn> func-fields)? (:> out-fields)?]
  Returns a map with keys :fn-spec, :<, :fn>, and :>."
  [arr]
  (let [[fn-var-or-coll & opts] arr
	      [fn-var fn-spec] (parse-fn-spec* fn-var-or-coll)
	      raw              (merge
	                         opt-defaults
	                         (select-keys (meta fn-var) [:fn>])
	                         (apply hash-map opts))
	      as-fields        (map-vals fields raw)]
	  (assoc as-fields :fn-spec fn-spec)))

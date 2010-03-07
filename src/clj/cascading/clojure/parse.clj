(ns cascading.clojure.parse
  (:import (cascading.tuple Tuple TupleEntry Fields))
  (:use [clojure.contrib.seq-utils :only [find-first indexed]]))

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

(defn collectify [obj]
  (if (sequential? obj) obj [obj]))

(defn fields
  {:tag Fields}
  [obj]
  (if (or (nil? obj) (instance? Fields obj))
    obj
    (Fields. (into-array String (collectify obj)))))

(defn fields-array
  [fields-seq]
  (into-array Fields (map fields fields-seq)))

(defn- fields-obj? [obj]
  "Returns true for a Fileds instance, a string, or an array of strings."
  (or
    (instance? Fields obj)
    (string? obj)
    (and (sequential? obj) (every? string? obj))))

(defn- idx-of-first [aseq pred]
  (first (find-first #(pred (last %)) (indexed aseq))))

(defn parse-args
  "arr => [func-spec in-fields? :fn> func-fields :> out-fields]
  Returns [in-fields func-fields spec out-fields]"
  ([arr]
    (parse-args arr Fields/RESULTS))
  ([arr defaultout]
     (let [func-args   (first arr)
	         varargs     (rest arr)
	         spec        (fn-spec func-args)
	         func-var    (if (var? func-args)
				                 func-args
				                 (first func-args))
	         defaults    {:fn> (or (:fields (meta func-var)) Fields/ARGS)
		                    :fn spec
		                    :> defaultout}
	         first-elem  (first varargs)
	         keyargs     (if (or (nil? first-elem) (keyword? first-elem))
		                     (apply hash-map (concat [:in Fields/ALL] varargs))
		                     (apply hash-map (concat [:in (first varargs)] (rest varargs))))
	         options     (merge defaults keyargs)
	         fieldsized  (into {} (for [[k v] options]
				                          (if (= k :fn)
				                            [k v]
				                            [k (fields v)])))]
       (vals fieldsized))))

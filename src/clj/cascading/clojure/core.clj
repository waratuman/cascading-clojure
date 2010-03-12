(ns cascading.clojure.core
  (:refer-clojure :exclude (count first filter mapcat map))
  (:import (cascading.tuple Fields)))

(defn fields [& args]
  (Fields. (into-array Comparable args)))

(ns cascading.clojure.utils)

(defn boot-clojure [init-map]
  (println "boot-clojure prepare")
  (require 'clojure.main)
  (require 'cascading.clojure.cascading)
  (require (symbol (init-map "fn-ns-name"))))

(defn reverse-indexed 
  "returns [index, item] tuples from coll starting from back"
  [coll]
  (map vector (iterate dec (- (count coll) 1)) (reverse coll)))
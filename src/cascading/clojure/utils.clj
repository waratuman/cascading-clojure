(ns cascading.clojure.utils)

(defn boot-clojure [init-map]
  (println "boot-clojure prepare")
  (require 'clojure.main)
  (require 'cascading.clojure.cascading)
  (require (symbol (init-map "fn-ns-name"))))
(ns org.parsimonygroup.utils)

(defn boot-clojure [init-map]
  (println "boot-clojure prepare")
  (require 'clojure.main)
  (require 'org.parsimonygroup.cascading)
  (require (symbol (init-map "fn-ns-name"))))



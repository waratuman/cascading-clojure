(ns cascading.clojure.load-all)

(def *all-clj-cascading-libs* '[
  function-bootstrap
  function-filter-bootstrap
  aggregation-bootstrap
  clj-iterator
  join-bootstrap
  java-interop
  makemain-utils
  workflow-structs
  cascading				
  ])

(doseq [name *all-clj-cascading-libs*]
  (require (symbol (str "cascading.clojure." name))))
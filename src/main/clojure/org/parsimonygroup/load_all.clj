(ns org.parsimonygroup.load-all)

(def *all-clj-cascading-libs* '[
  function-bootstrap
  function-filter-bootstrap
  aggregation-bootstrap				
  java-interop
  makemain-utils
  workflow-structs
  cascading				
  ])

(doseq [name *all-clj-cascading-libs*]
  (require (symbol (str "org.parsimonygroup." name))))
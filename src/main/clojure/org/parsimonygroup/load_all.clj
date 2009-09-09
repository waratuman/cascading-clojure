(ns org.parsimonygroup.load-all)

(def *all-clj-cascading-libs* '[
  function-bootstrap
  java-interop
  makemain-utils
  workflow-structs
  join-helper
  cascading				
  ])

(doseq [name *all-clj-cascading-libs*]
  (require (symbol (str "org.parsimonygroup." name))))
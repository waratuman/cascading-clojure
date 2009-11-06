(ns cascading.clojure.load-all)

(def *all-clj-cascading-libs* '[
  function-bootstrap
  function-filter-bootstrap
  aggregation-bootstrap
  clj-iterator
  join-bootstrap
  makemain-utils
  taps
  pipes
  cascading				
  ])

(doseq [name *all-clj-cascading-libs*]
  (require (symbol (str "cascading.clojure." name))))
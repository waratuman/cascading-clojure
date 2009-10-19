 (ns cascading.clojure.boot
  (:use org.parsimonygroup.cascading)
  (:require [org.parsimonygroup.makemain-utils :as m])
  (:gen-class))
	
(defn -main [& args]
  (let [opts (assoc (m/parse-args args) :main-class (class -main))]
      (execute (workflow opts))))
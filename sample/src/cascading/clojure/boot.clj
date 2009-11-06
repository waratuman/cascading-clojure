 (ns cascading.clojure.boot
  (:use cascading.clojure.cascading)
  (:require [cascading.clojure.makemain-utils :as m])
  (:gen-class))
	
(defn -main [& args]
  (let [opts (assoc (m/parse-args args) :main-class (class -main))]
      (execute (workflow opts))))
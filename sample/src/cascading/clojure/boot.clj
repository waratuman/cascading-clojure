 (ns cascading.clojure.boot
  (:require [org.parsimonygroup.cascading :as c])
  (:require [org.parsimonygroup.makemain-utils :as m])
  (:gen-class))
	
(defn- -main [& args]
  (let [opts (assoc (m/parse-args args) :main-class (class -main))]
    (if (:join opts) 
      (c/cascading-join opts)
      (c/cascading opts))))



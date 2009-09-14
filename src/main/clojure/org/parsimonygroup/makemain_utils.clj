(ns org.parsimonygroup.makemain-utils
   (:import [org.apache.commons.cli Options GnuParser OptionBuilder]))

(def cmdline-to-keys {"in" :input "out" :output "ns" :pipeline-ns
                      "wf" :pipeline "join" :join})

(defn mk-options []
  (let [opt (Options.)]
    (doto opt
      (.addOption (do (OptionBuilder/isRequired true)
                      (OptionBuilder/withArgName "in")
                      (OptionBuilder/withDescription
                       "Comma-separated input locations. (2 for joins, 1 normally)")
                      (OptionBuilder/hasArgs)
                      (OptionBuilder/withValueSeparator \,)
                      (OptionBuilder/create "in")))
      (.addOption "out" true "output of job")
      (.addOption "ns" true "namespace of job/workflow definition")
      (.addOption "wf" true "function that makes workflow map")
      (.addOption "join" true "indicates whether this job is a join or not"))))

(defn extract-arg [arg]
  "this extracts values out of a java String[]"
  (cond (nil? arg) arg
        (> (alength arg) 1) (seq arg)
        :else (aget arg 0)))

(defn parse-args [args]
  "gives a map of options back"
  (let [opt (mk-options)
        cmd-line (.parse (GnuParser.) opt (into-array String args))]
    (into {} (for [o (map #(.getOpt %) (.getOptions opt))]
      [(cmdline-to-keys o) (extract-arg (.getOptionValues cmd-line o))]))))



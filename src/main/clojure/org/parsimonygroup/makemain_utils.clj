(ns org.parsimonygroup.makemain-utils
   (:import [org.apache.commons.cli Options GnuParser OptionBuilder]))

(def cmdline-to-keys {"in" :input "out" :output "ns" :fnNsName "wf" :pipeline "join" :join})

(defn mk-options []
  (let [opt (Options.)]
    (doto opt
      (.addOption (do (. OptionBuilder (isRequired (. true booleanValue)))
                      (. OptionBuilder (withArgName "in"))
                      (. OptionBuilder (withDescription "input locations, 1 for normal workflows, 2 for joins, seperate by comma (,)"))
			                (. OptionBuilder hasArgs)
			                (. OptionBuilder (withValueSeparator (. "," (charAt 0))))
                      (. OptionBuilder (create "in"))))
      (.addOption "out" true "output of job")
      (.addOption "ns" true "namespace of job/workflow definition")
      (.addOption "wf" true "function that makes workflow map")
      (.addOption "join" true "indicates whether this job is a join or not"))))

(defn extract-arg [arg]
  "this extracts values out of a java String[]"
  (cond (nil? arg) arg
	(> (alength arg) 1) (seq arg) 
	:else (aget arg 0)))

(defn parseArgs [args]
  "gives a map of options back"
  (let [parser (GnuParser.)
        opt (mk-options)
        cmdLine (. parser parse opt (into-array String args))]
    (into {} (for [o (map #(. % getOpt) (. opt getOptions))]
      [(get cmdline-to-keys o) (extract-arg (. cmdLine getOptionValues o))]))))



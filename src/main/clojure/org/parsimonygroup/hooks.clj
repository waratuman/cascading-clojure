(ns org.parsimonygroup.hooks
  (:import    [cascading.flow FlowListener]))

(defn listen [{to :to with :with}]
 (do (.addListener to with))
 to)

(defn flow-listener 
  ":start is fired when a Flow instance receives the start() message.
   :stop is fired when a Flow instance receives the stop() message.
   :complete is fired when a Flow instance has completed all work whether if was success or failed.
   :error is fired if any child FlowStep throws a Throwable type."
  [{start :start stop :stop complete :complete error :error}]
  (proxy [FlowListener] []
    (onCompleted [flow] (if complete (complete flow)))
    (onStarting [flow] (if start (start flow)))
    (onStopping [flow] (if stop (stop flow)))
    (onThrowable [flow throwable] (if error (error flow throwable)))))

;;TODOs: 
;;on by default
;;1.  do we need stop-on-exit or can we just do everyhting by shelling out in the on completed.
;;2.  how can we get onComplete for cacades rather than flows?

;; (defn stop-on-exit 
;;   "Property stopJobsOnExit will tell the Flow to add a JVM shutdown hook that will kill all running processes if the underlying computing system supports it."
;; [flow properties])
;;   ;;.setStopJobsOnExit(Map<Object,Object> properties, boolean stopJobsOnExit)

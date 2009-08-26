(ns org.parsimonygroup.join-helper
  (:use org.parsimonygroup.workflow-structs))

(defn c-join [{join-fn :using onkeys :on out :to rdr :reader writer :writer numouts :numOutFields} wf1 sugar wf2]
  "Usage : (join {:using join-fn :on [& onkeys] :to outloc} wf1 :with wf2)"
  (merge join-default {:join-wfs [wf1 wf2] :using join-fn :on onkeys :to out :reader rdr :writer writer :numOutFields numouts}))
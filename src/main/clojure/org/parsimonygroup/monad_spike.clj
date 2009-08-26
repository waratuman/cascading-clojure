(ns org.parsimonygroup.monad-spike
  (:use org.parsimonygroup.cascading)
  (:use [clojure.contrib.monads
	 :only (defmonad domonad with-monad maybe-t m-lift m-chain state-m)]))

(defmonad c-pipe
   [m-result (fn m-result-pipe [v] 
	      (cascading-op-maker v))
    m-bind (fn m-bind-pipe [mv f] (f (cascading-op-maker mv))) 

])

; state -> [state, contents]
(defn => [wf]
  [])

(def c-pipe
     (domonad state-m
       [comp (make-start-pipe {:each "blah"})
	bine (makepipe comp {:every 1})
	lala (makepipe lala {})]))


(defn m-pipe [v]
  [v pipe-so-far(v)])


(m-bind-expanded [mv f])	        
  (fn [s] 
   ; (let [[v1 ss1] (mv s)]
      (f [each]
       ((fn [ss] 
	  (let [[v2 ss2] (mv pipe(each))]
	    ((f v2) ss2))) ;pipe(each))))

(m-bind [mv f])	        
  (fn [s] 
    (let [[v1 ss1] (mv s)]
      (f [v1]
       ((fn [ss] 
	  (let [[v2 ss2] (mv ss1)]
	    ((f v2) ss2))) ss1)))


;macro to wrap workflow, and replace the workflow steps with (x# fn(pipesofar/state) [workflowitem, more pipe]), outer macro writes it out as monad?


(ns cascading.clojure.sample
  (:use [clojure.contrib map-utils])
  (:require [clojure.contrib.str-utils2 :as s]))

(defn split-line [line] 
  (let [data (s/split line #"\t")]
    (cond (= 3 (count data)) (list data)
	  :otherwise (list (conj data "dummycontent")))))
(defn identity-each [& line]
  [line])

(defn filter-dummycontent-name [name id]
  (not (= "dummycontent" name)))

(defn test-with-fields []
  {:each {:using split-line 
	  :reader identity 
	  :writer str 
	  :outputFields ["name" "id" "content"]}
   :each {:using identity-each 
	  :reader identity 
	  :writer str 
	  :inputFields ["name" "id"] 
	  :outputFields ["name" "id"]}
   :filter {:using filter-dummycontent-name 
	    :reader identity 
	    :writer str 
	    :inputFields ["name" "id"] 
	    :outputFields ["name" "id"]}})
	
(defn test-with-fields1 []
  {:each {:using split-line 
	  :reader identity 
	  :writer str 
	  :outputFields ["name1" "id1" "content1"]}
   :each {:using identity-each 
	  :reader identity 
	  :writer str 
	  :inputFields ["name1" "id1"] 
	  :outputFields ["name1" "id1"]}
   :filter {:using filter-dummycontent-name 
	    :reader identity 
	    :writer str 
	    :inputFields ["name1" "id1"] 
	    :outputFields ["name1" "id1"]}})

(defn second-of-line [line]
  [[(rand-int 5) (second (s/split line #"\t"))]])

(defn line-tuple [line]
  [[(rand-int 5) 
    [(first (s/split line #"\t")) 
     (second (s/split line #"\t"))]]])

;;note the pattern of getting the first element of the wrapping vector before performing the operation.
(defn append-str [acc nxt]
  (let [seen (first acc)]
    [(str seen nxt)]))

(defn mock-counter [[a b] [c d]]
  [(str a c) (str b d)])

(defn wrap-every [acc nxt]
    [(mock-counter (first acc) nxt)])

;;Note that the field names must match or things blog up.  we should probably just make defaults so that we care about the number n in the n-tuple but not field names.				
(defn groupby-with-fields []
  {:groupBy {:using second-of-line 
	     :reader identity 
	     :writer str 
	     :outputFields ["key" "second"]}
   :everygroup {:using append-str 
		:reader identity 
		:init (fn [] [""]) 
		:writer str 
		:inputFields ["second"]}})

(defn classifier-example []
  {:groupBy {:using line-tuple 
	     :reader identity 
	     :writer pr-str 
	     :outputFields ["key" "second"]}
   :everygroup {:using wrap-every 
		:reader read-string 
		:init (fn [] [["" ""]]) 
		:writer str 
		:inputFields ["second"]}})

(defn sample-join []
  {:wfs [(test-with-fields) (test-with-fields1)] 
   :groupFields [["id"] ["id1"]]
   :using identity
   :outputFields ["id" "name" "id1" "name1"]
   :reader identity
   :writer str
   :wftype :join})

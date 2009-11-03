(ns cascading.clojure.cascading-test
   (:use [cascading.clojure 
	  cascading
	  pipes
	  function-filter-bootstrap])
   (:use [clojure.contrib test-is map-utils])
   (:require [clojure.contrib.str-utils2 :as s])
   (:import [cascading.pipe Pipe Each]
	    [cascading.flow Flow]
	    [cascading.clojure
	     FunctionFilterBootstrapInClojure]
	    [cascading.tuple Fields]))

(defn split-line [line] 
  (let [data (s/split line #"\t")]
    (cond (= 3 (count data)) (list data)
	  :otherwise (list (conj data "dummycontent")))))

(defn identity-each [& line]
  [line])

(defn second-of-line [line]
  [[(second (s/split line #"\t"))]])

(defn filter-dummycontent-name [name id]
  (not (= "dummycontent" name)))

(def test-with-fields
  {:each {:using split-line :reader identity :writer str :outputFields ["name" "id" "content"]}
		:each {:using identity-each :reader identity :writer str :inputFields ["name" "id"] :outputFields ["name" "id"]}
		:filter {:using filter-dummycontent-name :reader identity :writer str :inputFields ["name" "id"] :outputFields ["name" "id"]}})
	
(def test-with-fields1
  {:each {:using split-line :reader identity :writer str :outputFields ["name" "id" "content"]}
		:each {:using identity-each :reader identity :writer str :inputFields ["name" "id"] :outputFields ["name" "id"]}
		:filter {:using filter-dummycontent-name :reader identity :writer str :inputFields ["name" "id"] :outputFields ["name1" "id1"]}})

(def wf1 {:each {:using identity :reader identity :writer str :outputFields ["name" "id" "content"]}
		:each {:using identity :reader identity :writer str :inputFields ["name" "id"] :outputFields ["name" "id"]}
		:filter {:using (constantly true) :reader identity :writer str :inputFields ["name" "id"] :outputFields ["name1" "id1"]}})

(def sample-join
  {:wfs [test-with-fields test-with-fields1] 
   :groupFields [["id"] ["id1"]] ;fields
   :using (fn [id name id1 name1] [id name id1 name1])
   :outputFields ["id" "name" "id1" "name1"]
   :wftype :join})

(deftest mk-pipe-test
  (let [p (mk-pipe "test" "dummy-ns" test-with-fields)]
    (is (= (Fields. (into-array String ["name" "id"])) (.getFieldDeclaration p)))))

(deftest mk-wf-test
  (let [wf (workflow "in" "out" #'test-with-fields)]
    (is (= Flow (class wf)))))

(deftest mk-workflow-join-test
  (let [wf (workflow ["in1" "in2"] "out" #'sample-join)
	ops (.getAllOperations 
			  (first (.getSteps wf)))
	filter-ops (filter 
		 #(= FunctionFilterBootstrapInClojure (class %))
		 ops)
	tapmap (.getSources wf)]

    ;;there are two incoming sources
    (is (= 2 (count tapmap)))

    ;;the function filters are for "id" and "id1"
    (let [fields (into #{}
		       (map 
		  #(.print (.getFieldDeclaration %)) 
		  filter-ops))]
    (is (contains? fields "['name1', 'id1']"))
    (is (contains? fields "['name', 'id']"))) 

    (is (= 2 (count filter-ops)))
    (is (= 6 (count ops)))))
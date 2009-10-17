(ns cascading.clojure.cascading-test
   (:use [cascading.clojure cascading])
   (:use [clojure.contrib test-is map-utils])
   (:require [clojure.contrib.str-utils2 :as s])
   (:import [cascading.pipe Pipe Each]
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
  {:operations {:each {:using split-line :reader identity :writer str :outputFields ["name" "id" "content"]}
		:each {:using identity-each :reader identity :writer str :inputFields ["name" "id"] :outputFields ["name" "id"]}
		:filter {:using filter-dummycontent-name :reader identity :writer str :inputFields ["name" "id"] :outputFields ["name" "id"]}}})
	
(def test-with-fields1
  {:operations {:each {:using split-line :reader identity :writer str :outputFields ["name" "id" "content"]}
		:each {:using identity-each :reader identity :writer str :inputFields ["name" "id"] :outputFields ["name" "id"]}
		:filter {:using filter-dummycontent-name :reader identity :writer str :inputFields ["name" "id"] :outputFields ["name1" "id1"]}}})

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
  (let [p (mk-pipe "test" "dummy-ns" (:operations test-with-fields))]
    (is (= (Fields. (into-array String ["name" "id"])) (.getFieldDeclaration p)))))

(deftest mk-wf-test
  (let [input-wf test-with-fields
	wf (mk-workflow "dummy-ns" "in" "out" input-wf)
	pipe (:pipe wf)]
    (is (= Each (class pipe)))))

(deftest mk-workflow-join-test
  (let [executable-wf (mk-workflow "ns" ["in1" "in2"] "out" sample-join)
	actual-join-pipe (:pipe executable-wf)
	tapmap (:tap executable-wf)
	grpSelectors (vals (.. actual-join-pipe getGroupingSelectors))]
    (is (= 2 (count tapmap)))
    (is (= 2 (count grpSelectors)))
    (is (= [(Fields. (into-array String ["id"])) (Fields. (into-array String ["id1"]))]
	   grpSelectors))))
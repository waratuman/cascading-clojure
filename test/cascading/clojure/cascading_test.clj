(ns cascading.clojure.cascading-test
   (:use [cascading.clojure 
	  cascading
          taps
	  pipes
	  function-filter-bootstrap
          io])
   (:use [clojure.contrib map-utils logging])
   (:use clojure.test)
   (:require [clojure.contrib.str-utils2 :as s])
   (:import [cascading.pipe Pipe Each]
	    [cascading.flow Flow]
	    [cascading.clojure
	     FunctionFilterBootstrapInClojure]
	    [cascading.tuple Fields]
            [org.apache.log4j
             ConsoleAppender Logger SimpleLayout Level LogManager]))

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

(deftest build-workflow-from-symbol
  (let [wf (workflow "in" "out" #'test-with-fields)]
    (is (= Flow (class wf)))))

(deftest build-join-from-symbol
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

(def classifier-example
  {:groupBy {:using (fn [x] [:a (apply + x)])
	     :reader read-string
	     :writer pr-str 
	     :outputFields ["key" "second"]}
   :everygroup {:using +
		:reader read-string 
		:init (fn [] [[0 0]]) 
		:writer str 
		:inputFields ["second"]}})

(def grouper-example
  {:groupBy {:using (fn [x] [:a (apply + x)])
	     :reader read-string
	     :writer pr-str 
	     :outputFields ["key" "second"]}})

;;TODO: this is a shitty assert, come up with better way to assert
;;proper composition of assemblies and pipes.
(deftest build-grouper
  (let [grouper (mk-pipe
                 (str (.name *ns*))
                 grouper-example)]
    (is (= cascading.clojure.FunctionBootstrap
           (class (.getOperation (first (.getHeads grouper))))))))

(defn hadoop-logger []
  (let [r (Logger/getRootLogger)]
    (doto r (.setLevel Level/DEBUG)
          (.addAppender
           (ConsoleAppender.
            (SimpleLayout.))))))

(deftest grouping-test
  (with-tmp-files [in  (temp-dir "source")
                   out (temp-path "sink")]
    (let [lines [[1 2] [1 2 3]]
          logging ( hadoop-logger)
          _ (log :debug "foo")
          file (write-lines-in in "some.data" lines)
          copied (.openSink 
                  (execute 
                   (workflow
                    test-tap in out #'grouper-example)))]
      (is (= 3
             (read-tuple (.next copied))))
      (is (= 6
             (read-tuple (.next copied))))
      (is (not (.hasNext copied))))))

;; (deftest write-read-classifier
;;   (with-tmp-files [in  (temp-dir "source")
;;                    out (temp-path "sink")]
;;     (let [lines [[1 2] [1 2 3]]
;;           file (write-lines-in in "some.data" lines)
;;           copied (.openSink 
;;                   (execute 
;;                    (workflow
;;                     test-tap in out #'classifier-example)))]
;;       (is (= 3
;;              (read-tuple (.next copied))))
;;       (is (= 6
;;              (read-tuple (.next copied))))
;;       (is (not (.hasNext copied))))))

(def foo 10)

(deftest get-ns-and-name-from-symbol
  (is (= ['cascading.clojure.cascading-test 'foo] (var-symbols #'foo))))

(ns cascading.clojure.scenarios
  (:use clojure.test
        cascading.clojure.testing)
  (:import (cascading.tuple Fields)
           (cascading.pipe Pipe)
           (cascading.clojure Util ClojureMap))
  (:require (clj-json [core :as json]))
  (:require [clojure.contrib.duck-streams :as ds])
  (:require [clojure.contrib.java-utils :as ju])
  (:require (cascading.clojure [api :as c])))

;;notes - self joining requires duplicationg a tap pointed at the same source.

(deftest grouby-and-left-join-in-stages
  (test-flow
    (in-pipes {"p1" ["x" "y" "num"]
               "p2" ["x" "y" "num"]
               "p3" ["x" "y" "num"]})
    (in-tuples {"p1" [[0 1 5] [2 1 6] [0 1 7] [4 7 9]]
                "p2" [[0 1 1] [2 1 2]]
                "p3" [[2 1 7] [0 1 8]]})
    (fn [{p1 "p1" p2 "p2" p3 "p3"}]
      (let [grouped (c/group-by p1 ["x" "y"])
           joined1 (c/left-join [grouped p2]
                     [["x"]["x"]]
                     ["x1" "y1" "num1"
                      "x2" "y2" "num2"])
           joined2 (c/left-join [joined1 p3]
                     [["x1"]["x"]]
                     ["x1" "y1" "num1"
                      "x2" "y2" "num2"
                      "x3" "y3" "num3"])]
        (c/select joined2 ["num1" "num2" "num3"])))
    [[5 1 8] [7 1 8] [6 2 7] [9 nil nil]]))

(deftest grouby-join-into-left-in-one-stage
  (test-flow
    (in-pipes {"p1" ["x" "y" "num"]
               "p2" ["x" "y" "num"]
               "p3" ["x" "y" "num"]})
    (in-tuples {"p1" [[0 1 5] [2 1 6] [0 1 7] [4 7 9]]
                "p2" [[0 1 1] [2 1 2] [99 99 99]]
                "p3" [[2 1 7] [0 1 8] [6 7 11]]})
    (fn [{p1 "p1" p2 "p2" p3 "p3"}]
      (let [grouped (c/group-by p1 ["x" "y"])
            joined (c/join-into [grouped p2 p3]
                     [["x"]["x"]["x"]]
                     ["x1" "y1" "num1"
                      "x2" "y2" "num2"
                      "x3" "y3" "num3"])]
    (c/select joined ["num1" "num2" "num3"])))
    [[5 1 8] [7 1 8] [6 2 7] [9 nil nil]]))

(deftest groupyby-with-sorting
  (test-flow
    (in-pipes {"p1" ["x" "y" "num"]
               "p2" ["x" "y" "num"]})
    (in-tuples {"p1" [[0 1 5] [2 1 6] [0 1 7] [1 7 9]]
                "p2" [[0 1 7] [2 1 5] [0 1 6] [1 7 9]]})
    (fn [{p1 "p1" p2 "p2"}]
      (let [groups-left  (c/group-by p1 ["x"] ["num"])
	          groups-right (c/group-by p2 ["y"] ["num"])
            joined (c/join-into [groups-left groups-right]
                     [["x"]["y"]]
                     ["x1" "y1" "num1"
                      "x2" "y2" "num2"])]
    (c/select joined ["num1" "num2"])))
    [[5 nil] [7 nil] [9 5] [9 6] [9 7] [6 nil]]))

(defn x-and-y [x y z]
  (str x y))

(defn y-and-z [x y z]
  (str y z))

(deftest merging-different-groups
  (test-flow
    (in-pipes {"p1" ["x" "y" "z"]
               "p2" ["x" "y" "z"]})
    (in-tuples {"p1" [[0 1 5] [2 1 6] [0 1 7] [2 1 9]]
                "p2" [[0 0 1] [2 2 1] [0 2 1] [3 0 1]]})
    (fn [{p1 "p1" p2 "p2"}]
      (let [new-left  (-> p1 (c/extract #'x-and-y :fn> "dualgroup"))
	          new-right (-> p2 (c/extract #'y-and-z :fn> "dualgroup"))]
	      (c/group-by [new-left new-right] ["dualgroup"] ["z"])))
    [[0 0 1 "01"] [3 0 1 "01"] [0 1 5 "01"] [0 1 7 "01"]
     [2 2 1 "21"] [0 2 1 "21"] [2 1 6 "21"] [2 1 9 "21"]]))

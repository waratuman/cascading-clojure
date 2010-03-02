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

;; (deftest group-and-aggregate-with-pred
;;   "group on x.
;;  pred ensures we have data for y.
;;  max on num."
;;   (test-flow
;;    (in-pipes {"p1" ["x" "y" "num"]})
;;    (in-tuples {"p1" [[0 1 5] [2 1 6] [0 nil 7]
;;         [0 1 1] [2 1 2] [2 nil 7] [0 nil 8]]})
;;    (fn [{p1 "p1"}]
;;      (let [grouped (c/group-by p1 ["x"])]
;;        (c/aggregate grouped
;;        ["x" "y" "num"] [["maxx" "maxy" "max"] #'predicated-max] ["maxx" "maxy" "max"])))
;;        [[0 5] [2 6]]))

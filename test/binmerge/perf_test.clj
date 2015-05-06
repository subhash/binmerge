(ns binmerge.perf-test
  (:require [clojure.test :refer :all]
            [binmerge.core :refer :all]
            [clojure.java.io :refer [file output-stream input-stream reader]]))

(def perf-output "resources/data/perf-output")
(def million-objects "resources/data/million_objects")
(def million-attributes "resources/data/million_attributes")

(defn same-content? [f1 f2]
  (let [b1 (byte-array 100)
        b2 (byte-array 100)
        r1 (.read f1 b1 0 100)
        r2 (.read f2 b2 0 100)]
    (cond
     (and (= r1 -1) (= r2 -1)) true
     (or  (= r1 -1) (= r2 -1)) false
     (= (seq b1) (seq b2)) (recur f1 f2)
     :else false)))

(deftest test-object-iteration
  (testing "20 million objects don't fit in memory"
    (println "Trying to load 20 mil objects" (java.util.Date.))
    (is (thrown?
         OutOfMemoryError
         (doall (map slurp (repeat 20 million-objects))))))
  (testing "20 million objects merged in memory"
    (println "Trying to merge 20 mil objects" (java.util.Date.))
    (merge-bin (repeat 20 million-objects) perf-output)
    (println "Checking same content" (java.util.Date.))
    (is (same-content? (input-stream perf-output) (input-stream million-objects)))))


(deftest test-attr-iteration
  (testing "20 million attributes don't fit in memory"
    (println "Trying to load 20 objects with a million attrs each" (java.util.Date.))
     (is (thrown?
          OutOfMemoryError
          (doall (map slurp (repeat 20 million-attributes))))))
  (testing "20 million attributes merged in memory"
    (println "Trying to merge 20 million attrs" (java.util.Date.))
    (merge-bin (repeat 20 million-attributes) perf-output)
    (println "Checking same content" (java.util.Date.))
    (is (same-content? (input-stream perf-output) (input-stream million-attributes)))))

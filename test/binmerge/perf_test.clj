(ns binmerge.perf-test
  (:require [clojure.test :refer :all]
            [binmerge.core :refer :all]
            [clojure.java.io :refer [file output-stream input-stream reader]]))

(def perf-output "resources/data/perf-output")
(def million-objects "resources/data/million_objects")

(defn same-content [files]
  (->> (map (comp byte-seq input-stream file) files)
       (apply =)))

(deftest memory-footprint-test
  (testing "20 million objects don't fit in memory"
    (is (thrown?
         OutOfMemoryError
         (doall (map slurp (repeat 20 million-objects))))))
  (testing "20 million objects merged in memory"
    (merge-bin (repeat 20 million-objects) perf-output)
        (is (same-content [perf-output million-objects]))))

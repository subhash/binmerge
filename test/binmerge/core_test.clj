(ns binmerge.core-test
  (:require [clojure.test :refer :all]
            [binmerge.core :refer :all]
            [clojure.java.io :refer [file output-stream input-stream reader]]))

(def table1 "resources/data/table1")
(def table2 "resources/data/table2")
(def table3 "resources/data/table3")
(def output "resources/data/output")
(def test-output "resources/data/test-output")

(def sample-a "resources/data/a")
(def sample-b "resources/data/b")


(defn same-content [files]
  (->> (map (comp byte-seq input-stream file) files)
       (apply =)))

(deftest given-test
  (testing "Given test - merge table1, table2 and table3 to output"
    (let [inputs [table1 table2 table3]]
      (spit test-output "")
      (merge-bin inputs test-output)
      (is (same-content [test-output output])))))

(deftest degenerate-tests
  (testing "Degenerate test cases"
    (spit test-output "")
    (merge-bin [table1] test-output)
    (is (same-content [test-output table1]))
    (spit test-output "")
    (merge-bin [table2 table2] test-output)
    (is (same-content [test-output table2]))))

(deftest test-marshalling
  (let [a {:name "a" :attr [["age" "20"]]}]
    (spit sample-a "")
    (testing "Testing obj->bin"
      (with-open [out (java.io.FileOutputStream. sample-a)]
        (obj->bin a out)))
    (testing "Testing bin->objs"
      (is (= [a] (bin->objs sample-a))))))

(deftest test-basic-merge
  (testing "Testing merge"
    (let [a {:name "a" :attr [["age" "20"]]}
          b {:name "b" :attr [["occupation" "programmer"]]}]
      (for [f [sample-a sample-b test-output]] (spit f ""))
        (with-open [out (java.io.FileOutputStream. sample-a)]
          (obj->bin a out))
        (with-open [out (java.io.FileOutputStream. sample-b)]
          (obj->bin b out))
        (merge-bin [sample-a sample-b] test-output)
        (is (= [a b] (bin->objs test-output))))))

(deftest test-object-merge
  (testing "Duplicate objects should merge into one"
    (let [a {:name "a" :attr [["age" "20"]]}
          dup-a {:name "a" :attr [["occupation" "artist"]]}
          b {:name "b" :attr [["occupation" "programmer"]]}
          merged-a (assoc a :attr (concat (:attr a) (:attr dup-a)))]
      (for [f [sample-a sample-b test-output]] (spit f ""))
        (with-open [out (java.io.FileOutputStream. sample-a)]
          (obj->bin a out))
        (with-open [out (java.io.FileOutputStream. sample-b)]
          (obj->bin dup-a out)
          (obj->bin b out))
        (merge-bin [sample-a sample-b] test-output)
        (is (= [merged-a b] (bin->objs test-output))))))

(deftest test-merge-order
  (testing "Merge should preserve ASCII order"
    (let [o [{:name "o1" :attr [["age" "20"]]}
             {:name "o2" :attr [["age" "20"]]}
             {:name "o3" :attr [["age" "20"]]}
             {:name "o4" :attr [["age" "20"]]}
             {:name "o5" :attr [["age" "20"]]}]]
      (for [f [sample-a sample-b test-output]] (spit f ""))
        (with-open [out (java.io.FileOutputStream. sample-a)]
          (map #(obj->bin (nth o %) out) [2 3]))
        (with-open [out (java.io.FileOutputStream. sample-b)]
          (map #(obj->bin (nth o %) out) [1 4 5]))
        (merge-bin [sample-a sample-b] test-output)
        (is (= o (bin->objs test-output))))))

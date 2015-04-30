(ns binmerge.core)

(require '[clojure.java.io :refer [file output-stream input-stream reader]])

(def in (input-stream (file "/Users/subhash/Downloads/table1")))

(defn byte-seq [in]
  (take-while (partial <= 0) (repeatedly #(.read in))))

(defn bytes->int [b]
  (-> (map byte b) (byte-array) (biginteger)))

(defn read-obj [s]
  (let [x (->> s (take 8)(bytes->int))
        name (->> (drop 8 s) (take x) (map char) (apply str))
        n (->> (drop (+ 8 x) s) (take 8) (bytes->int))]
    [name n]))

(-> (read-obj (byte-seq in)) )

(ns binmerge.core)

(require '[clojure.java.io :refer [file output-stream input-stream reader]])

(def in (input-stream (file "/Users/subhash/Downloads/output")))

(defn byte-seq [in]
  (take-while (partial <= 0) (repeatedly #(.read in))))

(defn bytes->int [b]
  (-> (map byte b) (byte-array) (biginteger)))

(defn bytes->string [b]
  (->> (map char b) (apply str)))

(defn read-obj [s objs]
  (let [x (->> s (take 8) bytes->int)
        name (->> (drop 8 s) (take x) bytes->string)
        n (->> (drop (+ 8 x) s) (take 8) bytes->int)
        [rs attrs] (reduce (fn [[s a] _] (read-attr s a)) [(drop (+ 8 x 8) s) {}] (range 0 n))
        ]
    [rs (assoc objs name attrs)]))

(defn read-attr [s attrs]
  (let [y (->> s (take 8) bytes->int)
        k (->> (drop 8 s) (take y) bytes->string)
        z (->> (drop (+ 8 y) s) (take 8) bytes->int)
        v (->> (drop (+ 8 y 8) s) (take z) bytes->string)]
    [(drop (+ 8 y 8 z) s) (assoc attrs k v)]))




(second (reduce (fn [[s a] _] (read-obj s a))  [(byte-seq in) {}] (range 0 3)))

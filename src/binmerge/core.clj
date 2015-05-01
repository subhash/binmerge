(ns binmerge.core)

(require '[clojure.java.io :refer [file output-stream input-stream reader]])

(import '[java.nio ByteBuffer])

(def in (input-stream (file "/Users/subhash/Downloads/table1")))

(defn byte-seq [in]
  (take-while (partial <= 0) (repeatedly #(.read in))))

(defn bytes->int [b]
  (-> (map byte b) (byte-array) (biginteger)))

(defn int->bytes [i]
  (-> (biginteger i) (.toByteArray)))

(defn bytes->string [b]
  (->> (map char b) (apply str)))



(defn pull-name [s]
  (let [[xseq xrest] (split-at 8 s)
        x (bytes->int xseq)
        [nseq nrest] (split-at x xrest)
        n (bytes->string nseq)]
    [n (concat xseq nseq) nrest]))

(defn pull-attr-no [s]
  (let [[aseq arest] (split-at 8 s)
        a (bytes->int aseq)]
    [a aseq arest]))

(defn pull-name-value [m acc s]
  (let [[n nseq nrest] (pull-name s)
        [v vseq vrest] (pull-name nrest)]
    [(assoc m n v) (concat acc nseq vseq) vrest]))


(defn merge-attrs [a an b bn acc]
  (cond
   (<= an 0)
   (if (<= bn 0) nil
     (let [[n nseq nrest] (pull-name b)
           [v vseq vrest] (pull-name nrest)]
       (recur a an vrest (dec bn) (concat acc nseq vseq))))
   (<= bn 0) (recur b bn a an acc)
   ))


(defn pull-obj [s]
  (if-let [s (seq s)]
    (let [[n nseq nrest] (pull-name s)
          [a aseq arest] (pull-attr-no nrest)
          [attr attr-seq attr-rest] (reduce (fn [[m acc s] _] (pull-name-value m acc s)) [{} [] arest] (range 0 a))]
      [s n (concat nseq aseq attr-seq) attr-rest])))


(defn merger [seqs]
  (let [mgr (fn []
              (when-let [[f & r] (->> (map pull-obj seqs) (filter identity) (sort-by second) seq)]
                (do (println (second f)) (merger (conj (map first r) (last f))))))]
    mgr))


(def files ["/Users/subhash/Downloads/table1" "/Users/subhash/Downloads/table2" "/Users/subhash/Downloads/table3"])

(let [mgr (merger (map (comp byte-seq input-stream file) (reverse files)))]
  (trampoline mgr))


 (-> ((comp byte-seq input-stream file) (first files)) pull-obj last pull-obj last pull-obj last pull-obj)

(defn merge-seq [a b acc]
  (let [[an anseq aa aaseq aattr aattr-seq arest] (pull-obj a)
        [bn bnseq ba baseq battr battr-seq brest] (pull-obj a)
        c (compare an bn)]
    (cond
     (< c 0)
     (recur arest b (concat acc anseq aaseq aattr-seq))
     (> c 0)
     (recur a brest (concat acc bnseq baseq battr-seq))
     :else
     1)))


(->> [[1 2 3] [4 5 6] [7 9]] (vector :begin))



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



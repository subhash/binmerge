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


(defn pull-obj [s]
  (if-let [s (seq s)]
    (let [[n nseq nrest] (pull-name s)
          [a aseq arest] (pull-attr-no nrest)
          [attr attr-seq attr-rest] (reduce (fn [[m acc s] _] (pull-name-value m acc s)) [{} [] arest] (range 0 a))]
      [s n (concat nseq aseq attr-seq) attr-rest])))


(defn merger [seqs out]
  (fn []
    (when-let [[f & r] (->> (map pull-obj seqs)
                            (filter identity)
                            (sort-by second)
                            seq)]
      (do
        (println (second f))
        (.write out (byte-array (nth f 2)))
        (merger (conj (map first r) (last f)) out)))))


(def files ["/Users/subhash/Downloads/table1" "/Users/subhash/Downloads/table2" "/Users/subhash/Downloads/table3"])

(type (-> (byte-seq in) (byte-array)))

(with-open [out (java.io.FileOutputStream. "/tmp/see")]
  (let [mgr (merger (map (comp byte-seq input-stream file) (reverse files)) out)]
    (trampoline mgr)))





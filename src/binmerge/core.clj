(ns binmerge.core)

(require '[clojure.java.io :refer [file output-stream input-stream reader]])

(import '[java.nio ByteBuffer])

(def in (input-stream (file "/Users/subhash/Downloads/table1")))

(def files ["/Users/subhash/Downloads/table1" "/Users/subhash/Downloads/table2" "/Users/subhash/Downloads/table3"])

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


(defn name-iter [s]
  (when (seq s)
    #(let [[n nseq nrest] (pull-name s)
           [a aseq arest] (pull-attr-no nrest)]
       {:name n
        :seq (concat nseq aseq)
        :next (attr-iter arest a)
        :prev s})))


(defn attr-iter [s n]
  #(if (and (> n 0) (seq s))
     (let [[k kseq krest] (pull-name s)
           [v vseq vrest] (pull-name krest)]
       {:key k
        :val v
        :seq (concat kseq vseq)
        :next (attr-iter vrest (dec n))})
     {:next-obj s}))


(defn merge-attr [iters acc]
  (let [[done cont] (split-with :next-obj (map #(%) iters))]
    (if (seq cont)
      (let [o (->> cont (remove :next-obj) (sort-by :key))
            [f r] (split-with #(= (-> o first :key) (-> % :key)) o)]
        (do
          (println "...." (:key (first f)) (:val (first f)))
          (merge-attr (concat (map :next f) (map :seq r)) (concat acc done))))
      (concat acc done))))

(defn merge-objs [iters]
  (when (seq iters)
  (let [o (->> (map #(%) iters)
               (filter identity)
               (sort-by :name))
        [f r] (split-with #(= (-> o first :name) (-> % :name)) o)]
    (do
      (println "Object " (:name (first f)))
      (->> (merge-attr (map :next f) [])
           (map :next-obj)
           (concat (map :seq r))
           (map name-iter)
           (filter identity)
           (merge-objs))))))

 (merge-objs (map (comp name-iter byte-seq input-stream file) files))





 (comment

(defn pull-name-value [m acc s]
  (let [[n nseq nrest] (pull-name s)
        [v vseq vrest] (pull-name nrest)]
    [(assoc m n v) (concat acc nseq vseq) vrest]))


(defn pull-attr [s]
  (if-let [s (seq s)]
    (let [[n nseq nrest] (pull-name s)
          [v nseq vrest] (pull-name nrest)]
      [n v vrest s])))

(defn pull-obj [s]
  (if-let [s (seq s)]
    (let [[n nseq nrest] (pull-name s)
          [a aseq arest] (pull-attr-no nrest)
          [attr attr-seq attr-rest] (reduce (fn [[m acc s] _] (pull-name-value m acc s)) [{} [] arest] (range 0 a))]
      [s n (concat nseq aseq attr-seq) attr-rest])))


(defn attr-merger [seqs]
  (fn []
    (when-let [[f & r] (->> (map pull-attr seqs) (filter identity) (sort-by first) seq)]
      (do
        (println (first f) (last f))
        (attr-merger (conj (map last r) (nth f 2)))))))


(defn merger [seqs out]
  (fn []
    (when-let [[[_ n _ _] & r :as full]
               (->> (map pull-obj seqs)
                            (filter identity)
                            (sort-by second)
                            seq)]
      (let [[f r] (split-with #(= n (second %)) full)
            [_ n out-seq next-seq] (merge-attr f)]
        (do
          (println (second (first f)))
          (.write out (byte-array out-seq))
          (merger (conj (map first r) next-seq) out))))))



;(spit "/tmp/see" "")
;(with-open [out (java.io.FileOutputStream. "/tmp/see")]
;  (let [mgr (merger (map (comp byte-seq input-stream file) (reverse files)) out)]
;    (trampoline mgr)))
;(slurp "/tmp/see")

)



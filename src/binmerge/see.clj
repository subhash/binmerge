(ns binmerge.see)
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

(defn pull-string [s]
  (let [[xseq xrest] (split-at 8 s)
        x (bytes->int xseq)
        [nseq nrest] (split-at x xrest)
        n (bytes->string nseq)]
    [n (concat xseq nseq) nrest]))

(defn pull-int [s]
  (let [[aseq arest] (split-at 8 s)
        a (bytes->int aseq)]
    [a aseq arest]))


(defn attr-iter [s n]
  #(when (seq s)
     (let [[k kseq krest] (pull-string s)
           [v vseq vrest] (pull-string krest)]
       {:type :attr
        :key k
        :val v
        :seq (concat kseq vseq)
        :prev (attr-iter s n)
        :next (if (> n 1)
                (attr-iter vrest (dec n))
                (obj-iter vrest))})))


(defn merge-attr [iters obj-iters]
  (let [[apulls opulls] (->> (map #(%) iters) (filter identity) (sort-by :type) (split-with #(= (:type %) :attr)))]
    (if (seq apulls)
      (let [pulls (sort-by :key apulls)
            [f r] (split-with #(= (-> pulls first :key) (% :key)) pulls)
            foo (println "merged attr - " (:key (first f)) (:val (first f )))
            merged [(:next (first f))]
            unmerged (map :prev r)]
        #(merge-attr
          (concat merged unmerged)
          (concat obj-iters (map :prev opulls))))
      #(merge-obj (concat obj-iters (map :prev opulls))))))


(defn obj-iter [s]
  #(when (seq s)
     (let [[n nseq nrest] (pull-string s)
          [a aseq arest] (pull-int nrest)
           ]
      {:type :obj
       :name n
       :seq (concat nseq aseq)
       :next (attr-iter arest a)
       :prev (obj-iter s)})))



(defn merge-obj [iters]
  (when (seq iters)
    (let [pulls (->> (map #(%) iters) (filter identity) (sort-by :name))
          [f r] (split-with #(= (-> pulls first :name) (% :name)) pulls)
          foo (println "merged obj - " (-> pulls first :name))
          merged (->> (map :next f) )
          unmerged (->> (map :prev r))
          ]
      #(merge-attr merged unmerged)

    )))

(def prep (comp obj-iter byte-seq input-stream file))

(trampoline (merge-obj (map prep files)))

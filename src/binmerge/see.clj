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
  (let [[attrs objs] (->> (map #(%) iters) (filter identity) (sort-by :type) (split-with #(= (:type %) :attr)))]
    (if (seq attrs)
      (let [sorted (sort-by :key attrs)
            [now later] (split-with #(= (-> sorted first :key) (% :key)) sorted)]
        (println "merged attr - " (:key (first now)) (:val (first now )))
        #(merge-attr
          (concat [(:next (first now))] (map :prev later))
          (concat obj-iters (map :prev objs))))
      #(merge-obj (concat obj-iters (map :prev objs))))))


(defn obj-iter [s]
  #(when (seq s)
     (let [[n nseq nrest] (pull-string s)
          [a aseq arest] (pull-int nrest)]
      {:type :obj
       :name n
       :seq (concat nseq aseq)
       :next (attr-iter arest a)
       :prev (obj-iter s)})))


(defn merge-obj [iters]
  (when (seq iters)
    (let [objs (->> (map #(%) iters) (filter identity) (sort-by :name))
          [now later] (split-with #(= (-> objs first :name) (% :name)) objs)]
      (println "merged obj - " (-> objs first :name))
      #(merge-attr (map :next now) (map :prev later)))))

(def prep (comp obj-iter byte-seq input-stream file))

(trampoline (merge-obj (map prep files)))

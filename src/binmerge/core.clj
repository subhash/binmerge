(ns binmerge.core
  (:require [clojure.java.io :refer [file output-stream input-stream reader]]))

(defn byte-seq [in]
  (take-while (partial <= 0) (repeatedly #(.read in))))

(defn bytes->int [b]
  (-> (map byte b) (byte-array) (biginteger)))

(defn int->bytes [i]
  (let [s (-> (biginteger i) .toByteArray seq)]
    (byte-array (concat (repeat (- 8 (count s)) 0) s))))

(defn bytes->string [b]
  (->> (map char b) (apply str)))

(defn string->bytes [s]
  (-> (map byte s) byte-array))

(type (string->bytes "foo"))

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

(declare obj-iter)

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

(declare merge-obj)

(defn merge-attr [iters obj-iters acc out]
  (let [[attrs objs] (->> (map #(%) iters) (filter identity) (sort-by :type) (split-with #(= (:type %) :attr)))]
    (if (seq attrs)
      (let [sorted (sort-by :key attrs)
            [now later] (split-with #(= (-> sorted first :key) (% :key)) sorted)
            selected (first now)]
        #(merge-attr
          (concat [(:next selected)] (map :prev later))
          (concat obj-iters (map :prev objs))
          (conj acc selected)
          out))
      (do
        ;(println "merged attrs - " (count acc) (map :key acc))
        (.write out (int->bytes (count acc)))
        (doseq [a acc] (.write out (byte-array (:seq a))))
        #(merge-obj (concat obj-iters (map :prev objs)) out)))))


(defn obj-iter [s]
  #(when (seq s)
     (let [[n nseq nrest] (pull-string s)
          [a aseq arest] (pull-int nrest)]
      {:type :obj
       :name n
       :attr-no a
       :seq nseq
       :next (attr-iter arest a)
       :prev (obj-iter s)})))

(defn full-obj-iter [s]
  #(when (seq s)
     (let [[n _ nrest] (pull-string s)
          [a _ arest] (pull-int nrest)
          [attr onext] (reduce
                    (fn [[acc aseq] _]
                       (let [[k _ krest] (pull-string aseq)
                             [v _ vrest] (pull-string krest)]
                         [(conj acc [k v]) vrest]))
                    [[] arest]
                    (range 0 a))]
      {:obj {:name n :attr attr}
       :next (full-obj-iter onext)})))

(defn merge-obj [iters out]
  (when (seq iters)
    (let [objs (->> (map #(%) iters) (filter identity) (sort-by :name))
          [now later] (split-with #(= (-> objs first :name) (% :name)) objs)
          selected (first now)]
      ;(println "merged obj - " (:name selected))
      (.write out (byte-array (:seq selected)))
      #(merge-attr (map :next now) (map :prev later) [] out))))

(defn merge-bin [inputs output]
  (with-open [out (java.io.FileOutputStream. output)]
    (let [iters (map (comp obj-iter byte-seq input-stream file) inputs)]
      (trampoline (merge-obj iters out)))))


(defn obj->bin [{:keys [name attr]} out]
  (do
    (doto out
      (.write (int->bytes (count name)))
      (.write (string->bytes name))
      (.write (int->bytes (count attr))))
    (doseq [[an av] attr]
      (doto out
        (.write (int->bytes (count an)))
        (.write (string->bytes an))
        (.write (int->bytes (count av)))
        (.write (string->bytes av))))))


(defn bin->objs [f]
  (->> ((full-obj-iter ((comp byte-seq input-stream file) f)))
       (iterate (fn [o] ((:next o))))
       (take-while (comp not nil?))
       (map :obj)))


(nth [1 2 3] 1)






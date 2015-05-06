(ns binmerge.core
  (:require [clojure.java.io :refer [file output-stream input-stream reader]]))


;;; Conversions ;;;

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


;;; Iterators ;;;

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
  "Returns an iterator that starts from sequence position s,
  emits attribute information and the next attribute iterator
  and so on for n attributes and finally emits an object iterator
  pointing to the next object

  :next advances the iterator
  :prev rewinds the iterator
  :seq returns the byte seq iterated over"

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

(defn obj-iter [s]
  "Takes a byte sequence starting from an object and returns an iterator
  to provide object information and lead on to it's attribute iterator

  :next advances the iterator
  :prev rewinds the iterator
  :seq returns the byte seq iterated over"

  #(when (seq s)
     (let [[n nseq nrest] (pull-string s)
          [a aseq arest] (pull-int nrest)]
      {:type :obj
       :name n
       :attr-no a
       :seq nseq
       :next (attr-iter arest a)
       :prev (obj-iter s)})))



;;; Merge functions ;;;

(declare merge-obj)

(defn merge-attr [iters obj-iters pos attr-no acc out]
  "Takes a sequence of iterators, iters pointing to each object's attribute list and
  an output stream, out. pos marks the position of no of attributes and attr-no its value:
  1. Move all iterators forward.
    1.1 Remove the iterators that have reached the end of their sequence
    1.2 Seggregate the ones that have passed on to the next object
  2. Sort based on key and partition the first few that share the 'smallest' key
  3. Select the first attribute, and therefore the latest from the ones sharing the 'smallest' key
  4. Repeat the process by moving forward the rest of the iterators
  5. When only object iterators remain, we return a merge function for those iterators"

  (let [[attrs objs] (->> (map #(%) iters) (filter identity) (sort-by :type) (split-with #(= (:type %) :attr)))]
    (if (seq attrs)
      (let [sorted (sort-by :key attrs)
            [now later] (split-with #(= (-> sorted first :key) (% :key)) sorted)
            selected (first now)]
        (.write out (byte-array (:seq selected)))
        #(merge-attr
          (concat (map :next now) (map :prev later))
          (concat obj-iters (map :prev objs))
          pos
          (inc attr-no)
          acc
          out))
      (do
        ;(let [new-pos (.getFilePointer out)]
        ;  (.seek out pos)
        ;  (.write out (int->bytes attr-no))
        ;  (.seek out new-pos))
        #(merge-obj (concat obj-iters (map :prev objs)) (conj acc [pos attr-no]) out)))))



(defn merge-obj [iters acc out]
  "Takes a sequence of iterators and an output stream and writes objects incrementally
  to the output stream
  1. Move all object iterators forward
    1.1 Get rid of iterators that have reached the end of their sequences
  2. Sort based on name and partition into two groups - one with the 'smallest' name and without
  3. Pass on the selected iterators to merge-attr in order to merge their attributes together
    3.1 'Rewind' the rejected iterators with (:seq i) to point back to their objects
    3.2 Preserve the 'rewinded' iterators in the merge-attr closure for later iteration"

  (if (seq iters)
    (let [objs (->> (map #(%) iters) (filter identity) (sort-by :name))
          [now later] (split-with #(= (-> objs first :name) (% :name)) objs)
          selected (first now)]
      (.write out (byte-array (:seq selected)))
      (let [pos (-> out .getChannel .position)]
        (.write out (byte-array 8)) ; placeholder for no of attr
        #(merge-attr (map :next now) (map :prev later) pos 0 acc out)))
    acc))



(defn merge-bin [inputs output]
  "Given a sequence of input file paths and an output file path, merge inputs into output
  1. Create files from input file paths and sort them, latest first
  2. Create object iterators from input files
  3. Use trampoline to keep iterating until the sequences are all done
  4. Write back attribute nos from updates received after iteration"

  (let [files (->> inputs (map file) (sort-by #(.lastModified %)) reverse)]
    (spit output "")
    (let
      [updates
      (with-open [out (java.io.FileOutputStream. output )]
        (let [iters (map (comp obj-iter byte-seq input-stream) files)]
          (trampoline (merge-obj iters [] out))))]
      (with-open [out (java.io.RandomAccessFile. output "rw")]
        (doseq [[pos attr-no] updates]
          (.seek out pos)
          (.write out (int->bytes attr-no)))))))



;;; Object reader ;;;

(defn find-object [search-name f]
  "Given a name and an input file path, search for an object with the name in the file
  1. Read name and check match
    1.1 If matched, read no of attr
    1.2 for attr-no times, read key value pair and interpret to strings
    1.3 Return object
  2. If name does not match
    2.1 for attr-no times, read key value pair, but do not interpret
    2.2 Repeat search with the advanced input stream"

(letfn
  [(find-obj [search-name in]
  (let [bno (byte-array 8)
        rno (.read in bno 0 8)]
    (when (> rno -1)
      (let [no (bytes->int bno)
            bname (byte-array no)
            rname (.read in bname 0 no)
            obj-name (bytes->string bname)
            battr-no (byte-array 8)
            rattr-no (.read in battr-no 0 8)
            attr-no (bytes->int battr-no)
            bkey-no (byte-array 8)
            bval-no (byte-array 8)]
        (if (= obj-name search-name)
          {:name obj-name
           :attr
             (for [i (range attr-no)]
               (do
                 (.read in bkey-no 0 8)
                 (let [key-no (bytes->int bkey-no)
                       bkey (byte-array key-no)]
                   (.read in bkey 0 key-no)
                   (.read in bval-no 0 8)
                   (let [k (bytes->string bkey)
                         val-no (bytes->int bval-no)
                         bval (byte-array val-no)]
                     (.read in bval 0 val-no)
                     {:key k :value (bytes->string bval)}))))
           }
          (do
            (dotimes [i attr-no]
              (.read in bkey-no 0 8)
              (let [key-no (bytes->int bkey-no)]
                (.read in (byte-array key-no) 0 key-no))
              (.read in bval-no 0 8)
              (let [val-no (bytes->int bval-no)]
                (.read in (byte-array val-no) 0 val-no)))
            (recur search-name in)))))))]
  (find-obj search-name (input-stream  f))))



;;; Test helpers ;;;

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



(defn mill-attr [out]
  (do
    (doto out
      (.write (int->bytes (count "mill-attr")))
      (.write (string->bytes "mill-attr"))
      (.write (int->bytes 1000000)))
    (doseq [i (range 1000000)]
      (let [k (str "key-" i)
            v (str "val-" i)]
        (doto out
          (.write (int->bytes (count k)))
          (.write (string->bytes k))
          (.write (int->bytes (count v)))
          (.write (string->bytes v)))))))

(defn test-os [f]
  (with-open [out (java.io.FileOutputStream. f)]
    (let [c (.getChannel out)]
      (.write out (string->bytes "Foolhardy"))
      (with-open [out2 (java.io.FileOutputStream. f)]
        (do
          ;(.position (.getChannel out2) 4)
          ;(.write out2 (byte \c))
          )
      (.write out (byte \0))))))



(reverse (sort-by #(.lastModified %) (map file ["resources/data/table2" "resources/data/table1" "resources/data/table3" "/tmp/see"])))

(ns taoensso.nippy.tests.main
  (:require [clojure.test.check            :as check]
            [clojure.test.check.generators :as check-gen]
            [clojure.test.check.properties :as check-props]
            [expectations   :as test :refer :all]
            [taoensso.nippy :as nippy :refer (freeze thaw)]
            [taoensso.nippy.benchmarks  :as benchmarks]))

(comment (test/run-tests '[taoensso.nippy.tests.main]))

(def test-data nippy/stress-data-comparable)
(defn- before-run {:expectations-options :before-run} [])
(defn- after-run  {:expectations-options :after-run}  [])

;;;; Core

(expect (do (println (str "Clojure version: " *clojure-version*)) true))

(expect test-data ((comp thaw freeze) test-data))
(expect test-data ((comp #(thaw   % {:compressor nippy/lz4-compressor})
                         #(freeze % {:skip-header? true}))
                   test-data))
(expect test-data ((comp #(thaw   % {:password [:salted "p"]})
                         #(freeze % {:password [:salted "p"]}))
                   test-data))
(expect test-data ((comp #(thaw   % {:compressor nippy/lzma2-compressor})
                         #(freeze % {:compressor nippy/lzma2-compressor}))
                   test-data))
(expect test-data ((comp #(thaw   % {:compressor nippy/lzma2-compressor
                                     :password [:salted "p"]})
                         #(freeze % {:compressor nippy/lzma2-compressor
                                     :password [:salted "p"]}))
                   test-data))
(expect test-data ((comp #(thaw   % {:compressor nippy/lz4-compressor})
                         #(freeze % {:compressor nippy/lz4hc-compressor}))
                   test-data))

(expect ; Try roundtrip anything that simple-check can dream up
 (:result (check/quick-check 80 ; Time is n-non-linear
            (check-props/for-all [val check-gen/any]
              (= val (thaw (freeze val)))))))

;;; Trying to decrypt random (invalid) data can actually crash JVM
;; (expect Exception (thaw (freeze test-data {:password "malformed"})))
;; (expect Exception (thaw (freeze test-data {:password [:salted "p"]})))
;; (expect Exception (thaw (freeze test-data {:password [:salted "p"]})
;;                         {:compressor nil}))

(expect ; Snappy lib compatibility (for legacy versions of Nippy)
 (let [^bytes raw-ba    (freeze test-data {:compressor nil})
       ^bytes xerial-ba (org.xerial.snappy.Snappy/compress raw-ba)
       ^bytes iq80-ba   (org.iq80.snappy.Snappy/compress   raw-ba)]
   (= (thaw raw-ba)
      (thaw (org.xerial.snappy.Snappy/uncompress xerial-ba))
      (thaw (org.xerial.snappy.Snappy/uncompress iq80-ba))
      (thaw (org.iq80.snappy.Snappy/uncompress   iq80-ba    0 (alength iq80-ba)))
      (thaw (org.iq80.snappy.Snappy/uncompress   xerial-ba  0 (alength xerial-ba))))))

;;;; Custom types & records

;;; Extend to custom Type
(defrecord MyType [data])
(expect Exception (do (nippy/extend-freeze MyType 1 [x s] (.writeUTF s (:data x)))
                      (thaw (freeze (->MyType "val")))))
(expect (do (nippy/extend-thaw 1 [s] (->MyType (.readUTF s)))
            (let [type (->MyType "val")] (= type (thaw (freeze type))))))

;;; Extend to custom Record
(defrecord MyRec [data])
(expect (do (nippy/extend-freeze MyRec 2 [x s] (.writeUTF s (str "foo-" (:data x))))
            (nippy/extend-thaw 2 [s] (->MyRec (.readUTF s)))
            (= (->MyRec "foo-val") (thaw (freeze (->MyRec "val"))))))

;;; Keyword (prefixed) extensions
(expect
  (do (nippy/extend-freeze MyType :nippy-tests/MyType [x s] (.writeUTF s (:data x)))
      (nippy/extend-thaw :nippy-tests/MyType [s] (->MyType (.readUTF s)))
      (let [type (->MyType "val")] (= type (thaw (freeze type))))))

;;;; Stable binary representation of vals

(expect (seq (freeze test-data))
        (seq (freeze test-data))) ; f(x)=f(y) | x=y

;; As above, but try multiple times to catch possible protocol interface races:
(expect #(every? true? %)
        (repeatedly 1000 (fn [] (= (seq (freeze test-data))
                                  (seq (freeze test-data))))))

;; NB abandoning - no way to do this reliably w/o appropriate contracts from
;; (seq <unordered-coll>):
;;
;; (expect (seq (-> test-data freeze)) ; f(x)=f(f-1(f(x)))
;;         (seq (-> test-data freeze thaw freeze)))
;;
;; As above, but with repeated refreeze to catch possible protocol interface races:
;; (expect (= (seq (freeze test-data))
;;            (seq (reduce (fn [frozen _] (freeze (thaw frozen)))
;;                   (freeze test-data) (range 1000)))))

(defn qc-prop-bijection [& [n]]
  (let [bin->val (atom {})
        val->bin (atom {})]
    (merge
     (check/quick-check (or n 1)
       (check-props/for-all [val check-gen/any #_check-gen/any-printable]
         (let [;; Nb need `seq` for Clojure hash equality:
               bin (hash (seq (freeze val)))]
           (and
            (if (contains? val->bin val)
              (= (get val->bin val) bin) ; x=y => f(x)=f(y) by clj=
              (do (swap! val->bin assoc val bin)
                  true))

            (if (contains? bin->val bin)
              (= (get bin->val bin) val) ; f(x)=f(y) => x=y by clj=
              (do (swap! bin->val assoc bin val)
                  true))))))
     #_{:bin->val @bin->val
        :val->bin @val->bin}
     nil)))

(comment
  (check-gen/sample check-gen/any 10)
  (:result (qc-prop-bijection 80))
  (let [{:keys [result bin->val val->bin]} (qc-prop-bijection 10)]
    [result (vals bin->val)]))

(expect #(:result %) (qc-prop-bijection 80))

;;;; Thread safety

;; Not sure why, but record equality test fails in futures:
(def test-data-threaded (dissoc nippy/stress-data-comparable :stress-record))

(expect
  (let [futures
        (mapv
          (fn [_]
            (future
              (= (thaw (freeze test-data-threaded)) test-data-threaded)))
          (range 50))]
    (every? deref futures)))

(expect
  (let [futures
        (mapv
          (fn [_]
            (future
              (= (thaw (freeze test-data-threaded {:password [:salted "password"]})
                                                  {:password [:salted "password"]})
                test-data-threaded)))
          (range 50))]
    (every? deref futures)))

(expect
  (let [futures
        (mapv
          (fn [_]
            (future
              (= (thaw (freeze test-data-threaded {:password [:cached "password"]})
                                                  {:password [:cached "password"]})
                test-data-threaded)))
          (range 50))]
    (every? deref futures)))

;;;; Benchmarks

(expect (benchmarks/bench {})) ; Also tests :cached passwords

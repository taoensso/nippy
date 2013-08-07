(ns taoensso.nippy.tests.main
  (:require [expectations   :as test :refer :all]
            [taoensso.nippy :as nippy :refer (freeze thaw)]
            [taoensso.nippy.benchmarks :as benchmarks]))

;; Remove stuff from stress-data that breaks roundtrip equality
(def test-data (dissoc nippy/stress-data :bytes))

(defn- before-run {:expectations-options :before-run} [])
(defn- after-run  {:expectations-options :after-run}  [])

(expect test-data ((comp thaw freeze) test-data))
(expect test-data ((comp thaw #(freeze % {:legacy-mode true})) test-data))
(expect test-data ((comp #(thaw   % {:password [:salted "p"]})
                         #(freeze % {:password [:salted "p"]}))
                   test-data))

(expect AssertionError (thaw (freeze test-data {:password "malformed"})))
(expect Exception (thaw (freeze test-data {:password [:salted "p"]})))
(expect Exception (thaw (freeze test-data {:password [:salted "p"]})
                        {:compressor nil}))

(expect ; Snappy lib compatibility (for legacy versions of Nippy)
 (let [^bytes raw-ba    (freeze test-data {:compressor nil})
       ^bytes xerial-ba (org.xerial.snappy.Snappy/compress raw-ba)
       ^bytes iq80-ba   (org.iq80.snappy.Snappy/compress   raw-ba)]
   (= (thaw raw-ba)
      (thaw (org.xerial.snappy.Snappy/uncompress xerial-ba))
      (thaw (org.xerial.snappy.Snappy/uncompress iq80-ba))
      (thaw (org.iq80.snappy.Snappy/uncompress   iq80-ba    0 (alength iq80-ba)))
      (thaw (org.iq80.snappy.Snappy/uncompress   xerial-ba  0 (alength xerial-ba))))))

;;; Records
(defrecord RecordType [data])
(expect RecordType (thaw (freeze (RecordType. "Joe"))))
(expect (RecordType. "Joe") (thaw (freeze (RecordType. "Joe"))))

;;; Custom types
(defrecord MyType [data])
(nippy/extend-freeze MyType 1 [x s] (.writeUTF s (:data x)))
(expect Exception       (thaw (freeze (->MyType "Joe"))))
(expect (MyType. "Joe") (do (nippy/extend-thaw 1 [s] (->MyType (.readUTF s)))
                            (thaw (freeze (->MyType "Joe")))))

(expect (benchmarks/bench {:reader? false})) ; Also tests :cached passwords
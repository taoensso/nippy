(ns taoensso.nippy.tests.main
  (:require [expectations   :as test :refer :all]
            [taoensso.nippy :as nippy :refer (freeze thaw)]
            [taoensso.nippy.benchmarks :as benchmarks]))

;; Remove stuff from stress-data that breaks roundtrip equality
(def test-data (dissoc nippy/stress-data :bytes))

;;;; Basic data integrity
(expect test-data ((comp thaw freeze) test-data))
(expect test-data ((comp thaw #(freeze % {:legacy-mode true})) test-data))
(expect test-data ((comp #(thaw   % {:password [:salted "p"]})
                         #(freeze % {:password [:salted "p"]}))
                   test-data))

(expect ; Snappy lib compatibility (for legacy versions of Nippy)
 (let [^bytes raw-ba    (freeze test-data {:compressor nil})
       ^bytes xerial-ba (org.xerial.snappy.Snappy/compress raw-ba)
       ^bytes iq80-ba   (org.iq80.snappy.Snappy/compress   raw-ba)]
   (= (thaw raw-ba)
      (thaw (org.xerial.snappy.Snappy/uncompress xerial-ba))
      (thaw (org.xerial.snappy.Snappy/uncompress iq80-ba))
      (thaw (org.iq80.snappy.Snappy/uncompress   iq80-ba    0 (alength iq80-ba)))
      (thaw (org.iq80.snappy.Snappy/uncompress   xerial-ba  0 (alength xerial-ba))))))

;;;; API stuff

;;; Strict/auto mode - compression
(expect test-data (thaw (freeze test-data {:compressor nil})))
(expect Exception (thaw (freeze test-data {:compressor nil})
                        {:legacy-mode false
                         :strict?     true}))

;;; Strict/auto mode - encryption
(expect test-data (thaw (freeze test-data) {:password [:salted "p"]}))
(expect Exception (thaw (freeze test-data) {:password [:salted "p"]
                                            :legacy-mode false
                                            :strict?     true}))

;;; Encryption - passwords
(expect Exception (thaw (freeze test-data {:password "malformed"})))
(expect Exception (thaw (freeze test-data {:password [:salted "p"]})))
(expect test-data (thaw (freeze test-data {:password [:salted "p"]})
                        {:password [:salted "p"]}))

(expect (benchmarks/autobench)) ; Also tests :cached passwords
(ns test-nippy.main
  (:use [clojure.test])
  (:require [taoensso.nippy :as nippy]
            [taoensso.nippy.benchmarks :as benchmarks]))

;; Remove stuff from stress-data that breaks roundtrip equality
(def test-data (dissoc nippy/stress-data :bytes))

(def roundtrip-defaults  (comp nippy/thaw-from-bytes nippy/freeze-to-bytes))
(def roundtrip-encrypted (comp #(nippy/thaw-from-bytes % :password "secret")
                               #(nippy/freeze-to-bytes % :password "secret")))

(deftest test-roundtrip-defaults  (is (= test-data (roundtrip-defaults  test-data))))
(deftest test-roundtrip-encrypted (is (= test-data (roundtrip-encrypted test-data))))

(println "Benchmarking roundtrips (x3)")
(println "----------------------------")
(println (benchmarks/autobench))
(println (benchmarks/autobench))
(println (benchmarks/autobench))

(deftest test-snappy-library-compatibility
  (let [thaw             #(nippy/thaw-from-bytes % :compressed? false)
        ^bytes raw-ba    (nippy/freeze-to-bytes test-data :compress? false)
        ^bytes xerial-ba (org.xerial.snappy.Snappy/compress raw-ba)
        ^bytes iq80-ba   (org.iq80.snappy.Snappy/compress   raw-ba)]
    (is (= (thaw raw-ba)
           (thaw (org.xerial.snappy.Snappy/uncompress xerial-ba))
           (thaw (org.xerial.snappy.Snappy/uncompress iq80-ba))
           (thaw (org.iq80.snappy.Snappy/uncompress   iq80-ba    0 (alength iq80-ba)))
           (thaw (org.iq80.snappy.Snappy/uncompress   xerial-ba  0 (alength xerial-ba)))))))
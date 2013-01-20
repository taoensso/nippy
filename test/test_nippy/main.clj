(ns test-nippy.main
  (:use [clojure.test])
  (:require [taoensso.nippy :as nippy]
            [taoensso.nippy.benchmarks :as benchmarks]))

;; Remove stuff from stress-data that breaks roundtrip equality
(def test-data (dissoc nippy/stress-data :bytes))

(def roundtrip (comp nippy/thaw-from-bytes nippy/freeze-to-bytes))

(deftest test-roundtrip (is (= test-data (roundtrip test-data))))

(println "Benchmarking roundtrips (x3)")
(println "----------------------------")
(println (benchmarks/autobench))
(println (benchmarks/autobench))
(println (benchmarks/autobench))

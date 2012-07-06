(ns test-nippy.main
  (:use [clojure.test])
  (:require [taoensso.nippy :as nippy]))

;; Remove stuff from stress-data that breaks roundtrip equality
(def test-data (dissoc nippy/stress-data :bytes))

(def roundtrip (comp nippy/thaw-from-bytes nippy/freeze-to-bytes))

(deftest test-roundtrip (is (= test-data (roundtrip test-data))))
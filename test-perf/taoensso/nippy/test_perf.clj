(ns taoensso.nippy.test-perf
  (:require
    [clojure.test :refer [deftest is testing use-fixtures]]
    [citius.core :as c]
    [clojure.tools.reader.edn :as edn]
    [clojure.data.fressian    :as fressian]
    [taoensso.nippy           :as nippy]))


(def data nippy/stress-data-benchable)


(defn fressian-freeze [value]
  (let [^java.nio.ByteBuffer bb (fressian/write value)
        len (.remaining bb)
        ba  (byte-array len)]
    (.get bb ba 0 len)
    ba))


(defn fressian-thaw [value]
  (let [bb (java.nio.ByteBuffer/wrap value)]
    (fressian/read bb)))


(use-fixtures :once (c/make-bench-wrapper
                      ["tools.reader.edn" "data.fressian" "Nippy-default" "Nippy-fast" "Nippy-encrypted" "Nippy-lzma2"]
                      {:chart-title "Nippy"
                       :chart-filename (format "bench-clj-%s.png" c/clojure-version-str)}))


(deftest test-roundtrip
  (c/compare-perf "Roundtrip"
    (edn/read-string (pr-str data))
    (fressian-thaw (fressian-freeze data))
    (nippy/thaw (nippy/freeze data))
    (nippy/thaw (nippy/freeze data {:compressor nil
                                    :skip-header? true}) {:compressor nil
                                                          :encryptor  nil})
    (nippy/thaw (nippy/freeze data {:password [:cached "p"]}) {:password [:cached "p"]})
    (nippy/thaw (nippy/freeze data {:compressor nippy/lzma2-compressor}) {:compressor nippy/lzma2-compressor})))


(deftest test-freeze
  (c/compare-perf "Freeze"
    (pr-str data)
    (fressian-freeze data)
    (nippy/freeze data)
    (nippy/freeze data {:compressor nil
                        :skip-header? true})
    (nippy/freeze data {:password [:cached "p"]})
    (nippy/freeze data {:compressor nippy/lzma2-compressor})))


(deftest test-thaw
  (let [edn-frozen         (pr-str data)
        fressian-frozen    (fressian-freeze data)
        nippy-frozen       (nippy/freeze data)
        nippy-frozen-fast  (nippy/freeze data {:compressor nil
                                               :skip-header? true})
        nippy-frozen-encr  (nippy/freeze data {:password [:cached "p"]})
        nippy-frozen-lzma2 (nippy/freeze data {:compressor nippy/lzma2-compressor})]
    (c/compare-perf "Thaw"
      (edn/read-string edn-frozen)
      (fressian-thaw   fressian-frozen)
      (nippy/thaw      nippy-frozen)
      (nippy/thaw      nippy-frozen-fast  {:compressor nil
                                           :encryptor  nil})
      (nippy/thaw      nippy-frozen-encr  {:password [:cached "p"]})
      (nippy/thaw      nippy-frozen-lzma2 {:compressor nippy/lzma2-compressor}))))

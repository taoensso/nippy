(ns taoensso.nippy.compression "Alpha - subject to change."
  {:author "Peter Taoussanis"}
  (:require [taoensso.encore :as encore])
  (:import  [java.io ByteArrayInputStream ByteArrayOutputStream DataInputStream
             DataOutputStream]))

;;;; Interface

(defprotocol ICompressor
  (compress   ^bytes [compressor ba])
  (decompress ^bytes [compressor ba]))

;;;; Default implementations

(deftype SnappyCompressor []
  ICompressor
  (compress   [_ ba] (org.iq80.snappy.Snappy/compress   ba))
  (decompress [_ ba] (org.iq80.snappy.Snappy/uncompress ba 0 (alength ^bytes ba))))

(def snappy-compressor
  "Default org.iq80.snappy.Snappy compressor:
        Ratio: low.
  Write speed: very high.
   Read speed: very high.

  A good general-purpose compressor."
  (->SnappyCompressor))

(deftype LZMA2Compressor [compression-level]
  ;; Compression level ∈ℕ[0,9] (low->high) with 6 LZMA2 default (we use 0)
  ICompressor
  (compress [_ ba]
    (let [ba-len (alength ^bytes ba)
          ba-os  (ByteArrayOutputStream.)
          ;; Prefix with uncompressed length:
          _      (.writeInt (DataOutputStream. ba-os) ba-len)
          xzs    (org.tukaani.xz.XZOutputStream. ba-os
                   (org.tukaani.xz.LZMA2Options. compression-level))]
      (.write xzs ^bytes ba)
      (.close xzs)
      (.toByteArray ba-os)))

  (decompress [_ ba]
    (let [ba-is  (ByteArrayInputStream. ba)
          ba-len (.readInt (DataInputStream. ba-is))
          ba     (byte-array ba-len)
          xzs    (org.tukaani.xz.XZInputStream. ba-is)]
      (.read xzs ba 0 ba-len)
      (when (not= -1 (.read xzs)) ; Good practice as extra safety measure
        (throw (Exception. "LZMA2 Decompress failed: corrupt data?")))
      ba)))

(def lzma2-compressor
  "Alpha - subject to change.
  Default org.tukaani.xz.LZMA2 compressor:
        Ratio: high.
  Write speed: _very_ slow (also currently single-threaded).
   Read speed: slow.

  A specialized compressor for large, low-write data in space-sensitive
  environments."
  (->LZMA2Compressor 0))

(deftype LZ4Compressor [^net.jpountz.lz4.LZ4Compressor       compressor
                        ^net.jpountz.lz4.LZ4SafeDecompressor decompressor]
  ICompressor
  (compress [_ ba]
    (let [in-len      (alength ^bytes ba)
          max-out-len (.maxCompressedLength compressor in-len)
          ba-out*     (byte-array max-out-len)
          out-len     (.compress compressor ba 0 in-len ba-out* 0 max-out-len)
          ba-out      (java.util.Arrays/copyOf ba-out* out-len)]
      ba-out))

  (decompress [_ ba]
    (let [in-len      (alength ^bytes ba)
          max-out-len in-len
          ba-out*     (byte-array (* max-out-len 3.0)) ; Nb over-sized!
          out-len     (.decompress decompressor ba 0 in-len ba-out* 0)
          ba-out      (java.util.Arrays/copyOf ba-out* out-len)]
      ba-out)))

(def ^:private ^net.jpountz.lz4.LZ4Factory lz4-factory
  (net.jpountz.lz4.LZ4Factory/fastestInstance))

(def lz4-compressor
  "Default net.jpountz.lz4 compressor:
        Ratio: low.
  Write speed: very high.
   Read speed: very high.

  A good general-purpose compressor, competitive with Snappy."
  (->LZ4Compressor (.fastCompressor   lz4-factory)
                   (.safeDecompressor lz4-factory)))

(def lz4hc-compressor "Like `lz4-compressor` but trades some speed for ratio."
  (->LZ4Compressor (.highCompressor   lz4-factory)
                   (.safeDecompressor lz4-factory)))

(comment
  (def  ba-bench (.getBytes (apply str (repeatedly 1000 rand)) "UTF-8"))
  (defn bench1 [compressor]
    {:time  (encore/bench 10000 {:nlaps-warmup 10000}
              (->> ba-bench (compress compressor) (decompress compressor)))
     :ratio (encore/round2 (/ (count (compress compressor ba-bench))
                              (count ba-bench)))})

  (println
   {:snappy  (bench1 snappy-compressor)
    ;; :lzma (bench1 lzma2-compressor) ; Slow!
    :lz4     (bench1 lz4-compressor)
    :lz4hc   (bench1 lz4hc-compressor)})

  ;;; 2014 April 5, initial benchmarks
  {:snappy   {:time 2214  :ratio 0.848}
   :lzma     {:time 46684 :ratio 0.494}
   :lz4      {:time 1327  :ratio 0.819} ; w/o uncompressed size prefix
   :lz4hc    {:time 5762  :ratio 0.763} ; ''
   ;; :lz4   {:time 1404  :ratio 0.819} ; with uncompressed size prefix
   ;; :lz4hc {:time 6028  :ratio 0.763} ; ''
   })

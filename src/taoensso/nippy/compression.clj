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

(deftype LZ4Compressor [^net.jpountz.lz4.LZ4Compressor   compressor
                        ^net.jpountz.lz4.LZ4Decompressor decompressor]
  ICompressor
  (compress [_ ba]
    (let [len-decomp   (alength ^bytes ba)
          max-len-comp (.maxCompressedLength compressor len-decomp)
          ba-comp*     (byte-array max-len-comp) ; Over-sized
          len-comp     (.compress compressor ba 0 len-decomp ba-comp* 0 max-len-comp)
          ;;
          baos (ByteArrayOutputStream. (+ len-comp 4))
          dos  (DataOutputStream. baos)]
      (.writeInt dos len-decomp) ; Prefix with uncompressed length
      (.write    dos ba-comp* 0 len-comp)
      (.toByteArray baos)))

  (decompress [_ ba]
    (let [bais (ByteArrayInputStream. ba)
          dis  (DataInputStream. bais)
          ;;
          len-decomp (.readInt dis)
          len-comp   (- (alength ^bytes ba) 4)
          ba-comp    (byte-array len-comp)
          _          (.readFully dis ba-comp 0 len-comp)
          ba-decomp  (byte-array len-decomp)
          _          (.decompress decompressor ba-comp 0 ba-decomp 0 len-decomp)]
      ba-decomp)))

(def ^:private ^net.jpountz.lz4.LZ4Factory lz4-factory
  (net.jpountz.lz4.LZ4Factory/fastestInstance))

(def lz4-compressor
  "Default net.jpountz.lz4 compressor:
        Ratio: low.
  Write speed: very high.
   Read speed: very high.

  A good general-purpose compressor, competitive with Snappy.

  Thanks to Max Penet (@mpenet) for our first implementation,
  Ref. https://github.com/mpenet/nippy-lz4"
  (->LZ4Compressor (.fastCompressor   lz4-factory)
                   (.fastDecompressor lz4-factory)))

(def lz4hc-compressor "Like `lz4-compressor` but trades some speed for ratio."
  (->LZ4Compressor (.highCompressor   lz4-factory)
                   (.fastDecompressor lz4-factory)))

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
   :lz4      {:time 1363  :ratio 0.819}
   :lz4hc    {:time 6045  :ratio 0.763}})

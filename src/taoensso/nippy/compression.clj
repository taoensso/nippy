(ns ^:no-doc taoensso.nippy.compression
  "Private, implementation detail."
  (:require [taoensso.encore :as enc])
  (:import
   [java.nio ByteBuffer]
   [java.io
    ByteArrayInputStream ByteArrayOutputStream
    DataInputStream DataOutputStream]))

;;;; TODO
;; - NB always prepend uncompressed length?
;; - Consider (enc based)? dynamic uint from Tempel?

;;;; Interface

(defprotocol ICompressor
  (header-id         [compressor])
  (compress   ^bytes [compressor ba])
  (decompress ^bytes [compressor ba]))

(def ^:const standard-header-ids
  "These support `:auto` thaw."
  #{:snappy-v1 :lz4-v1 :lzma2-v1})

;;;; Utils

(defn- int-size->ba ^bytes [size]
  (let [ba   (byte-array 4)
        baos (ByteArrayOutputStream. 4)
        dos  (DataOutputStream. baos)]
    (.writeInt dos (int size))
    (.toByteArray baos)))

(defn- ba->int-size [ba]
  (let [bais (ByteArrayInputStream. ba)
        dis  (DataInputStream. bais)]
    (.readInt dis)))

(comment (ba->int-size (int-size->ba 3737)))

(defn- airlift-compress
  ^bytes [^io.airlift.compress.Compressor c ^bytes ba prepend-size?]

  (let [in-len      (alength ba)
        max-out-len (.maxCompressedLength c in-len)]

    (if prepend-size?
      (let [ba-max-out  (byte-array (int (+ 4 max-out-len)))
            int-size-ba (int-size->ba in-len)
            _           (System/arraycopy int-size-ba 0 ba-max-out 0 4)
            out-len
            (.compress c
              ba         0 in-len
              ba-max-out 4 max-out-len)]

        (if (== out-len max-out-len)
          (do                           ba-max-out)
          (java.util.Arrays/copyOfRange ba-max-out 0 (+ 4 out-len))))

      (let [ba-max-out (byte-array max-out-len)
            out-len
            (.compress c
              ba         0 in-len
              ba-max-out 0 max-out-len)]

        (if (== out-len max-out-len)
          (do                           ba-max-out)
          (java.util.Arrays/copyOfRange ba-max-out 0 out-len))))))

(defn- airlift-decompress
  ^bytes [^io.airlift.compress.Decompressor d ^bytes ba max-out-len]
  (if max-out-len
    (let [ba-max-out (byte-array (int max-out-len))
          out-len
          (.decompress d
            ba         0 (alength ba)
            ba-max-out 0 max-out-len)]

      (if (== out-len max-out-len)
        (do                           ba-max-out)
        (java.util.Arrays/copyOfRange ba-max-out 0 out-len)))

    ;; Prepended size
    (let [out-len (ba->int-size ba)
          ba-out  (byte-array (int out-len))]
      (.decompress d
        ba     4 (- (alength ba) 4)
        ba-out 0 out-len)
      ba-out)))

;;;; Snappy

(deftype SnappyCompressorV1 []
  ICompressor
  (header-id  [_] :snappy-v1)
  (compress   [_ ba] (org.iq80.snappy.Snappy/compress   ba))
  (decompress [_ ba] (org.iq80.snappy.Snappy/uncompress ba 0 (alength ^bytes ba))))

(do
  (enc/def* ^:private airlift-snappy-compressor_   (enc/thread-local (io.airlift.compress.snappy.SnappyCompressor.)))
  (enc/def* ^:private airlift-snappy-decompressor_ (enc/thread-local (io.airlift.compress.snappy.SnappyDecompressor.)))

  (deftype SnappyCompressorV2 [prepend-size?]
    ICompressor
    (header-id  [_] :snappy-v2)
    (compress   [_ ba] (airlift-compress   @airlift-snappy-compressor_   ba prepend-size?))
    (decompress [_ ba] (airlift-decompress @airlift-snappy-decompressor_ ba
                         (when-not prepend-size?
                           (io.airlift.compress.snappy.SnappyDecompressor/getUncompressedLength ba 0))))))

(comment
  (let [vba (vec (range 64))
        ba  (byte-array vba)
        v1  (SnappyCompressorV1.)
        v2  (SnappyCompressorV2. false)
        v2p (SnappyCompressorV2. true)]

    [(every? true? (for [c [v1 v2], d [v1 v2]] (= vba (->> ba (compress c) (decompress d) vec))))
     (enc/qb 1e6 ; [306.24 271.6 296.76]
       (->> ba (compress v1)  (decompress v1))
       (->> ba (compress v2)  (decompress v2))
       (->> ba (compress v2p) (decompress v2p)))]))

;;;; LZ4

(def ^:private lz4-factory_ (delay (net.jpountz.lz4.LZ4Factory/fastestInstance)))

(deftype LZ4CompressorV1 [compressor_ decompressor_]
  ICompressor
  (header-id [_] :lz4-v1)
  (compress  [_ ba]
    (let [^net.jpountz.lz4.LZ4Compressor compressor @compressor_
          len-uncomp   (alength ^bytes ba)
          max-len-comp (.maxCompressedLength compressor len-uncomp)
          ba-comp      (byte-array max-len-comp)
          len-comp     (.compress compressor ^bytes ba 0 len-uncomp
                         ba-comp 0 max-len-comp)

          baos (ByteArrayOutputStream. (+ len-comp 4))
          dos  (DataOutputStream. baos)]

      (.writeInt dos len-uncomp) ; Prepend with uncompressed length
      (.write    dos ba-comp 0 len-comp)
      (.toByteArray baos)))

  (decompress [_ ba]
    (let [^net.jpountz.lz4.LZ4Decompressor decompressor @decompressor_
          bais (ByteArrayInputStream. ba)
          dis  (DataInputStream. bais)

          len-uncomp (.readInt dis)
          len-comp   (- (alength ^bytes ba) 4)
          ba-uncomp  (byte-array len-uncomp)]

      (.decompress decompressor ba 4 ba-uncomp 0 len-uncomp)
      ba-uncomp)))

(do
  (enc/def* ^:private airlift-lz4-compressor_   (enc/thread-local (io.airlift.compress.lz4.Lz4Compressor.)))
  (enc/def* ^:private airlift-lz4-decompressor_ (enc/thread-local (io.airlift.compress.lz4.Lz4Decompressor.)))

  (deftype LZ4CompressorV2 []
    ICompressor
    (header-id  [_] :lz4-v2)
    (compress   [_ ba] (airlift-compress   @airlift-lz4-compressor_   ba true))
    (decompress [_ ba] (airlift-decompress @airlift-lz4-decompressor_ ba nil))))

(comment
  (let [vba (vec (range 64))
        ba  (byte-array vba)
        v2  (LZ4CompressorV2.)
        v1  (LZ4CompressorV1.
              ;;  (delay (.highCompressor   ^net.jpountz.lz4.LZ4Factory @lz4-factory_))
              ;;  (delay (.fastDecompressor ^net.jpountz.lz4.LZ4Factory @lz4-factory_))
              (do (delay (.fastCompressor   ^net.jpountz.lz4.LZ4Factory @lz4-factory_)))
              (do (delay (.fastDecompressor ^net.jpountz.lz4.LZ4Factory @lz4-factory_))))]

    [(every? true? (for [c [v1 v2], d [v1 v2]] (= vba (->> ba (compress c) (decompress d) vec))))
     (enc/qb 1e6 ; [638.55 284.19]
       (->> ba (compress v1) (decompress v1))
       (->> ba (compress v2) (decompress v2)))]))

;;;; LZMA

(deftype LZMA2Compressor [compression-level]
  ;; Compression level ∈ℕ[0,9] (low->high) with 6 LZMA2 default (we use 0)
  ICompressor
  (header-id [_] :lzma2)
  (compress  [_ ba]
    (let [baos (ByteArrayOutputStream.)
          dos  (DataOutputStream. baos)
          ;;
          len-decomp (alength ^bytes ba)
          ;; Prefix with uncompressed length:
          _   (.writeInt dos len-decomp)
          xzs (org.tukaani.xz.XZOutputStream. baos
                (org.tukaani.xz.LZMA2Options. compression-level))]
      (.write xzs ^bytes ba)
      (.close xzs)
      (.toByteArray baos)))

  (decompress [_ ba]
    (let [bais (ByteArrayInputStream. ba)
          dis  (DataInputStream. bais)
          ;;
          len-decomp (.readInt dis)
          ba         (byte-array len-decomp)
          xzs        (org.tukaani.xz.XZInputStream. bais)]
      (.read xzs ba 0 len-decomp)
      (if (== -1 (.read xzs)) ; Good practice as extra safety measure
        nil
        (throw (ex-info "LZMA2 Decompress failed: corrupt data?" {:ba ba})))
      ba)))

;;;; LZO

(do
  (enc/def* ^:private airlift-lzo-compressor_   (enc/thread-local (io.airlift.compress.lzo.LzoCompressor.)))
  (enc/def* ^:private airlift-lzo-decompressor_ (enc/thread-local (io.airlift.compress.lzo.LzoDecompressor.)))

  (deftype LzoCompressor []
    ICompressor
    (header-id  [_] :snappy-v2)
    (compress   [_ ba] (airlift-compress   @airlift-lzo-compressor_   ba true))
    (decompress [_ ba] (airlift-decompress @airlift-lzo-decompressor_ ba nil))))

;;;; Zstd

(do
  (enc/def* ^:private airlift-zstd-compressor_   (enc/thread-local (io.airlift.compress.zstd.ZstdCompressor.)))
  (enc/def* ^:private airlift-zstd-decompressor_ (enc/thread-local (io.airlift.compress.zstd.ZstdDecompressor.)))

  (deftype LzoCompressor [prepend-size?]
    ICompressor
    (header-id  [_] :snappy-v2)
    (compress   [_ ba] (airlift-compress   @airlift-zstd-compressor_   ba prepend-size?))
    (decompress [_ ba] (airlift-decompress @airlift-zstd-decompressor_ ba
                         (when-not prepend-size?
                           (io.airlift.compress.zstd.ZstdDecompressor/getDecompressedSize ba
                             0 (alength ^bytes ba)))))))

;;;;;;;;;;;;

(def snappy-compressor
  "Default org.iq80.snappy.Snappy compressor:
        Ratio: low.
  Write speed: very high.
   Read speed: very high.

  A good general-purpose compressor."
  (SnappyCompressor.))

(deftype LZMA2Compressor [compression-level]
  ;; Compression level ∈ℕ[0,9] (low->high) with 6 LZMA2 default (we use 0)
  ICompressor
  (header-id [_] :lzma2)
  (compress  [_ ba]
    (let [baos (ByteArrayOutputStream.)
          dos  (DataOutputStream. baos)
          ;;
          len-decomp (alength ^bytes ba)
          ;; Prefix with uncompressed length:
          _   (.writeInt dos len-decomp)
          xzs (org.tukaani.xz.XZOutputStream. baos
                (org.tukaani.xz.LZMA2Options. compression-level))]
      (.write xzs ^bytes ba)
      (.close xzs)
      (.toByteArray baos)))

  (decompress [_ ba]
    (let [bais (ByteArrayInputStream. ba)
          dis  (DataInputStream. bais)
          ;;
          len-decomp (.readInt dis)
          ba         (byte-array len-decomp)
          xzs        (org.tukaani.xz.XZInputStream. bais)]
      (.read xzs ba 0 len-decomp)
      (if (== -1 (.read xzs)) ; Good practice as extra safety measure
        nil
        (throw (ex-info "LZMA2 Decompress failed: corrupt data?" {:ba ba})))
      ba)))

(def lzma2-compressor
  "Default org.tukaani.xz.LZMA2 compressor:
        Ratio: high.
  Write speed: _very_ slow (also currently single-threaded).
   Read speed: slow.

  A specialized compressor for large, low-write data in space-sensitive
  environments."
  (LZMA2Compressor. 0))

(def lz4-compressor
  "Default net.jpountz.lz4 compressor:
        Ratio: low.
  Write speed: very high.
   Read speed: very high.

  A good general-purpose compressor, competitive with Snappy.

  Thanks to Max Penet (@mpenet) for our first implementation,
  Ref. https://github.com/mpenet/nippy-lz4"
  (LZ4Compressor.
    (delay (.fastCompressor   ^net.jpountz.lz4.LZ4Factory @lz4-factory_))
    (delay (.fastDecompressor ^net.jpountz.lz4.LZ4Factory @lz4-factory_))))

(def lz4hc-compressor
  "Like `lz4-compressor` but trades some write speed for ratio."
  (LZ4Compressor.
    (delay (.highCompressor   ^net.jpountz.lz4.LZ4Factory @lz4-factory_))
    (delay (.fastDecompressor ^net.jpountz.lz4.LZ4Factory @lz4-factory_))))

(comment
  (def ba-bench (.getBytes (apply str (repeatedly 1000 rand)) "UTF-8"))
  (defn bench1 [compressor]
    {:time
     (enc/bench 1e4 {:nlaps-warmup 25e3}
       (->> ba-bench (compress compressor) (decompress compressor)))

     :ratio
     (enc/round2
       (/
         (count (compress compressor ba-bench))
         (count                      ba-bench)))})

  {:snappy (bench1 snappy-compressor)
   :lzma2  (bench1 lzma2-compressor) ; Slow!
   :lz4    (bench1 lz4-compressor)
   :lz4hc  (bench1 lz4hc-compressor)}

  ;; 2023 Sep 12, 2020 Apple MBP M1
  {:snappy {:time 1111,  :ratio 0.85},
   :lzma2  {:time 23980, :ratio 0.49},
   :lz4    {:time 494,   :ratio 0.82},
   :lz4hc  {:time 2076,  :ratio 0.76}})

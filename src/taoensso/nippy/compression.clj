(ns ^:no-doc taoensso.nippy.compression
  "Private, implementation detail."
  (:require [taoensso.encore :as enc])
  (:import
   [java.nio ByteBuffer]
   [java.io
    ByteArrayInputStream ByteArrayOutputStream
    DataInputStream DataOutputStream]))

;;;; Interface

(defprotocol ICompressor
  (header-id         [compressor])
  (compress   ^bytes [compressor ba])
  (decompress ^bytes [compressor ba]))

(def ^:const standard-header-ids
  "These support `:auto` thaw."
  #{:zstd :lz4 #_:lzo :lzma2 :snappy})

;;;; Misc utils

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

;;;; Airlift

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
    (let [max-out-len (int        max-out-len)
          ba-max-out  (byte-array max-out-len)
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

(do
  (enc/def* ^:private airlift-zstd-compressor_   (enc/thread-local (io.airlift.compress.zstd.ZstdCompressor.)))
  (enc/def* ^:private airlift-zstd-decompressor_ (enc/thread-local (io.airlift.compress.zstd.ZstdDecompressor.)))
  (deftype ZstdCompressor [prepend-size?]
    ICompressor
    (header-id  [_] :zstd)
    (compress   [_ ba] (airlift-compress   @airlift-zstd-compressor_   ba prepend-size?))
    (decompress [_ ba] (airlift-decompress @airlift-zstd-decompressor_ ba
                         (when-not prepend-size?
                           (io.airlift.compress.zstd.ZstdDecompressor/getDecompressedSize ba
                             0 (alength ^bytes ba)))))))

(do
  (enc/def* ^:private airlift-lz4-compressor_   (enc/thread-local (io.airlift.compress.lz4.Lz4Compressor.)))
  (enc/def* ^:private airlift-lz4-decompressor_ (enc/thread-local (io.airlift.compress.lz4.Lz4Decompressor.)))
  (deftype LZ4Compressor []
    ICompressor
    (header-id  [_] :lz4)
    (compress   [_ ba] (airlift-compress   @airlift-lz4-compressor_   ba true))
    (decompress [_ ba] (airlift-decompress @airlift-lz4-decompressor_ ba nil))))

(do
  (enc/def* ^:private airlift-lzo-compressor_   (enc/thread-local (io.airlift.compress.lzo.LzoCompressor.)))
  (enc/def* ^:private airlift-lzo-decompressor_ (enc/thread-local (io.airlift.compress.lzo.LzoDecompressor.)))
  (deftype LZOCompressor []
    ICompressor
    (header-id  [_] :lzo)
    (compress   [_ ba] (airlift-compress   @airlift-lzo-compressor_   ba true))
    (decompress [_ ba] (airlift-decompress @airlift-lzo-decompressor_ ba nil))))

(do
  (enc/def* ^:private airlift-snappy-compressor_   (enc/thread-local (io.airlift.compress.snappy.SnappyCompressor.)))
  (enc/def* ^:private airlift-snappy-decompressor_ (enc/thread-local (io.airlift.compress.snappy.SnappyDecompressor.)))
  (deftype SnappyCompressor [prepend-size?]
    ICompressor
    (header-id  [_] :snappy)
    (compress   [_ ba] (airlift-compress   @airlift-snappy-compressor_   ba prepend-size?))
    (decompress [_ ba] (airlift-decompress @airlift-snappy-decompressor_ ba
                         (when-not prepend-size?
                           (io.airlift.compress.snappy.SnappyDecompressor/getUncompressedLength ba 0))))))

;;;; LZMA2

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

;;;; Public API

(def zstd-compressor
  "Default `Zstd` (`Zstandard`) compressor:
    -   Compression ratio: `B` (0.53       on reference benchmark).
    -   Compression speed: `C` (1300 msecs on reference benchmark).
    - Decompression speed: `B` (400  msecs on reference benchmark).

  Good general-purpose compressor, balances ratio & speed.
  See `taoensso.nippy-benchmarks` for detailed comparative benchmarks."
  (ZstdCompressor. false))

(def lz4-compressor
  "Default `LZ4` compressor:
    -   Compression ratio: `C`  (0.58      on reference benchmark).
    -   Compression speed: `A`  (238 msecs on reference benchmark).
    - Decompression speed: `A+` (31  msecs on reference benchmark).

  Good general-purpose compressor, favours speed.
  See `taoensso.nippy-benchmarks` for detailed comparative benchmarks."
  (LZ4Compressor.))

(def lzo-compressor
  "Default `LZO` compressor:
    -   Compression ratio: `C` (0.58      on reference benchmark).
    -   Compression speed: `A` (216 msecs on reference benchmark).
    - Decompression speed: `A` (43  msecs on reference benchmark).

  Good general-purpose compressor, favours speed.
  See `taoensso.nippy-benchmarks` for detailed comparative benchmarks."
  (LZOCompressor.))

(def snappy-compressor
  "Default `Snappy` compressor:
    -   Compression ratio: `C`  (0.58      on reference benchmark).
    -   Compression speed: `A+` (206 msecs on reference benchmark).
    - Decompression speed: `B`  (134 msecs on reference benchmark).

  Good general-purpose compressor, favours speed.
  See `taoensso.nippy-benchmarks` for detailed comparative benchmarks."
  (SnappyCompressor. false))

(def lzma2-compressor
  "Default `LZMA2` compressor:
    -   Compression ratio: `A+` (0.4       on reference benchmark).
    -   Compression speed: `E`  (18.5 secs on reference benchmark).
    - Decompression speed: `D`  (11.8 secs on reference benchmark).

  Specialized compressor, strongly favours ratio.
  See `taoensso.nippy-benchmarks` for detailed comparative benchmarks."
  (LZMA2Compressor. 0))

(enc/def* ^:no-doc lz4hc-compressor
  "Different LZ4 modes no longer supported, prefer `lz4-compressor`."
  {:deprecated "vX.Y.Z (YYYY-MM-DD)"}
  (LZ4Compressor.))

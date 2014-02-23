(ns taoensso.nippy.compression
  "Alpha - subject to change."
  {:author "Peter Taoussanis"}
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

  A good general-purpose compressor for Redis."
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

  A specialized compressor for large, low-write data."
  (->LZMA2Compressor 0))

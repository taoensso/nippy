(ns taoensso.nippy.compression
  (:import  [java.io ByteArrayInputStream ByteArrayOutputStream DataInputStream
             DataOutputStream]))

;;;; Interface

(defprotocol ICompressor
  (header-id         [compressor])
  (compress   ^bytes [compressor ba])
  (decompress ^bytes [compressor ba]))

;;;; Default implementations

(def standard-header-ids "These'll support :auto thaw" #{:snappy :lzma2 :lz4})

(deftype SnappyCompressor []
  ICompressor
  (header-id  [_] :snappy)
  (compress   [_ ba] (org.iq80.snappy.Snappy/compress   ba))
  (decompress [_ ba] (org.iq80.snappy.Snappy/uncompress ba 0 (alength ^bytes ba))))

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

(deftype LZ4Compressor [compressor_ decompressor_]
  ICompressor
  (header-id [_] :lz4)
  (compress  [_ ba]
    (let [^net.jpountz.lz4.LZ4Compressor compressor @compressor_
          len-decomp   (alength ^bytes ba)
          max-len-comp (.maxCompressedLength compressor len-decomp)
          ba-comp*     (byte-array max-len-comp) ; Over-sized
          len-comp     (.compress compressor ^bytes ba 0 len-decomp
                         ba-comp* 0 max-len-comp)
          ;;
          baos (ByteArrayOutputStream. (+ len-comp 4))
          dos  (DataOutputStream. baos)]
      (.writeInt dos len-decomp) ; Prefix with uncompressed length
      (.write    dos ba-comp* 0 len-comp)
      (.toByteArray baos)))

  (decompress [_ ba]
    (let [^net.jpountz.lz4.LZ4Decompressor decompressor @decompressor_
          bais (ByteArrayInputStream. ba)
          dis  (DataInputStream. bais)
          ;;
          len-decomp (.readInt dis)
          len-comp   (- (alength ^bytes ba) 4)
          ;; ba-comp (byte-array len-comp)
          ;; _       (.readFully dis ba-comp 0 len-comp)
          ba-decomp  (byte-array len-decomp)
          _          (.decompress decompressor ba 4 ba-decomp 0 len-decomp)]
      ba-decomp)))

(def ^:private lz4-factory_ (delay (net.jpountz.lz4.LZ4Factory/fastestInstance)))
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
    {:time  (enc/bench 10000 {:nlaps-warmup 10000}
              (->> ba-bench (compress compressor) (decompress compressor)))
     :ratio (enc/round2 (/ (count (compress compressor ba-bench))
                           (count ba-bench)))})

  (println
   {:snappy (bench1 snappy-compressor)
    ;:lzma2  (bench1 lzma2-compressor) ; Slow!
    :lz4    (bench1 lz4-compressor)
    :lz4hc  (bench1 lz4hc-compressor)})

  ;;; 2014 April 7
  {:snappy {:time 2251, :ratio 0.852},
   :lzma2  {:time 46684 :ratio 0.494}
   :lz4    {:time 1184, :ratio 0.819},
   :lz4hc  {:time 5422, :ratio 0.761}})

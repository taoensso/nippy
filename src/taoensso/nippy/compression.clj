(ns taoensso.nippy.compression
  "Alpha - subject to change."
  {:author "Peter Taoussanis"}
  (:require [taoensso.nippy.utils :as utils]
            [primitive-math :as pm :refer [<< >>>]])
  (:import (net.jpountz.lz4 LZ4Factory)))

;;;; Interface

(defprotocol ICompressor
  (compress   ^bytes [compressor ba])
  (decompress ^bytes [compressor ba]))

;;;; Default implementations

(deftype SnappyCompressor []
  ICompressor
  (compress   [_ ba] (org.iq80.snappy.Snappy/compress   ba))
  (decompress [_ ba] (org.iq80.snappy.Snappy/uncompress ba 0 (alength ^bytes ba))))

(def snappy-compressor "Default org.iq80.snappy.Snappy compressor."
  (->SnappyCompressor))


;;;; LZ4 Compression

(def ^net.jpountz.lz4.LZ4Factory LZ4-factory
  (net.jpountz.lz4.LZ4Factory/fastestInstance))

(def ^:const int-bytes (int 4))

;; Stolen impl from cassandra where we prefix the compressed bytes
;; array with the compressed length to make decompression faster
(deftype LZ4Compressor
    [^net.jpountz.lz4.LZ4Compressor compressor
     ^net.jpountz.lz4.LZ4Decompressor decompressor]
  ICompressor
  (compress [_ ba]
    (let [input-len (alength ^bytes ba)
          max-compressed-length (.maxCompressedLength compressor input-len)
          output (byte-array (pm/+ int-bytes max-compressed-length))]
      (aset-byte output 0 (pm/byte (>>> input-len 24)))
      (aset-byte output 1 (pm/byte (>>> input-len 16)))
      (aset-byte output 2 (pm/byte (>>> input-len 8)))
      (aset-byte output 3 (pm/byte input-len))
      (.compress compressor ba 0 input-len output int-bytes max-compressed-length)
      output))
  (decompress [_ ba]
    (let [uncompressed-len (pm/bit-or (<< (pm/byte->ubyte (aget ^bytes ba 0)) 24)
                                      (<< (pm/byte->ubyte (aget ^bytes ba 1)) 16)
                                      (<< (pm/byte->ubyte (aget ^bytes ba 2)) 8)
                                      (pm/byte->ubyte (aget ^bytes ba 3)))
          output (byte-array uncompressed-len)]
      (.decompress decompressor ba int-bytes output 0 uncompressed-len)
      output)))

(def lz4-compressor "Default net.jpountz.lz4 compressor."
  (->LZ4Compressor (.fastCompressor LZ4-factory)
                   (.fastDecompressor LZ4-factory)))

(def lz4hc-compressor "High compression net.jpountz.lz4 compressor."
  (->LZ4Compressor (.highCompressor LZ4-factory)
                   (.fastDecompressor LZ4-factory)))

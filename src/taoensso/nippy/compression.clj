(ns taoensso.nippy.compression
  "Alpha - subject to change."
  {:author "Peter Taoussanis"}
  (:require [taoensso.nippy.utils :as utils]))

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
(ns taoensso.nippy.compression
  "Alpha - subject to change."
  {:author "Peter Taoussanis"}
  (:require [taoensso.nippy.utils :as utils]))

;;;; Interface

(defprotocol ICompressor
  (header-id [compressor]) ; Unique, >0, <= 128
  (compress   ^bytes [compressor ba])
  (decompress ^bytes [compressor ba]))

;;;; Default implementations

(deftype DefaultSnappyCompressor []
  ICompressor
  (header-id  [_] 1)
  (compress   [_ ba] (org.iq80.snappy.Snappy/compress   ba))
  (decompress [_ ba] (org.iq80.snappy.Snappy/uncompress ba 0 (alength ^bytes ba))))

(def default-snappy-compressor
  "Default org.iq80.snappy.Snappy compressor."
  (DefaultSnappyCompressor.))
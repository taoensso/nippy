(ns taoensso.nippy
  "High-performance JVM Clojure serialization library. Originally adapted from
  Deep-Freeze (https://goo.gl/OePPGr)."
  {:author "Peter Taoussanis (@ptaoussanis)"}
  (:require [taoensso.encore :as enc]
            [taoensso.nippy
             (utils       :as utils)
             (compression :as compression)
             (encryption  :as encryption)])
  (:import  [java.io ByteArrayInputStream ByteArrayOutputStream DataInputStream
             DataOutputStream Serializable ObjectOutputStream ObjectInputStream
             DataOutput DataInput]
            [java.lang.reflect Method]
            [java.util Date UUID]
            [clojure.lang Keyword BigInt Ratio
             APersistentMap APersistentVector APersistentSet
             IPersistentMap ; IPersistentVector IPersistentSet IPersistentList
             PersistentQueue PersistentTreeMap PersistentTreeSet PersistentList ; LazySeq
             IRecord ISeq]))

(if (vector? enc/encore-version)
  (enc/assert-min-encore-version [2 16 0])
  (enc/assert-min-encore-version  2.16))

;;;; Nippy data format
;; * 4-byte header (Nippy v2.x+) (may be disabled but incl. by default) [1]
;; { * 1-byte type id.
;;   * Arb-length payload. } ...
;;
;; [1] Inclusion of header is *strongly* recommended. Purpose:
;;   * Sanity check (confirm that data appears to be Nippy data)
;;   * Nippy version check (=> supports changes to data schema over time)
;;   * Supports :auto thaw compressor, encryptor
;;   * Supports :auto freeze compressor (since this depends on :auto thaw
;;     compressor)
;;
(def ^:private ^:const head-version 1)
(def ^:private head-sig (.getBytes "NPY" "UTF-8"))
(def ^:private ^:const head-meta "Final byte stores version-dependent metadata"
  {(byte 0)  {:version 1 :compressor-id nil     :encryptor-id nil}
   (byte 4)  {:version 1 :compressor-id nil     :encryptor-id :else}
   (byte 5)  {:version 1 :compressor-id :else   :encryptor-id nil}
   (byte 6)  {:version 1 :compressor-id :else   :encryptor-id :else}
   ;;
   (byte 2)  {:version 1 :compressor-id nil     :encryptor-id :aes128-sha512}
   ;;
   (byte 1)  {:version 1 :compressor-id :snappy :encryptor-id nil}
   (byte 3)  {:version 1 :compressor-id :snappy :encryptor-id :aes128-sha512}
   (byte 7)  {:version 1 :compressor-id :snappy :encryptor-id :else}
   ;;
   ;;; :lz4 used for both lz4 and lz4hc compressor (the two are compatible)
   (byte 8)  {:version 1 :compressor-id :lz4    :encryptor-id nil}
   (byte 9)  {:version 1 :compressor-id :lz4    :encryptor-id :aes128-sha512}
   (byte 10) {:version 1 :compressor-id :lz4    :encryptor-id :else}
   ;;
   (byte 11) {:version 1 :compressor-id :lzma2  :encryptor-id nil}
   (byte 12) {:version 1 :compressor-id :lzma2  :encryptor-id :aes128-sha512}
   (byte 13) {:version 1 :compressor-id :lzma2  :encryptor-id :else}})

(defmacro when-debug [& body] (when #_true false `(do ~@body)))

;;;; Data type IDs

(do
  ;; ** Negative ids reserved for user-defined types **
  ;;
  (def ^:const id-reserved          (byte 0))
  ;;                                      1     ; Deprecated
  (def ^:const id-bytes             (byte 2))
  (def ^:const id-nil               (byte 3))
  (def ^:const id-boolean           (byte 4))
  (def ^:const id-reader            (byte 5))   ; Fallback #2
  (def ^:const id-serializable      (byte 6))   ; Fallback #1
  (def ^:const id-sm-bytes          (byte 7))

  (def ^:const id-char              (byte 10))
  ;;                                      11    ; Deprecated
  ; ;                                     12    ; Deprecated
  (def ^:const id-string            (byte 13))
  (def ^:const id-keyword           (byte 14))

  (def ^:const id-list              (byte 20))
  (def ^:const id-vec               (byte 21))
   ;;                                     22    ; Deprecated
  (def ^:const id-set               (byte 23))
  (def ^:const id-seq               (byte 24))
  (def ^:const id-meta              (byte 25))
  (def ^:const id-queue             (byte 26))
   ;;                                     27    ; Deprecated
  (def ^:const id-sorted-set        (byte 28))
   ;;                                     29    ; Deprecated
  (def ^:const id-map               (byte 30))
  (def ^:const id-sorted-map        (byte 31))

  (def ^:const id-byte              (byte 40))
  (def ^:const id-short             (byte 41))
  (def ^:const id-integer           (byte 42))
  (def ^:const id-long              (byte 43))
  (def ^:const id-bigint            (byte 44))
  (def ^:const id-biginteger        (byte 45))

  (def ^:const id-float             (byte 60))
  (def ^:const id-double            (byte 61))
  (def ^:const id-bigdec            (byte 62))

  (def ^:const id-ratio             (byte 70))

  (def ^:const id-record            (byte 80))
  ;; (def ^:const id-type           (byte 81))  ; TODO?
  (def ^:const id-prefixed-custom   (byte 82))

  (def ^:const id-date              (byte 90))
  (def ^:const id-uuid              (byte 91))

  ;;; Optimized, common-case types (v2.6+)
  (def ^:const id-byte-as-long      (byte 100)) ; 1 vs 8 bytes
  (def ^:const id-short-as-long     (byte 101)) ; 2 vs 8 bytes
  (def ^:const id-int-as-long       (byte 102)) ; 4 vs 8 bytes
  ;;
  (def ^:const id-sm-string         (byte 105)) ; 1 vs 4 byte length prefix
  (def ^:const id-sm-keyword        (byte 106)) ; ''
  ;;
  (def ^:const id-sm-vec            (byte 110)) ; ''
  (def ^:const id-sm-set            (byte 111)) ; ''
  (def ^:const id-sm-map            (byte 112)) ; ''
  ;;
  ;; TODO Additional optimizations (types) for 2-vecs and 3-vecs?

  ;;; DEPRECATED (old types will be supported only for thawing)
  (def ^:const id-reader-depr1      (byte 1))   ; v0.9.2+ for +64k support
  (def ^:const id-string-depr1      (byte 11))  ; v0.9.2+ for +64k support
  (def ^:const id-map-depr1         (byte 22))  ; v0.9.0+ for more efficient thaw
  (def ^:const id-keyword-depr1     (byte 12))  ; v2.0.0-alpha5+ for str consistecy
  (def ^:const id-map-depr2         (byte 27))  ; v2.11+ for count/2
  (def ^:const id-sorted-map-depr1  (byte 29))  ; v2.11+ for count/2
  )

;;;; Ns imports (mostly for convenience of lib consumers)

(do
  (enc/defalias compress          compression/compress)
  (enc/defalias decompress        compression/decompress)
  (enc/defalias snappy-compressor compression/snappy-compressor)
  (enc/defalias lzma2-compressor  compression/lzma2-compressor)
  (enc/defalias lz4-compressor    compression/lz4-compressor)
  (enc/defalias lz4hc-compressor  compression/lz4hc-compressor)

  (enc/defalias encrypt           encryption/encrypt)
  (enc/defalias decrypt           encryption/decrypt)
  (enc/defalias aes128-encryptor  encryption/aes128-encryptor)

  (enc/defalias freezable?        utils/freezable?))

;;;; Freezing

(defprotocol Freezable
  "Implementation detail. Be careful about extending to interfaces,
  Ref. http://goo.gl/6gGRlU."
  (-freeze-to-out [this out]))

(defmacro write-id [out id] `(.writeByte ~out ~id))
(defn write-bytes [^DataOutput out ^bytes ba]
  (let [len (alength ba)]
    (.writeInt  out      len)
    (.write     out ba 0 len)))

(defn write-sm-bytes [^DataOutput out ^bytes ba]
  (let [len (alength ba)]
    (byte len) ; Safety check
    (.writeByte out      len)
    (.write     out ba 0 len)))

(defn write-biginteger [out ^BigInteger n] (write-bytes    out (.toByteArray n)))
(defn write-utf8       [out     ^String s] (write-bytes    out (.getBytes s "UTF-8")))
(defn write-sm-utf8    [out     ^String s] (write-sm-bytes out (.getBytes s "UTF-8")))

(defn  byte-sized? [^long n] (<= n 127    #_Byte/MAX_VALUE))
(defn short-sized? [^long n] (<= n 32767 #_Short/MAX_VALUE))

(defn write-ided-bytes
  ([            out                 ba] (write-ided-bytes out id-sm-bytes id-bytes ba))
  ([^DataOutput out id-sm id ^bytes ba]
   (if (byte-sized? (alength ba))
     (do (write-id       out id-sm)
         (write-sm-bytes out ba))
     (do (write-id       out id)
         (write-bytes    out ba)))))

(defn write-ided-string [out ^String s]
  (write-ided-bytes out id-sm-string id-string (.getBytes s "UTF-8")))

(defn write-ided-keyword [out kw]
  (let [^String s (if-let [ns (namespace kw)] (str ns "/" (name kw)) (name kw))]
    (write-ided-bytes out id-sm-keyword id-keyword (.getBytes s "UTF-8"))))

(defn write-ided-long [^DataOutput out ^long n]
  (cond
    (and (<= n  127 #_Byte/MAX_VALUE)
         (>= n -128 #_Byte/MIN_VALUE))
    (do (write-id   out id-byte-as-long)
        (.writeByte out n))

    (and (<= n  32767 #_Short/MAX_VALUE)
         (>= n -32768 #_Short/MIN_VALUE))
    (do (write-id    out id-short-as-long)
        (.writeShort out n))

    (and (<= n 2147483647 #_Integer/MAX_VALUE)
         (>=  -2147483648 #_Integer/MIN_VALUE))
    (do (write-id  out id-int-as-long)
        (.writeInt out n))

    :else
    (do (write-id   out id-long)
        (.writeLong out n))))

(defn freeze-to-out!
  "Serializes arg (any Clojure data type) to a DataOutput"
  ;; Basically just wraps `-freeze-to-out` with different arg order + metadata support
  [^DataOutput data-output x]
  (when-let [m (meta x)]
    (write-id data-output id-meta)
    (-freeze-to-out m data-output))
  (-freeze-to-out x data-output))

(defn write-ided-coll [^DataOutput out ?id-sm id coll]
  (if (counted? coll)
    (let [cnt (count coll)]
      (if (and ?id-sm (byte-sized? cnt))
        (do (write-id   out ?id-sm)
            (.writeByte out cnt))
        (do (write-id   out id)
            (.writeInt  out cnt)))
      (enc/run!* (fn [in] (freeze-to-out! out in)) coll))

    (let [bas  (ByteArrayOutputStream. 64)
          sout (DataOutputStream. bas)
          cnt  (reduce (fn [^long cnt in]
                         (freeze-to-out! sout in)
                         (unchecked-inc cnt))
                 0 coll)
          ba   (.toByteArray bas)]

      (if (and ?id-sm (byte-sized? cnt))
        (do (write-id   out ?id-sm)
            (.writeByte out cnt))
        (do (write-id   out id)
            (.writeInt  out cnt)))

      (.write out ba 0 (alength ba)))))

(defn write-ided-kvs [^DataOutput out ?id-sm id coll]
  (let [cnt (count coll)]
    (if (and ?id-sm (byte-sized? cnt))
      (do (write-id   out ?id-sm)
          (.writeByte out cnt))
      (do (write-id   out id)
          (.writeInt  out cnt)))
    (enc/run-kv!
      (fn [k v]
        (freeze-to-out! out k)
        (freeze-to-out! out v))
      coll)))

(defn write-ided-vec [out v] (write-ided-coll out id-sm-vec id-vec v))
(defn write-ided-set [out s] (write-ided-coll out id-sm-set id-set s))
(defn write-ided-map [out m] (write-ided-kvs  out id-sm-map id-map m))

(defmacro ^:private freezer* [type & body]
  `(extend-type ~type
     Freezable
     (~'-freeze-to-out [~'x ~(with-meta 'out {:tag 'DataOutput})]
       ~@body)))

(defmacro ^:private freezer [type id & body]
  `(extend-type ~type
     Freezable
     (~'-freeze-to-out [~'x ~(with-meta 'out {:tag 'DataOutput})]
       (write-id ~'out ~id)
       ~@body)))

(freezer  nil                  id-nil)
(freezer  Boolean              id-boolean (.writeBoolean out        x))
(freezer  Character            id-char    (.writeChar    out   (int x)))
(freezer* (Class/forName "[B")            (write-ided-bytes   out x))
(freezer* String                          (write-ided-string  out x))
(freezer* Keyword                         (write-ided-keyword out x))

(freezer* PersistentQueue   (write-ided-coll out nil        id-queue      x))
(freezer* PersistentTreeSet (write-ided-coll out nil        id-sorted-set x))
(freezer* PersistentTreeMap (write-ided-kvs  out nil        id-sorted-map x))
(freezer* APersistentMap    (write-ided-kvs  out id-sm-map  id-map        x))
(freezer* APersistentVector (write-ided-coll out id-sm-vec  id-vec        x))
(freezer* APersistentSet    (write-ided-coll out id-sm-set  id-set        x))

;; No APersistentList:
(freezer* PersistentList    (write-ided-coll out nil        id-list       x))
(freezer* (type '())        (write-ided-coll out nil        id-list       x))

;; Nb low-level interface!! Acts as fallback for seqs that don't have a
;; concrete implementation. Will conflict with any other coll interfaces!
(freezer* ISeq              (write-ided-coll out nil        id-seq        x))

(freezer  IRecord id-record
  (write-utf8     out (.getName (class x))) ; Reflect
  (-freeze-to-out (into {} x) out))

(freezer  Byte       id-byte    (.writeByte  out x))
(freezer  Short      id-short   (.writeShort out x))
(freezer  Integer    id-integer (.writeInt   out x))
(freezer* Long                  (write-ided-long out x))

(freezer  BigInt     id-bigint     (write-biginteger out (.toBigInteger x)))
(freezer  BigInteger id-biginteger (write-biginteger out x))

(freezer  Float      id-float   (.writeFloat  out x))
(freezer  Double     id-double  (.writeDouble out x))
(freezer  BigDecimal id-bigdec
  (write-biginteger out (.unscaledValue x))
  (.writeInt out (.scale x)))

(freezer Ratio id-ratio
  (write-biginteger out (.numerator   x))
  (write-biginteger out (.denominator x)))

(freezer Date id-date (.writeLong out (.getTime x)))
(freezer UUID id-uuid
  (.writeLong out (.getMostSignificantBits  x))
  (.writeLong out (.getLeastSignificantBits x)))

(enc/defonce* ^:dynamic *final-freeze-fallback* nil)
(defn freeze-fallback-as-str [out x]
  (-freeze-to-out {:nippy/unfreezable (enc/pr-edn x) :type (type x)} out))

(comment
  (require '[clojure.core.async :as async])
  (binding [*final-freeze-fallback* freeze-fallback-as-str]
    (-> (async/chan) (freeze) (thaw))))

;; Fallbacks. Note that we'll extend *only* to (lowly) Object to prevent
;; interfering with higher-level implementations, Ref. http://goo.gl/6f7SKl
(extend-type Object
  Freezable
  (-freeze-to-out [x ^DataOutput out]
    (cond
     (utils/serializable? x) ; Fallback #1: Java's Serializable interface
     (do (when-debug (println (format "DEBUG - Serializable fallback: %s" (type x))))
         (write-id   out id-serializable)
         (write-utf8 out (.getName (class x))) ; Reflect
         (.writeObject (ObjectOutputStream. out) x))

     (utils/readable? x) ; Fallback #2: Clojure's Reader
     (do (when-debug (println (format "DEBUG - Reader fallback: %s" (type x))))
         (write-id   out id-reader)
         (write-utf8 out (enc/pr-edn x)))

     :else ; Fallback #3: *final-freeze-fallback*
     (if-let [ffb *final-freeze-fallback*]
       (ffb out x)
       (throw (ex-info (format "Unfreezable type: %s %s" (type x) (str x))
                {:type   (type x)
                 :as-str (enc/pr-edn x)}))))))

(def ^:private head-meta-id (reduce-kv #(assoc %1 %3 %2) {} head-meta))
(def ^:private get-head-ba
  (memoize
   (fn [head-meta]
     (when-let [meta-id (get head-meta-id (assoc head-meta :version head-version))]
       (enc/ba-concat head-sig (byte-array [meta-id]))))))

(defn- wrap-header [data-ba head-meta]
  (if-let [head-ba (get-head-ba head-meta)]
    (enc/ba-concat head-ba data-ba)
    (throw (ex-info (format "Unrecognized header meta: %s" head-meta)
             {:head-meta head-meta}))))

(comment (wrap-header (.getBytes "foo") {:compressor-id :lz4
                                         :encryptor-id  nil}))

(defn default-freeze-compressor-selector
  "Strategy:
    * Prioritize speed, but allow lz4.
    * Skip lz4 unless it's likely that lz4's space benefit will outweigh its
      space overhead."
  [^bytes ba]
  (let [ba-len (alength ba)]
    (cond
      ;; (> ba-len 8192) lzma2-compressor
      ;; (> ba-len 4098) lz4hc-compressor
         (> ba-len 1024) lz4-compressor
      :else              nil)))

(enc/defonce* ^:dynamic *default-freeze-compressor-selector*
  "(fn selector [^bytes ba])->compressor used by `(freeze <x> {:compressor :auto})."
  default-freeze-compressor-selector)

(defn set-default-freeze-compressor-selector!
  "Sets root binding of `*default-freeze-compressor-selector*`"
  [selector]
  (alter-var-root #'*default-freeze-compressor-selector* (constantly selector)))

(defn freeze
  "Serializes arg (any Clojure data type) to a byte array. To freeze custom
  types, extend the Clojure reader or see `extend-freeze`."
  (^bytes [x] (freeze x nil))
  (^bytes [x {:keys [compressor encryptor password]
              :or   {compressor :auto
                     encryptor  aes128-encryptor}
              :as   opts}]
    (let [;; Intentionally undocumented:
          no-header? (or (:no-header? opts) (:skip-header? opts))
          encryptor  (when password encryptor)
          baos (ByteArrayOutputStream. 64)
          dos  (DataOutputStream. baos)]

      (if (and (nil? compressor) (nil? encryptor))
        (do ; Optimized case
          (when-not no-header? ; Avoid `wrap-header`'s array copy:
            (let [head-ba (get-head-ba {:compressor-id nil :encryptor-id nil})]
              (.write dos head-ba 0 4)))
          (freeze-to-out! dos x)
          (.toByteArray baos))

        (do
          (freeze-to-out! dos x)
          (let [ba (.toByteArray baos)

                compressor
                (if (identical? compressor :auto)
                  (if no-header?
                    lz4-compressor
                    (*default-freeze-compressor-selector* ba))
                  (if (fn? compressor)
                    (compressor ba) ; Assume compressor selector fn
                    compressor      ; Assume compressor
                    ))

                ba (if compressor (compress compressor         ba) ba)
                ba (if encryptor  (encrypt  encryptor password ba) ba)]

            (if no-header?
              ba
              (wrap-header ba
                {:compressor-id
                 (when-let [c compressor]
                   (or (compression/standard-header-ids
                       (compression/header-id c))
                       :else))

                 :encryptor-id
                 (when-let [e encryptor]
                   (or (encryption/standard-header-ids
                       (encryption/header-id e))
                       :else))}))))))))

;;;; Thawing

(declare thaw-from-in!)

(defn read-bytes ^bytes [^DataInput in]
  (let [len (.readInt in)
        ba  (byte-array len)]
    (.readFully in ba 0 len)
    ba))

(defn read-sm-bytes ^bytes [^DataInput in]
  (let [len (.readByte in)
        ba  (byte-array len)]
    (.readFully in ba 0 len)
    ba))

(defn read-biginteger ^BigInteger [^DataInput in] (BigInteger. (read-bytes    in)))
(defn read-utf8           ^String [^DataInput in] (String.     (read-bytes    in) "UTF-8"))
(defn read-sm-utf8        ^String [^DataInput in] (String.     (read-sm-bytes in) "UTF-8"))

(defn read-coll [^DataInput in to-coll]
  (enc/repeatedly-into to-coll (.readInt in) (fn [] (thaw-from-in! in))))

(defn read-sm-coll [^DataInput in to-coll]
  (enc/repeatedly-into to-coll (.readByte in) (fn [] (thaw-from-in! in))))

(defn read-kvs [^DataInput in to-coll]
  (enc/repeatedly-into to-coll (.readInt in)
    (fn [] [(thaw-from-in! in) (thaw-from-in! in)])))

(defn read-sm-kvs [^DataInput in to-coll]
  (enc/repeatedly-into to-coll (.readByte in)
    (fn [] [(thaw-from-in! in) (thaw-from-in! in)])))

(defn read-kvs-depr1 [^DataInput in to-coll]
  (enc/repeatedly-into to-coll (quot (.readInt in) 2)
    (fn [] [(thaw-from-in! in) (thaw-from-in! in)])))

(def ^:private class-method-sig (into-array Class [IPersistentMap]))

(enc/defonce* ^:dynamic *custom-readers* "{<hash-or-byte-id> (fn [data-input])}" nil)
(defn swap-custom-readers! [f] (alter-var-root #'*custom-readers* f))

(defn- read-custom! [type-id in]
  (if-let [custom-reader (get *custom-readers* type-id)]
    (try
      (custom-reader in)
      (catch Exception e
        (throw
          (ex-info
            (format "Reader exception for custom type with internal id: %s"
              type-id) {:internal-type-id type-id} e))))
    (throw
      (ex-info
        (format "No reader provided for custom type with internal id: %s"
          type-id)
        {:internal-type-id type-id}))))

(defn thaw-from-in!
  "Deserializes a frozen object from given DataInput to its original Clojure
  data type"
  [^DataInput data-input]
  (let [in      data-input
        type-id (.readByte in)]
    (when-debug (println (format "DEBUG - thawing type-id: %s" type-id)))
    (try
      (enc/case-eval type-id

        id-reader
        (let [edn (read-utf8 in)]
          (try
            (enc/read-edn {:readers *data-readers*} edn)
            (catch Exception e
              {:type :reader
               :throwable e
               :nippy/unthawable edn})))

        id-serializable
        (let [class-name (read-utf8 in)]
          (try
            (let [content (.readObject (ObjectInputStream. in))]
              (try
                (let [class (Class/forName class-name)]
                  (cast class content))
                (catch Exception e
                  {:type :serializable
                   :throwable e
                   :nippy/unthawable {:class-name class-name :content content}})))
            (catch Exception e
              {:type :serializable
               :throwable e
               :nippy/unthawable {:class-name class-name :content nil}})))

        id-record
        (let [class-name (read-utf8     in)
              content    (thaw-from-in! in)]
          (try
            (let [class  (Class/forName class-name)
                  method (.getMethod class "create" class-method-sig)]
              (.invoke method class (into-array Object [content])))
            (catch Exception e
              {:type :record
               :throwable e
               :nippy/unthawable {:class-name class-name :content content}})))

        id-nil     nil
        id-boolean (.readBoolean in)
        id-char    (.readChar    in)

        id-bytes    (read-bytes    in)
        id-sm-bytes (read-sm-bytes in)

        id-string              (read-utf8    in)
        id-sm-string           (read-sm-utf8 in)
        id-keyword    (keyword (read-utf8    in))
        id-sm-keyword (keyword (read-sm-utf8 in))

        id-queue      (read-coll in (PersistentQueue/EMPTY))
        id-sorted-set (read-coll in (sorted-set))
        id-sorted-map (read-kvs  in (sorted-map))

        id-vec        (read-coll    in  [])
        id-sm-vec     (read-sm-coll in  [])
        id-set        (read-coll    in #{})
        id-sm-set     (read-sm-coll in #{})
        id-map        (read-kvs     in  {})
        id-sm-map     (read-sm-kvs  in  {})

        id-list (into '() (rseq (read-coll in [])))
        id-seq  (or (seq (read-coll in []))
                    (lazy-seq nil) ; Empty coll
                    )

        id-meta (let [m (thaw-from-in! in)] (with-meta (thaw-from-in! in) m))

        id-byte    (.readByte  in)
        id-short   (.readShort in)
        id-integer (.readInt   in)
        id-long    (.readLong  in)

        ;;; Optimized, common-case types (v2.6+)
        id-byte-as-long  (long (.readByte  in))
        id-short-as-long (long (.readShort in))
        id-int-as-long   (long (.readInt   in))

        id-bigint     (bigint (read-biginteger in))
        id-biginteger         (read-biginteger in)

        id-float  (.readFloat  in)
        id-double (.readDouble in)
        id-bigdec (BigDecimal. (read-biginteger in) (.readInt in))

        id-ratio (clojure.lang.Ratio.
                   (read-biginteger in)
                   (read-biginteger in))

        id-date  (Date. (.readLong in))
        id-uuid  (UUID. (.readLong in) (.readLong in))

        ;;; DEPRECATED
        id-sorted-map-depr1 (read-kvs-depr1 in (sorted-map))
        id-map-depr2        (read-kvs-depr1 in {})
        id-reader-depr1 (enc/read-edn (.readUTF in))
        id-string-depr1           (.readUTF in)
        id-keyword-depr1 (keyword (.readUTF in))
        id-map-depr1    (apply hash-map
                          (enc/repeatedly-into [] (* 2 (.readInt in))
                            (fn [] (thaw-from-in! in))))

        id-prefixed-custom ; Prefixed custom type
        (let [hash-id (.readShort in)]
          (read-custom! hash-id in))

        (read-custom! type-id in) ; Unprefixed custom type (catchall)
        )

      (catch Exception e
        (throw (ex-info (format "Thaw failed against type-id: %s" type-id)
                 {:type-id type-id} e))))))

(defn- try-parse-header [ba]
  (when-let [[head-ba data-ba] (enc/ba-split ba 4)]
    (let [[head-sig* [meta-id]] (enc/ba-split head-ba 3)]
      (when (enc/ba= head-sig* head-sig) ; Header appears to be well-formed
        [data-ba (get head-meta meta-id {:unrecognized-meta? true})]))))

(defn- get-auto-compressor [compressor-id]
  (case compressor-id
    nil        nil
    :snappy    snappy-compressor
    :lzma2     lzma2-compressor
    :lz4       lz4-compressor
    :no-header (throw (ex-info ":auto not supported on headerless data." {}))
    :else (throw (ex-info ":auto not supported for non-standard compressors." {}))
    (throw (ex-info (format "Unrecognized :auto compressor id: %s" compressor-id)
                    {:compressor-id compressor-id}))))

(defn- get-auto-encryptor [encryptor-id]
  (case encryptor-id
    nil            nil
    :aes128-sha512 aes128-encryptor
    :no-header     (throw (ex-info ":auto not supported on headerless data." {}))
    :else (throw (ex-info ":auto not supported for non-standard encryptors." {}))
    (throw (ex-info (format "Unrecognized :auto encryptor id: %s" encryptor-id)
                    {:encryptor-id encryptor-id}))))

(def ^:private err-msg-unknown-thaw-failure
  "Decryption/decompression failure, or data unfrozen/damaged.")

(def ^:private err-msg-unrecognized-header
  "Unrecognized (but apparently well-formed) header. Data frozen with newer Nippy version?")

(defn thaw
  "Deserializes a frozen object from given byte array to its original Clojure
  data type. To thaw custom types, extend the Clojure reader or see `extend-thaw`.

  ** By default, supports data frozen with Nippy v2+ ONLY **
  Add `{:v1-compatibility? true}` option to support thawing of data frozen with
  legacy versions of Nippy.

  Options include:
    :v1-compatibility? - support data frozen by legacy versions of Nippy?
    :compressor - :auto (checks header, default)  an ICompressor, or nil
    :encryptor  - :auto (checks header, default), an IEncryptor,  or nil"

  ([ba] (thaw ba nil))
  ([^bytes ba
    {:keys [v1-compatibility? compressor encryptor password]
     :or   {compressor :auto
            encryptor  :auto}
     :as   opts}]

   (assert (not (:headerless-meta opts))
     ":headerless-meta `thaw` opt removed in Nippy v2.7+")

   (let [v2+?       (not v1-compatibility?)
         no-header? (:no-header? opts) ; Intentionally undocumented
         ex (fn ex
              ([  msg] (ex nil msg))
              ([e msg] (throw (ex-info (format "Thaw failed: %s" msg)
                                {:opts (merge opts
                                         {:compressor compressor
                                          :encryptor  encryptor})}
                                e))))

         thaw-data
         (fn [data-ba compressor-id encryptor-id ex-fn]
           (let [compressor (if (identical? compressor :auto)
                              (get-auto-compressor compressor-id)
                              compressor)
                 encryptor  (if (identical? encryptor :auto)
                              (get-auto-encryptor encryptor-id)
                              encryptor)]

             (when (and encryptor (not password))
               (ex "Password required for decryption."))

             (try
               (let [ba data-ba
                     ba (if encryptor  (decrypt    encryptor password ba) ba)
                     ba (if compressor (decompress compressor         ba) ba)
                     dis (DataInputStream. (ByteArrayInputStream. ba))]
                 (thaw-from-in! dis))

               (catch Exception e (ex-fn e)))))

         ;; Hackish + can actually segfault JVM due to Snappy bug,
         ;; Ref. http://goo.gl/mh7Rpy - no better alternatives, unfortunately
         thaw-v1-data
         (fn [data-ba ex-fn]
           (thaw-data data-ba :snappy nil
             (fn [_] (thaw-data data-ba nil nil (fn [_] (ex-fn nil))))))]

     (if no-header?
       (if v2+?
         (thaw-data ba :no-header :no-header (fn [e] (ex e err-msg-unknown-thaw-failure)))
         (thaw-data ba :no-header :no-header
           (fn [e] (thaw-v1-data ba (fn [_] (ex e err-msg-unknown-thaw-failure))))))

       ;; At this point we assume that we have a header iff we have v2+ data
       (if-let [[data-ba {:keys [compressor-id encryptor-id unrecognized-meta?]
                          :as   head-meta}] (try-parse-header ba)]

         ;; A well-formed header _appears_ to be present (it's possible though
         ;; unlikely that this is a fluke and data is actually headerless):
         (if v2+?
           (if unrecognized-meta?
             (ex err-msg-unrecognized-header)
             (thaw-data data-ba compressor-id encryptor-id
               (fn [e] (ex e err-msg-unknown-thaw-failure))))

           (if unrecognized-meta?
             (thaw-v1-data ba (fn [_] (ex err-msg-unrecognized-header)))
             (thaw-data data-ba compressor-id encryptor-id
               (fn [e] (thaw-v1-data ba (fn [_] (ex e err-msg-unknown-thaw-failure)))))))

         ;; Well-formed header definitely not present
         (if v2+?
           (ex err-msg-unknown-thaw-failure)
           (thaw-v1-data ba (fn [_] (ex err-msg-unknown-thaw-failure)))))))))

(comment
  (thaw (freeze "hello"))
  (thaw (freeze "hello" {:compressor nil}))
  (thaw (freeze "hello" {:password [:salted "p"]})) ; ex: no pwd
  (thaw (freeze "hello") {:password [:salted "p"]}))

;;;; Custom types

(defn- assert-custom-type-id [custom-type-id]
  (assert (or      (keyword? custom-type-id)
              (and (integer? custom-type-id) (<= 1 custom-type-id 128)))))

(defn- coerce-custom-type-id
  "* +ive byte id -> -ive byte id (for unprefixed custom types)
  * Keyword id   -> Short hash id (for prefixed custom types)"
  [custom-type-id]
  (assert-custom-type-id custom-type-id)
  (if-not (keyword? custom-type-id)
    (int (- ^long custom-type-id))
    (let [^int hash-id  (hash custom-type-id)
          short-hash-id (if (pos? hash-id)
                          (mod hash-id Short/MAX_VALUE)
                          (mod hash-id Short/MIN_VALUE))]
      ;; Make sure hash ids can't collide with byte ids (unlikely anyway):
      (assert (not (<= -128 short-hash-id -1))
        "Custom type id hash collision; please choose a different id")
      (int short-hash-id))))

(comment (coerce-custom-type-id 77)
         (coerce-custom-type-id :foo/bar))

(defmacro extend-freeze
  "Extends Nippy to support freezing of a custom type (ideally concrete) with
  given id of form:
    * Keyword           - 2 byte overhead, resistent to id collisions
    * Integer ∈[1, 128] - no overhead, subject to id collisions

  (defrecord MyType [data])
  (extend-freeze MyType :foo/my-type [x data-output] ; Keyword id
    (.writeUTF [data-output] (:data x)))
  ;; or
  (extend-freeze MyType 1 [x data-output] ; Byte id
    (.writeUTF [data-output] (:data x)))"
  [type custom-type-id [x out] & body]
  (assert-custom-type-id custom-type-id)
  `(extend-type ~type Freezable
     (~'-freeze-to-out [~x ~(with-meta out {:tag 'java.io.DataOutput})]
       (if-not ~(keyword? custom-type-id)
         ;; Unprefixed [cust byte id][payload]:
         (write-id ~out ~(coerce-custom-type-id custom-type-id))
         ;; Prefixed [const byte id][cust hash id][payload]:
         (do (write-id    ~out id-prefixed-custom)
             (.writeShort ~out ~(coerce-custom-type-id custom-type-id))))
       ~@body)))

(defmacro extend-thaw
  "Extends Nippy to support thawing of a custom type with given id:
  (extend-thaw :foo/my-type [data-input] ; Keyword id
    (->MyType (.readUTF data-input)))
  ;; or
  (extend-thaw 1 [data-input] ; Byte id
    (->MyType (.readUTF data-input)))"
  [custom-type-id [in] & body]
  (assert-custom-type-id custom-type-id)
  `(do
     (when (contains? *custom-readers* ~(coerce-custom-type-id custom-type-id))
       (println (format "Warning: resetting Nippy thaw for custom type with id: %s"
                  ~custom-type-id)))
     (swap-custom-readers!
       (fn [m#]
         (assoc m#
           ~(coerce-custom-type-id custom-type-id)
           (fn [~(with-meta in {:tag 'java.io.DataInput})]
             ~@body))))))

(comment
  *custom-readers*
  (defrecord MyType [data])
  (extend-freeze MyType 1 [x out] (.writeUTF out (:data x)))
  (extend-thaw 1 [in] (->MyType (.readUTF in)))
  (thaw (freeze (->MyType "Joe"))))

;;;; Stress data

(defrecord StressRecord [data])
(def stress-data "Reference data used for tests & benchmarks"
  {:bytes        (byte-array [(byte 1) (byte 2) (byte 3)])
   :nil          nil
   :boolean      true

   :char-utf8    \ಬ
   :string-utf8  "ಬಾ ಇಲ್ಲಿ ಸಂಭವಿಸ"
   :string-long  (apply str (range 1000))
   :keyword      :keyword
   :keyword-ns   ::keyword

   ;;; Try reflect real-world data:
   :lotsa-small-numbers  (vec (range 200))
   :lotsa-small-keywords (->> (java.util.Locale/getISOLanguages)
                              (mapv keyword))
   :lotsa-small-strings  (->> (java.util.Locale/getISOCountries)
                              (mapv #(.getDisplayCountry
                                       (java.util.Locale. "en" %))))

   :queue        (-> (PersistentQueue/EMPTY) (conj :a :b :c :d :e :f :g))
   :queue-empty  (PersistentQueue/EMPTY)
   :sorted-set   (sorted-set 1 2 3 4 5)
   :sorted-map   (sorted-map :b 2 :a 1 :d 4 :c 3)

   :list         (list 1 2 3 4 5 (list 6 7 8 (list 9 10)))
   :list-quoted  '(1 2 3 4 5 (6 7 8 (9 10)))
   :list-empty   (list)
   :vector       [1 2 3 4 5 [6 7 8 [9 10]]]
   :vector-empty []
   :map          {:a 1 :b 2 :c 3 :d {:e 4 :f {:g 5 :h 6 :i 7}}}
   :map-empty    {}
   :set          #{1 2 3 4 5 #{6 7 8 #{9 10}}}
   :set-empty    #{}
   :meta         (with-meta {:a :A} {:metakey :metaval})

   :lazy-seq     (repeatedly 1000 rand)
   :lazy-seq-empty (map identity '())

   :byte         (byte 16)
   :short        (short 42)
   :integer      (int 3)
   :long         (long 3)
   :bigint       (bigint 31415926535897932384626433832795)

   :float        (float 3.14)
   :double       (double 3.14)
   :bigdec       (bigdec 3.1415926535897932384626433832795)

   :ratio        22/7
   :uuid         (java.util.UUID/randomUUID)
   :date         (java.util.Date.)

   :stress-record (->StressRecord "data")

   ;; Serializable
   :throwable    (Throwable. "Yolo")
   :exception    (try (/ 1 0) (catch Exception e e))
   :ex-info      (ex-info "ExInfo" {:data "data"})})

(def stress-data-comparable
  "Reference data with stuff removed that breaks roundtrip equality"
  (dissoc stress-data :bytes :throwable :exception :ex-info))

(def stress-data-benchable
  "Reference data with stuff removed that breaks reader or other utils we'll
  be benching against"
  (dissoc stress-data :bytes :throwable :exception :ex-info :queue :queue-empty
                      :byte :stress-record))

;;;; Tools

(defn inspect-ba "Alpha - subject to change"
  ([ba          ] (inspect-ba ba nil))
  ([ba thaw-opts]
   (when (enc/bytes? ba)
     (let [[first2bytes nextbytes] (enc/ba-split ba 2)
           ?known-wrapper
           (cond
             (enc/ba= first2bytes (.getBytes "\u0000<" "UTF8")) :carmine/bin
             (enc/ba= first2bytes (.getBytes "\u0000>" "UTF8")) :carmine/clj)

           unwrapped-ba (if ?known-wrapper nextbytes ba)
           [data-ba ?nippy-header] (or (try-parse-header unwrapped-ba)
                                       [unwrapped-ba :no-header])]

       {:?known-wrapper  ?known-wrapper
        :?header         ?nippy-header
        :thawable?       (try (thaw unwrapped-ba thaw-opts) true
                              (catch Exception _ false))
        :unwrapped-ba    unwrapped-ba
        :data-ba         data-ba
        :unwrapped-len   (alength ^bytes unwrapped-ba)
        :ba-len          (alength ^bytes ba)
        :data-len        (alength ^bytes data-ba)}))))

(comment
  (inspect-ba (freeze "hello"))
  (seq (:data-ba (inspect-ba (freeze "hello")))))

;;;; Deprecated

;; Deprecated by :auto compressor selection
(defrecord     Compressable-LZMA2 [value]) ; Why was this `LZMA2` instead of `lzma2`?
(extend-freeze Compressable-LZMA2 128 [x out]
  (let [ba (freeze (:value x) {:no-header? true :compressor nil})
        ba-len    (alength ba)
        compress? (> ba-len 1024)]
    (.writeBoolean out compress?)
    (if compress?
      (write-bytes out (compress lzma2-compressor ba))
      (write-bytes out ba))))

(extend-thaw 128 [in]
  (let [compressed? (.readBoolean in)
        ba          (read-bytes   in)]
    (thaw ba {:no-header? true
              :compressor (when compressed? lzma2-compressor)
              :encryptor  nil})))

(comment
  (->> (apply str (repeatedly 1000 rand))
       (->Compressable-LZMA2)
       (freeze)
       (thaw))
  (count (->> (apply str (repeatedly 1000 rand)) (freeze)))
  (count (->> (apply str (repeatedly 1000 rand))
              (->Compressable-LZMA2)
              (freeze))))

(ns taoensso.nippy
  "High-performance JVM Clojure serialization library. Originally adapted from
  Deep-Freeze (https://goo.gl/OePPGr)."
  {:author "Peter Taoussanis (@ptaoussanis)"}
  (:require [taoensso.encore :as encore]
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

(if (vector? taoensso.encore/encore-version)
  (encore/assert-min-encore-version [2 16 0])
  (encore/assert-min-encore-version  2.16))

;;;; Nippy data format
;; * 4-byte header (Nippy v2.x+) (may be disabled but incl. by default) [1].
;; { * 1-byte type id.
;;   * Arb-length payload. } ...
;;
;; [1] Inclusion of header is strongly recommended. Purpose:
;;   * Sanity check (confirm that data appears to be Nippy data).
;;   * Nippy version check (=> supports changes to data schema over time).
;;   * Supports :auto thaw compressor, encryptor.
;;   * Supports :auto freeze compressor (since this depends on :auto thaw
;;     compressor).
;;
(def ^:private ^:const head-version 1)
(def ^:private head-sig (.getBytes "NPY" "UTF-8"))
(def ^:private ^:const head-meta "Final byte stores version-dependent metadata."
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

(defmacro when-debug-mode [& body] (when #_true false `(do ~@body)))

;;;; Data type IDs

(do ; Just for easier IDE collapsing

  ;; ** Negative ids reserved for user-defined types **
  ;;
  (def ^:const id-reserved        (int 0))
  ;;                                   1     ; Deprecated
  (def ^:const id-bytes           (int 2))
  (def ^:const id-nil             (int 3))
  (def ^:const id-boolean         (int 4))
  (def ^:const id-reader          (int 5))   ; Fallback #2
  (def ^:const id-serializable    (int 6))   ; Fallback #1

  (def ^:const id-char            (int 10))
  ;;                                   11    ; Deprecated
  ;;                                   12    ; Deprecated
  (def ^:const id-string          (int 13))
  (def ^:const id-keyword         (int 14))

  (def ^:const id-list            (int 20))
  (def ^:const id-vector          (int 21))
  ;;                                   22    ; Deprecated
  (def ^:const id-set             (int 23))
  (def ^:const id-seq             (int 24))
  (def ^:const id-meta            (int 25))
  (def ^:const id-queue           (int 26))
  (def ^:const id-map             (int 27))
  (def ^:const id-sorted-set      (int 28))
  (def ^:const id-sorted-map      (int 29))

  (def ^:const id-byte            (int 40))
  (def ^:const id-short           (int 41))
  (def ^:const id-integer         (int 42))
  (def ^:const id-long            (int 43))
  (def ^:const id-bigint          (int 44))
  (def ^:const id-biginteger      (int 45))

  (def ^:const id-float           (int 60))
  (def ^:const id-double          (int 61))
  (def ^:const id-bigdec          (int 62))

  (def ^:const id-ratio           (int 70))

  (def ^:const id-record          (int 80))
  ;; (def ^:const id-type         (int 81))  ; TODO?
  (def ^:const id-prefixed-custom (int 82))

  (def ^:const id-date            (int 90))
  (def ^:const id-uuid            (int 91))

  ;;; Optimized, common-case types (v2.6+)
  (def ^:const id-byte-as-long    (int 100)) ; 1 vs 8 bytes
  (def ^:const id-short-as-long   (int 101)) ; 2 vs 8 bytes
  (def ^:const id-int-as-long     (int 102)) ; 4 vs 8 bytes
  ;; (def ^:const id-compact-long (int 103)) ; 6->7 vs 8 bytes
  ;;
  (def ^:const id-string-small    (int 105)) ; 1 vs 4 byte length prefix
  (def ^:const id-keyword-small   (int 106)) ; ''
  ;;
  ;; (def ^:const id-vector-small (int 110)) ; ''
  ;; (def ^:const id-set-small    (int 111)) ; ''
  ;; (def ^:const id-map-small    (int 112)) ; ''

  ;;; DEPRECATED (old types will be supported only for thawing)
  (def ^:const id-reader-depr1    (int 1))   ; v0.9.2+ for +64k support
  (def ^:const id-string-depr1    (int 11))  ; v0.9.2+ for +64k support
  (def ^:const id-map-depr1       (int 22))  ; v0.9.0+ for more efficient thaw
  (def ^:const id-keyword-depr1   (int 12))  ; v2.0.0-alpha5+ for str consistecy
  )

;;;; Ns imports (mostly for convenience of lib consumers)

(encore/defalias compress          compression/compress)
(encore/defalias decompress        compression/decompress)
(encore/defalias snappy-compressor compression/snappy-compressor)
(encore/defalias lzma2-compressor  compression/lzma2-compressor)
(encore/defalias lz4-compressor    compression/lz4-compressor)
(encore/defalias lz4hc-compressor  compression/lz4hc-compressor)

(encore/defalias encrypt           encryption/encrypt)
(encore/defalias decrypt           encryption/decrypt)
(encore/defalias aes128-encryptor  encryption/aes128-encryptor)

(encore/defalias freezable?        utils/freezable?)

;;;; Freezing

(defprotocol Freezable
  "Be careful about extending to interfaces, Ref. http://goo.gl/6gGRlU."
  (freeze-to-out* [this out]))

(defmacro write-id    [out id] `(.writeByte ~out ~id))
(defmacro write-bytes [out ba & [small?]]
  (let [out (with-meta out {:tag 'java.io.DataOutput})
        ba  (with-meta ba  {:tag 'bytes})]
    (if small? ; Optimization, must be known before id's written
      `(let [out# ~out, ba# ~ba
             size# (alength ba#)]
         (.writeByte out# (byte size#))
         (.write     out# ba# 0 size#))
      `(let [out# ~out, ba# ~ba
             size# (alength ba#)]
         (.writeInt out#  (int size#))
         (.write    out# ba# 0 size#)))))

(defmacro write-biginteger [out x]
  (let [x (with-meta x {:tag 'java.math.BigInteger})]
    `(write-bytes ~out (.toByteArray ~x))))

(defmacro write-utf8 [out x & [small?]]
  (let [x (with-meta x {:tag 'String})]
    `(write-bytes ~out (.getBytes ~x "UTF-8") ~small?)))

(defmacro write-compact-long "Uses 2->9 bytes." [out x]
  `(write-bytes ~out (.toByteArray (java.math.BigInteger/valueOf (long ~x)))
                :small))

(comment (alength (.toByteArray (java.math.BigInteger/valueOf Long/MAX_VALUE))))

(defmacro ^:private freeze-to-out
  "Like `freeze-to-out*` but with metadata support."
  [out x]
  `(let [out# ~out, x# ~x]
     (when-let [m# (meta x#)]
       (write-id  out# ~id-meta)
       (freeze-to-out* m# out#))
     (freeze-to-out* x# out#)))

(defmacro ^:private freezer [type id & body]
  `(extend-type ~type
     Freezable
     (~'freeze-to-out* [~'x ~(with-meta 'out {:tag 'DataOutput})]
       (write-id ~'out ~id)
       ~@body)))

(defmacro ^:private freezer-coll [type id & body]
  `(freezer ~type ~id
     (when-debug-mode
      (when (instance? ISeq ~type)
        (println (format "DEBUG - freezer-coll: %s for %s" ~type (type ~'x)))))
     (if (counted? ~'x)
       (do (.writeInt ~'out (count ~'x))
           (encore/run!* (fn [i#] (freeze-to-out ~'out i#)) ~'x))
       (let [bas#  (ByteArrayOutputStream. 64)
             sout# (DataOutputStream. bas#)
             cnt#  (reduce (fn [^long cnt# i#]
                             (freeze-to-out sout# i#)
                             (unchecked-inc cnt#))
                           0 ~'x)
             ba# (.toByteArray bas#)]
         (.writeInt ~'out cnt#)
         (.write ~'out ba# 0 (alength ba#))))))

(defmacro ^:private freezer-kvs [type id & body]
  `(freezer ~type ~id
    (.writeInt ~'out (* 2 (count ~'x))) ; *2 here is vestigial
    (encore/run-kv!
      (fn [k# v#]
        (freeze-to-out ~'out k#)
        (freeze-to-out ~'out v#))
      ~'x)))

(freezer (Class/forName "[B") id-bytes   (write-bytes out ^bytes x))
(freezer nil                  id-nil)
(freezer Boolean              id-boolean (.writeBoolean out x))

(freezer Character id-char    (.writeChar out (int x)))
;; (freezer String id-string  (write-utf8 out x))

(extend-type String ; Optimized common-case type
  Freezable
  (freeze-to-out* [x ^DataOutput out]
    (let [ba (.getBytes x "UTF-8")]
      (if (<= (alength ^bytes ba) Byte/MAX_VALUE)
        (do (write-id    out id-string-small)
            (write-bytes out ba :small))

        (do (write-id    out id-string)
            (write-bytes out ba))))))

(extend-type Keyword ; Optimized common-case type
  Freezable
  (freeze-to-out* [x ^DataOutput out]
    (let [s (if-let [ns (namespace x)]
              (str ns "/" (name x))
              (name x))
          ba (.getBytes s "UTF-8")]

      (if (<= (alength ^bytes ba) Byte/MAX_VALUE)
        (do (write-id    out id-keyword-small)
            (write-bytes out ba :small))

        (do (write-id    out id-keyword)
            (write-bytes out ba))))))

(freezer-coll PersistentQueue       id-queue)
(freezer-coll PersistentTreeSet     id-sorted-set)
(freezer-kvs  PersistentTreeMap     id-sorted-map)

(freezer-kvs  APersistentMap        id-map)
(freezer-coll APersistentVector     id-vector)
(freezer-coll APersistentSet        id-set)
(freezer-coll PersistentList        id-list) ; No APersistentList
(freezer-coll (type '())            id-list)

;; Nb low-level interface!! Acts as fallback for seqs that don't have a
;; concrete implementation. Will conflict with any other coll interfaces!
(freezer-coll ISeq                  id-seq)

(freezer IRecord id-record
         (write-utf8 out (.getName (class x))) ; Reflect
         (freeze-to-out out (into {} x)))

(freezer Byte    id-byte    (.writeByte  out x))
(freezer Short   id-short   (.writeShort out x))
(freezer Integer id-integer (.writeInt   out x))
;;(freezer Long  id-long    (.writeLong  out x))
(extend-type Long ; Optimized common-case type
  Freezable
  (freeze-to-out* [x ^DataOutput out]
    (let [^long x x]
      (cond
        (and (<= x #_Byte/MAX_VALUE  127)
             (<=   #_Byte/MIN_VALUE -128 x))
        (do (write-id   out id-byte-as-long)
            (.writeByte out x))

        (and (<= x #_Short/MAX_VALUE  32767)
             (<=   #_Short/MIN_VALUE -32768 x))
        (do (write-id    out id-short-as-long)
            (.writeShort out x))

        (and (<= x #_Integer/MAX_VALUE  2147483647)
             (<=   #_Integer/MIN_VALUE -2147483648 x))
        (do (write-id  out id-int-as-long)
            (.writeInt out x))

        :else (do (write-id   out id-long)
                  (.writeLong out x))))))

;;

(freezer BigInt     id-bigint     (write-biginteger out (.toBigInteger x)))
(freezer BigInteger id-biginteger (write-biginteger out x))

(freezer Float      id-float   (.writeFloat  out x))
(freezer Double     id-double  (.writeDouble out x))
(freezer BigDecimal id-bigdec
         (write-biginteger out (.unscaledValue x))
         (.writeInt out (.scale x)))

(freezer Ratio id-ratio
         (write-biginteger out (.numerator   x))
         (write-biginteger out (.denominator x)))

(freezer Date id-date (.writeLong out (.getTime x)))
(freezer UUID id-uuid
         (.writeLong out (.getMostSignificantBits  x))
         (.writeLong out (.getLeastSignificantBits x)))

(def ^:dynamic *final-freeze-fallback* "Alpha - subject to change." nil)
(defn freeze-fallback-as-str "Alpha-subject to change." [x out]
  (freeze-to-out* {:nippy/unfreezable (encore/pr-edn x) :type (type x)} out))

(comment
  (require '[clojure.core.async :as async])
  (binding [*final-freeze-fallback* freeze-fallback-as-str]
    (-> (async/chan) (freeze) (thaw))))

;; Fallbacks. Note that we'll extend *only* to (lowly) Object to prevent
;; interfering with higher-level implementations, Ref. http://goo.gl/6f7SKl
(extend-type Object
  Freezable
  (freeze-to-out* [x ^DataOutput out]
    (cond
     (utils/serializable? x) ; Fallback #1: Java's Serializable interface
     (do (when-debug-mode
          (println (format "DEBUG - Serializable fallback: %s" (type x))))
         (write-id out id-serializable)
         (write-utf8 out (.getName (class x))) ; Reflect
         (.writeObject (ObjectOutputStream. out) x))

     (utils/readable? x) ; Fallback #2: Clojure's Reader
     (do (when-debug-mode
          (println (format "DEBUG - Reader fallback: %s" (type x))))
         (write-id out id-reader)
         (write-utf8 out (encore/pr-edn x)))

     :else ; Fallback #3: *final-freeze-fallback*
     (if-let [ffb *final-freeze-fallback*] (ffb x out)
       (throw (ex-info (format "Unfreezable type: %s %s" (type x) (str x))
                {:type   (type x)
                 :as-str (encore/pr-edn x)}))))))

(def ^:private head-meta-id (reduce-kv #(assoc %1 %3 %2) {} head-meta))
(def ^:private get-head-ba
  (memoize
   (fn [head-meta]
     (when-let [meta-id (get head-meta-id (assoc head-meta :version head-version))]
       (encore/ba-concat head-sig (byte-array [meta-id]))))))

(defn- wrap-header [data-ba head-meta]
  (if-let [head-ba (get-head-ba head-meta)]
    ;; TODO Would be nice if we could avoid the array copy here:
    (encore/ba-concat head-ba data-ba)
    (throw (ex-info (format "Unrecognized header meta: %s" head-meta)
             {:head-meta head-meta}))))

(comment (wrap-header (.getBytes "foo") {:compressor-id :lz4
                                         :encryptor-id  nil}))

(defn freeze-to-out!
  "Low-level API. Serializes arg (any Clojure data type) to a DataOutput."
  [^DataOutput data-output x]
  (freeze-to-out data-output x))

(defn default-freeze-compressor-selector
  "Strategy:
    * Prioritize speed, but allow lz4.
    * Skip lz4 unless it's likely that lz4's space benefit will outweigh its
      space overhead."
  [^bytes ba]
  (let [ba-len (alength ba)]
    (cond
      ;; (> ba-len 4098) lzma2-compressor
      ;; (> ba-len 2048) lz4hc-compressor
         (> ba-len 1024) lz4-compressor
      :else              nil)))

(encore/defonce* ^:dynamic *default-freeze-compressor-selector*
  "(fn selector [^bytes ba])->compressor used by `(freeze <x> {:compressor :auto})."
  default-freeze-compressor-selector)

(defn set-default-freeze-compressor-selector!
  "Sets root binding of `*default-freeze-compressor-selector*`."
  [selector]
  (alter-var-root #'*default-freeze-compressor-selector* (constantly selector)))

(defn freeze
  "Serializes arg (any Clojure data type) to a byte array. To freeze custom
  types, extend the Clojure reader or see `extend-freeze`."
  (^bytes [x] (freeze x nil))
  (^bytes [x {:keys [compressor encryptor password skip-header?]
              :or   {compressor :auto
                     encryptor  aes128-encryptor}
              :as   opts}]
    (let [legacy-mode? (:legacy-mode opts) ; DEPRECATED Nippy v1-compatible freeze
          compressor   (if legacy-mode? snappy-compressor compressor)
          encryptor    (when password (if-not legacy-mode? encryptor nil))
          skip-header? (or skip-header? legacy-mode?)
          baos (ByteArrayOutputStream. 64)
          dos  (DataOutputStream. baos)]
      (freeze-to-out! dos x)
      (let [ba (.toByteArray baos)

            compressor
            (if (identical? compressor :auto)
              (if skip-header?
                lz4-compressor
                (*default-freeze-compressor-selector* ba))
              (if (fn? compressor)
                (compressor ba) ; Assume compressor selector fn
                compressor      ; Assume compressor
                ))

            ba (if compressor (compress compressor         ba) ba)
            ba (if encryptor  (encrypt  encryptor password ba) ba)]

        (if skip-header? ba
          (wrap-header ba
            {:compressor-id (when-let [c compressor]
                              (or (compression/standard-header-ids
                                  (compression/header-id c)) :else))
             :encryptor-id  (when-let [e encryptor]
                              (or (encryption/standard-header-ids
                                  (encryption/header-id e)) :else))}))))))

;;;; Thawing

(declare thaw-from-in)

(defmacro read-bytes [in & [small?]]
  (if small? ; Optimization, must be known before id's written
    `(let [in#   ~in
           size# (.readByte in#)
           ba#   (byte-array size#)]
       (.readFully in# ba# 0 size#)
       ba#)
    `(let [in#   ~in
           size# (.readInt in#)
           ba#   (byte-array size#)]
       (.readFully in# ba# 0 size#)
       ba#)))

(defmacro read-biginteger [in] `(BigInteger. (read-bytes ~in)))
(defmacro read-utf8 [in & [small?]]
  `(String. (read-bytes ~in ~small?) "UTF-8"))

(defmacro read-compact-long [in] `(long (BigInteger. (read-bytes ~in :small))))

(defmacro ^:private read-coll [in coll]
  `(let [in# ~in] (encore/repeatedly-into ~coll (.readInt in#)
                    (fn [] (thaw-from-in in#)))))

(defmacro ^:private read-kvs [in coll]
  `(let [in# ~in]
     (encore/repeatedly-into ~coll (quot (.readInt in#) 2) ; /2 here is vestigial
       (fn [] [(thaw-from-in in#) (thaw-from-in in#)]))))

(def ^:private class-method-sig (into-array Class [IPersistentMap]))

(def ^:dynamic *custom-readers* "{<hash-or-byte-id> (fn [data-input])}" nil)
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

(defn- thaw-from-in
  [^DataInput in]
  (let [type-id (.readByte in)]
    (try
      (when-debug-mode
       (println (format "DEBUG - thawing type-id: %s" type-id)))

      (encore/case-eval type-id

        id-reader
        (let [edn (read-utf8 in)]
          (try
            (encore/read-edn {:readers *data-readers*} edn)
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
        (let [class-name (read-utf8    in)
              content    (thaw-from-in in)]
          (try
            (let [class  (Class/forName class-name)
                  method (.getMethod class "create" class-method-sig)]
              (.invoke method class (into-array Object [content])))
            (catch Exception e
              {:type :record
               :throwable e
               :nippy/unthawable {:class-name class-name :content content}})))

        id-bytes   (read-bytes in)
        id-nil     nil
        id-boolean (.readBoolean in)

        id-char    (.readChar in)
        id-string           (read-utf8 in)
        id-keyword (keyword (read-utf8 in))

        ;;; Optimized, common-case types (v2.6+)
        id-string-small           (read-utf8 in :small)
        id-keyword-small (keyword (read-utf8 in :small))

        id-queue      (read-coll in (PersistentQueue/EMPTY))
        id-sorted-set (read-coll in (sorted-set))
        id-sorted-map (read-kvs  in (sorted-map))

        id-list    (into '() (rseq (read-coll in [])))
        id-vector  (read-coll in  [])
        id-set     (read-coll in #{})
        id-map     (read-kvs  in  {})
        id-seq     (or (seq (read-coll in []))
                       (lazy-seq nil) ; Empty coll
                       )

        id-meta (let [m (thaw-from-in in)] (with-meta (thaw-from-in in) m))

        id-byte    (.readByte  in)
        id-short   (.readShort in)
        id-integer (.readInt   in)
        id-long    (.readLong  in)

        ;;; Optimized, common-case types (v2.6+)
        id-byte-as-long  (long (.readByte  in))
        id-short-as-long (long (.readShort in))
        id-int-as-long   (long (.readInt   in))
        ;; id-compact-long  (read-compact-long in)

        id-bigint     (bigint (read-biginteger in))
        id-biginteger (read-biginteger in)

        id-float  (.readFloat  in)
        id-double (.readDouble in)
        id-bigdec (BigDecimal. (read-biginteger in) (.readInt in))

        ;; id-ratio (/ (bigint (read-biginteger in))
        ;;             (bigint (read-biginteger in)))

        id-ratio (clojure.lang.Ratio.
                   (read-biginteger in)
                   (read-biginteger in))

        id-date  (Date. (.readLong in))
        id-uuid  (UUID. (.readLong in) (.readLong in))

        ;;; DEPRECATED
        id-reader-depr1 (encore/read-edn (.readUTF in))
        id-string-depr1 (.readUTF in)
        id-map-depr1    (apply hash-map (encore/repeatedly-into [] (* 2 (.readInt in))
                                        (fn [] (thaw-from-in in))))
        id-keyword-depr1 (keyword (.readUTF in))

        id-prefixed-custom ; Prefixed custom type
        (let [hash-id (.readShort in)]
          (read-custom! hash-id in))

        (read-custom! type-id in) ; Unprefixed custom type (catchall)
        )

      (catch Exception e
        (throw (ex-info (format "Thaw failed against type-id: %s" type-id)
                 {:type-id type-id} e))))))

(defn thaw-from-in!
  "Low-level API. Deserializes a frozen object from given DataInput to its
  original Clojure data type."
  [data-input]
  (thaw-from-in data-input))

(defn- try-parse-header [ba]
  (when-let [[head-ba data-ba] (encore/ba-split ba 4)]
    (let [[head-sig* [meta-id]] (encore/ba-split head-ba 3)]
      (when (encore/ba= head-sig* head-sig) ; Header appears to be well-formed
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

(defn thaw
  "Deserializes a frozen object from given byte array to its original Clojure
  data type. Supports data frozen with current and all previous versions of
  Nippy. To thaw custom types, extend the Clojure reader or see `extend-thaw`.

  Options include:
    :compressor - An ICompressor, :auto (requires Nippy header), or nil.
    :encryptor  - An IEncryptor,  :auto (requires Nippy header), or nil."

  ([ba] (thaw ba nil))
  ([^bytes ba
    {:keys [v1-compatibility? compressor encryptor password]
     :or   {v1-compatibility? true ; Recommend disabling when possible
            compressor        :auto
            encryptor         :auto}
     :as   opts}]

   (assert (not (:headerless-meta opts))
     ":headerless-meta `thaw` opt removed in Nippy v2.7+")

   (let [ex (fn [msg & [e]] (throw (ex-info (format "Thaw failed: %s" msg)
                                    {:opts (merge opts
                                             {:compressor compressor
                                              :encryptor  encryptor})}
                                    e)))
         thaw-data
         (fn [data-ba compressor-id encryptor-id]
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

               (catch Exception e
                 (ex "Decryption/decompression failure, or data unfrozen/damaged."
                   e)))))

         ;; This is hackish and can actually currently result in JVM core dumps
         ;; due to buggy Snappy behaviour, Ref. http://goo.gl/mh7Rpy.
         thaw-nippy-v1-data
         (fn [data-ba]
           (if-not v1-compatibility?
             (throw (Exception. "v1 compatibility disabled"))
             (try (thaw-data data-ba :snappy nil)
                  (catch Exception _
                    (thaw-data data-ba nil nil)))))]

     (if-let [[data-ba {:keys [compressor-id encryptor-id unrecognized-meta?]
                        :as   head-meta}] (try-parse-header ba)]

       ;; A well-formed header _appears_ to be present (it's possible though
       ;; unlikely that this is a fluke and data is actually headerless):
       (try (thaw-data data-ba compressor-id encryptor-id)
            (catch Exception e
              (try (thaw-nippy-v1-data data-ba)
                   (catch Exception _
                     (if unrecognized-meta?
                       (ex "Unrecognized (but apparently well-formed) header. Data frozen with newer Nippy version?"
                         e)
                       (throw e))))))

       ;; Well-formed header definitely not present
       (try (thaw-nippy-v1-data ba)
            (catch Exception _
              (thaw-data ba :no-header :no-header)))))))

(comment (thaw (freeze "hello"))
         (thaw (freeze "hello" {:compressor nil}))
         (thaw (freeze "hello" {:password [:salted "p"]})) ; ex: no pwd
         (thaw (freeze "hello") {:password [:salted "p"]}))

;;;; Custom types

(defn- assert-custom-type-id [custom-type-id]
  (assert (or      (keyword? custom-type-id)
              (and (integer? custom-type-id) (<= 1 custom-type-id 128)))))

(defn- coerce-custom-type-id
  "* +ive byte id -> -ive byte id (for unprefixed custom types).
   * Keyword id   -> Short hash id (for prefixed custom types)."
  [custom-type-id]
  (assert-custom-type-id custom-type-id)
  (if-not (keyword? custom-type-id)
    (int (- ^long custom-type-id))
    (let [^long hash-id (hash custom-type-id)
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
    * Keyword        - 2 byte overhead, resistent to id collisions.
    * Byte ∈[1, 128] - no overhead, subject to id collisions.

  (defrecord MyType [data])
  (extend-freeze MyType :foo/my-type [x data-output] ; Keyword id
    (.writeUTF [data-output] (:data x)))
  ;; or
  (extend-freeze MyType 1 [x data-output] ; Byte id
    (.writeUTF [data-output] (:data x)))"
  [type custom-type-id [x out] & body]
  (assert-custom-type-id custom-type-id)
  `(extend-type ~type Freezable
     (~'freeze-to-out* [~x ~(with-meta out {:tag 'java.io.DataOutput})]
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

;;; Some useful custom types - EXPERIMENTAL

;; Mostly deprecated by :auto compressor selection
(defrecord Compressable-LZMA2 [value]) ; Why was this `LZMA2`, not `lzma2`?
(extend-freeze Compressable-LZMA2 128 [x out]
  (let [ba (freeze (:value x) {:skip-header? true :compressor nil})
        ba-len    (alength ba)
        compress? (> ba-len 1024)]
    (.writeBoolean out compress?)
    (if compress?
      (write-bytes out (compress lzma2-compressor ba))
      (write-bytes out ba))))

(extend-thaw 128 [in]
  (let [compressed? (.readBoolean in)
        ba          (read-bytes   in)]
    (thaw ba {:compressor (when compressed? lzma2-compressor)
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

;;;; Stress data

(defrecord StressRecord [data])
(def stress-data "Reference data used for tests & benchmarks."
  (let []
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
     :ex-info      (ex-info "ExInfo" {:data "data"})}))

(def stress-data-comparable
  "Reference data with stuff removed that breaks roundtrip equality."
  (dissoc stress-data :bytes :throwable :exception :ex-info))

(def stress-data-benchable
  "Reference data with stuff removed that breaks reader or other utils we'll
  be benching against."
  (dissoc stress-data :bytes :throwable :exception :ex-info :queue :queue-empty
                      :byte :stress-record))

;;;; Tools

(defn inspect-ba "Alpha - subject to change."
  [ba & [thaw-opts]]
  (if-not (encore/bytes? ba) :not-ba
    (let [[first2bytes nextbytes] (encore/ba-split ba 2)
          known-wrapper
          (cond
           (encore/ba= first2bytes (.getBytes "\u0000<" "UTF8")) :carmine/bin
           (encore/ba= first2bytes (.getBytes "\u0000>" "UTF8")) :carmine/clj)

          unwrapped-ba (if known-wrapper nextbytes ba)
          [data-ba nippy-header] (or (try-parse-header unwrapped-ba)
                                     [unwrapped-ba :no-header])]

      {:known-wrapper   known-wrapper
       :nippy-v2-header nippy-header ; Nippy v1.x didn't have a header
       :thawable?       (try (thaw unwrapped-ba thaw-opts) true
                             (catch Exception _ false))
       :unwrapped-ba    unwrapped-ba
       :data-ba         data-ba
       :unwrapped-size  (alength ^bytes unwrapped-ba)
       :ba-size         (alength ^bytes ba)
       :data-size       (alength ^bytes data-ba)})))

(comment (inspect-ba (freeze "hello"))
         (seq (:data-ba (inspect-ba (freeze "hello")))))

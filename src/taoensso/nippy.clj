(ns taoensso.nippy
  "Simple, high-performance Clojure serialization library. Originally adapted
  from Deep-Freeze."
  {:author "Peter Taoussanis"}
  (:require [clojure.tools.reader.edn :as edn]
            [taoensso.encore :as encore]
            [taoensso.nippy
             (utils       :as utils)
             (compression :as compression :refer (snappy-compressor))
             (encryption  :as encryption  :refer (aes128-encryptor))])
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

;;;; Nippy 2.x+ header spec (4 bytes)
;; Header is optional but recommended + enabled by default. Uses:
;; * Sanity check (data appears to be Nippy data).
;; * Nippy version check (=> supports changes to data schema over time).
;; * Encrypted &/or compressed data identification.
;;
(def ^:private ^:const head-version 1)
(def ^:private head-sig (.getBytes "NPY" "UTF-8"))
(def ^:private ^:const head-meta "Final byte stores version-dependent metadata."
  {(byte 0) {:version 1 :compressed? false :encrypted? false}
   (byte 1) {:version 1 :compressed? true  :encrypted? false}
   (byte 2) {:version 1 :compressed? false :encrypted? true}
   (byte 3) {:version 1 :compressed? true  :encrypted? true}})

(defmacro when-debug-mode [& body] (when #_true false `(do ~@body)))

;;;; Data type IDs

;; **Negative ids reserved for user-defined types**
(do ; Just for easier IDE collapsing
  (def ^:const id-reserved   (int 0))
  ;;                              1
  (def ^:const id-bytes      (int 2))
  (def ^:const id-nil        (int 3))
  (def ^:const id-boolean    (int 4))
  (def ^:const id-reader     (int 5)) ; Fallback #2: pr-str output
  (def ^:const id-serializable (int 6)) ; Fallback #1

  (def ^:const id-char       (int 10))
  ;;                              11
  ;;                              12
  (def ^:const id-string     (int 13))
  (def ^:const id-keyword    (int 14))

  (def ^:const id-list       (int 20))
  (def ^:const id-vector     (int 21))
  ;;                              22
  (def ^:const id-set        (int 23))
  (def ^:const id-seq        (int 24))
  (def ^:const id-meta       (int 25))
  (def ^:const id-queue      (int 26))
  (def ^:const id-map        (int 27))
  (def ^:const id-sorted-set (int 28))
  (def ^:const id-sorted-map (int 29))

  (def ^:const id-byte       (int 40))
  (def ^:const id-short      (int 41))
  (def ^:const id-integer    (int 42))
  (def ^:const id-long       (int 43))
  (def ^:const id-bigint     (int 44))
  (def ^:const id-biginteger (int 45))

  (def ^:const id-float      (int 60))
  (def ^:const id-double     (int 61))
  (def ^:const id-bigdec     (int 62))

  (def ^:const id-ratio      (int 70))

  (def ^:const id-record     (int 80))
  ;; (def ^:const id-type    (int 81)) ; TODO

  (def ^:const id-date       (int 90))
  (def ^:const id-uuid       (int 91))

  ;;; Optimized, common-case types (v2.6+)
  (def ^:const id-byte-as-long  (int 100)) ; 1 vs 8 bytes
  (def ^:const id-short-as-long (int 101)) ; 2 vs 8 bytes
  (def ^:const id-int-as-long   (int 102)) ; 4 vs 8 bytes
  ;; (def ^:const id-compact-long  (int 103)) ; 6->7 vs 8 bytes
  ;;
  (def ^:const id-string-small  (int 105)) ; 1 vs 4 byte length prefix
  (def ^:const id-keyword-small (int 106)) ; ''
  ;;
  ;; (def ^:const id-vector-small  (int 110)) ; ''
  ;; (def ^:const id-set-small     (int 111)) ; ''
  ;; (def ^:const id-map-small     (int 112)) ; ''

  ;;; DEPRECATED (old types will be supported only for thawing)
  (def ^:const id-old-reader  (int 1))  ; as of 0.9.2, for +64k support
  (def ^:const id-old-string  (int 11)) ; as of 0.9.2, for +64k support
  (def ^:const id-old-map     (int 22)) ; as of 0.9.0, for more efficient thaw
  (def ^:const id-old-keyword (int 12)) ; as of 2.0.0-alpha5, for str consistecy
  )

;;;; Freezing

(defprotocol Freezable
  "Be careful about extending to interfaces, Ref. http://goo.gl/6gGRlU."
  (freeze-to-out* [this out]))

(defmacro write-id    [out id] `(.writeByte ~out ~id))
(defmacro write-bytes [out ba & [small?]]
  (let [out (with-meta out {:tag 'java.io.DataOutput})
        ba  (with-meta ba  {:tag 'bytes})]
    `(let [out# ~out, ba# ~ba
           size# (alength ba#)]
       (if ~small?             ; Optimization, must be known before id's written
         (.writeByte out# (byte size#))  ; `byte` to throw on range error
         (.writeInt  out# (int  size#))  ; `int`  ''
         )
       (.write out# ba# 0 size#))))

(defmacro write-biginteger [out x]
  (let [x (with-meta x {:tag 'java.math.BigInteger})]
    `(write-bytes ~out (.toByteArray ~x))))

(defmacro write-utf8 [out x & [small?]]
  (let [x (with-meta x {:tag 'String})]
    `(write-bytes ~out (.getBytes ~x "UTF-8") ~small?)))

(defmacro write-compact-long "EXPERIMENTAL! Uses 2->9 bytes." [out x]
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
           (doseq [i# ~'x] (freeze-to-out ~'out i#)))
       (let [bas#  (ByteArrayOutputStream.)
             sout# (DataOutputStream. bas#)
             cnt#  (reduce (fn [cnt# i#]
                             (freeze-to-out sout# i#)
                             (unchecked-inc cnt#))
                           0 ~'x)
             ba# (.toByteArray bas#)]
         (.writeInt ~'out cnt#)
         (.write ~'out ba# 0 (alength ba#))))))

(defmacro ^:private freezer-kvs [type id & body]
  `(freezer ~type ~id
    (.writeInt ~'out (* 2 (count ~'x)))
    (doseq [kv# ~'x]
      (freeze-to-out ~'out (key kv#))
      (freeze-to-out ~'out (val kv#)))))

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

(freezer Byte       id-byte    (.writeByte  out x))
(freezer Short      id-short   (.writeShort out x))
(freezer Integer    id-integer (.writeInt   out x))
;;(freezer Long    id-long    (.writeLong  out x))
(extend-type Long ; Optimized common-case type
  Freezable
  (freeze-to-out* [x ^DataOutput out]
    (cond
     (<= Byte/MIN_VALUE x Byte/MAX_VALUE)
     (do (write-id out id-byte-as-long) (.writeByte out x))

     (<= Short/MIN_VALUE x Short/MAX_VALUE)
     (do (write-id out id-short-as-long) (.writeShort out x))

     (<= Integer/MIN_VALUE x Integer/MAX_VALUE)
     (do (write-id out id-int-as-long) (.writeInt out x))

     :else (do (write-id out id-long) (.writeLong out x)))))

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
  (freeze-to-out* {:nippy/unfreezable (pr-str x) :type (type x)} out))

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
         (write-utf8 out (pr-str x)))

     :else ; Fallback #3: *final-freeze-fallback*
     (if-let [ffb *final-freeze-fallback*] (ffb x out)
       (throw (Exception. (format "Unfreezable type: %s %s"
                                  (type x) (str x))))))))

(def ^:private head-meta-id (reduce-kv #(assoc %1 %3 %2) {} head-meta))

(defn- wrap-header [data-ba metadata]
  (if-let [meta-id (head-meta-id (assoc metadata :version head-version))]
    (let [head-ba (encore/ba-concat head-sig (byte-array [meta-id]))]
      (encore/ba-concat head-ba data-ba))
    (throw (Exception. (str "Unrecognized header metadata: " metadata)))))

(comment (wrap-header (.getBytes "foo") {:compressed? true
                                         :encrypted?  false}))

(declare assert-legacy-args) ; Deprecated

(defn freeze-to-out!
  "Low-level API. Serializes arg (any Clojure data type) to a DataOutput."
  [^DataOutput data-output x & _]
  (freeze-to-out data-output x))

(defn freeze
  "Serializes arg (any Clojure data type) to a byte array. For custom types
  extend the Clojure reader or see `extend-freeze`."
  ^bytes [x & [{:keys [password compressor encryptor skip-header?]
                :or   {compressor snappy-compressor
                       encryptor  aes128-encryptor}
                :as   opts}]]
  (when (:legacy-mode opts) ; Deprecated
    (assert-legacy-args compressor password))
  (let [skip-header? (or skip-header? (:legacy-mode opts)) ; Deprecated
        bas  (ByteArrayOutputStream.)
        sout (DataOutputStream. bas)]
    (freeze-to-out! sout x)
    (let [ba (.toByteArray bas)
          ba (if compressor (compression/compress compressor ba) ba)
          ba (if password   (encryption/encrypt encryptor password ba) ba)]
      (if skip-header? ba
        (wrap-header ba {:compressed? (boolean compressor)
                         :encrypted?  (boolean password)})))))

;;;; Thawing

(declare thaw-from-in)

(defmacro read-bytes [in & [small?]]
  `(let [in#   ~in
         size# (if ~small? ; Optimization, must be known before id's written
                 (.readByte in#)
                 (.readInt  in#))
         ba#   (byte-array size#)]
     (.readFully in# ba# 0 size#) ba#))

(defmacro read-biginteger [in] `(BigInteger. (read-bytes ~in)))
(defmacro read-utf8 [in & [small?]]
  `(String. (read-bytes ~in ~small?) "UTF-8"))

(defmacro read-compact-long "EXPERIMENTAL!" [in]
  `(long (BigInteger. (read-bytes ~in :small))))

(defmacro ^:private read-coll [in coll]
  `(let [in# ~in] (encore/repeatedly-into* ~coll (.readInt in#) (thaw-from-in in#))))

(defmacro ^:private read-kvs [in coll]
  `(let [in# ~in] (encore/repeatedly-into* ~coll (/ (.readInt in#) 2)
                    [(thaw-from-in in#) (thaw-from-in in#)])))

(declare ^:private custom-readers)

(defn- thaw-from-in
  [^DataInput in]
  (let [type-id (.readByte in)]
    (try
      (when-debug-mode
       (println (format "DEBUG - thawing type-id: %s" type-id)))

      (encore/case-eval type-id

        id-reader
        (let [edn (read-utf8 in)]
          (try (edn/read-string {:readers *data-readers*} edn)
               (catch Exception _ {:nippy/unthawable edn
                                   :type :reader})))

        id-serializable
        (let [class-name (read-utf8 in)]
          (try (let [;; .readObject _before_ Class/forName: it'll always read
                     ;; all data before throwing
                     object (.readObject (ObjectInputStream. in))
                     class ^Class (Class/forName class-name)]
                 (cast class object))
               (catch Exception _ {:nippy/unthawable class-name
                                   :type :serializable})))

        id-bytes   (read-bytes in)
        id-nil     nil
        id-boolean (.readBoolean in)

        id-char    (.readChar in)
        id-string  (read-utf8 in)
        id-keyword (keyword (read-utf8 in))

        ;;; Optimized, common-case types (v2.6+)
        id-string-small           (String. (read-bytes in :small) "UTF-8")
        id-keyword-small (keyword (String. (read-bytes in :small) "UTF-8"))

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

        id-ratio (/ (bigint (read-biginteger in))
                    (bigint (read-biginteger in)))

        id-record
        (let [class    ^Class (Class/forName (read-utf8 in))
              meth-sig (into-array Class [IPersistentMap])
              method   ^Method (.getMethod class "create" meth-sig)]
          (.invoke method class (into-array Object [(thaw-from-in in)])))

        id-date  (Date. (.readLong in))
        id-uuid  (UUID. (.readLong in) (.readLong in))

        ;;; DEPRECATED
        id-old-reader (edn/read-string (.readUTF in))
        id-old-string (.readUTF in)
        id-old-map    (apply hash-map (encore/repeatedly-into* []
                        (* 2 (.readInt in)) (thaw-from-in in)))
        id-old-keyword (keyword (.readUTF in))

        (if-not (neg? type-id)
          (throw (Exception. (str "Unknown type ID: " type-id)))

          ;; Custom types
          (if-let [reader (get @custom-readers type-id)]
            (try (reader in)
                 (catch Exception e
                   (throw (Exception. (str "Reader exception for custom type ID: "
                                           (- type-id)) e))))
            (throw (Exception. (str "No reader provided for custom type ID: "
                                    (- type-id)))))))

      (catch Exception e
        (throw (Exception. (format "Thaw failed against type-id: %s" type-id) e))))))

(defn thaw-from-in!
  "Low-level API. Deserializes a frozen object from given DataInput to its
  original Clojure data type."
  [data-input & _]
  (thaw-from-in data-input))

(defn- try-parse-header [ba]
  (when-let [[head-ba data-ba] (encore/ba-split ba 4)]
    (let [[head-sig* [meta-id]] (encore/ba-split head-ba 3)]
      (when (encore/ba= head-sig* head-sig) ; Appears to be well-formed
        [data-ba (head-meta meta-id {:unrecognized-meta? true})]))))

(defn thaw
  "Deserializes a frozen object from given byte array to its original Clojure
  data type. By default[1] supports data frozen with current and all previous
  versions of Nippy. For custom types extend the Clojure reader or see
  `extend-thaw`.

  [1] :headerless-meta provides a fallback facility for data frozen without a
  standard Nippy header (notably all Nippy v1 data). A default is provided for
  Nippy v1 thaw compatibility, but it's recommended that you _disable_ this
  fallback (`{:headerless-meta nil}`) if you're certain you won't be thawing
  headerless data."
  [^bytes ba & [{:keys [password compressor encryptor headerless-meta]
                 :or   {compressor snappy-compressor
                        encryptor  aes128-encryptor
                        headerless-meta ; Recommend set to nil when possible
                        {:version     1
                         :compressed? true
                         :encrypted?  false}}
                 :as   opts}]]

  (let [headerless-meta (merge headerless-meta (:legacy-opts opts)) ; Deprecated
        _ (assert (or (nil? headerless-meta)
                      (head-meta-id headerless-meta))
                  "Bad :headerless-meta (should be nil or a valid `head-meta` value)")

        ex (fn [msg & [e]] (throw (Exception. (str "Thaw failed: " msg) e)))
        try-thaw-data
        (fn [data-ba {:keys [compressed? encrypted?] :as _head-or-headerless-meta}]
          (let [password   (when encrypted?  password)
                compressor (when compressed? compressor)]
            (try
              (let [ba data-ba
                    ba (if password (encryption/decrypt encryptor password ba) ba)
                    ba (if compressor (compression/decompress compressor ba)   ba)
                    sin (DataInputStream. (ByteArrayInputStream. ba))]
                (thaw-from-in! sin))

              (catch Exception e
                (cond
                 password   (if head-meta (ex "Wrong password/encryptor?" e)
                                          (ex "Unencrypted data?" e))
                 compressor (if head-meta (ex "Encrypted data or wrong compressor?" e)
                                          (ex "Uncompressed data?" e))
                 :else      (if head-meta (ex "Corrupt data?" e)
                                          (ex "Data may be unfrozen, corrupt, compressed &/or encrypted.")))))))]

    (if-let [[data-ba {:keys [unrecognized-meta? compressed? encrypted?]
                       :as   head-meta}] (try-parse-header ba)]

      (cond ; A well-formed header _appears_ to be present
       (and (not headerless-meta) ; Cautious. It's unlikely but possible the
                                  ; header sig match was a fluke and not an
                                  ; indication of a real, well-formed header.
                                  ; May really be headerless.
            unrecognized-meta?)
       (ex "Unrecognized (but apparently well-formed) header. Data frozen with newer Nippy version?")

       ;;; It's still possible below that the header match was a fluke, but it's
       ;;; _very_ unlikely. Therefore _not_ going to incl.
       ;;; `(not headerless-meta)` conditions below.

       (and compressed? (not compressor))
       (ex "Compressed data? Try again with compressor.")
       (and encrypted? (not password))
       (if (::tools-thaw? opts) ::need-password
           (ex "Encrypted data? Try again with password."))
       :else (try (try-thaw-data data-ba head-meta)
                  (catch Exception e
                    (if headerless-meta
                      (try (try-thaw-data ba headerless-meta)
                           (catch Exception _
                             (throw e)))
                      (throw e)))))

      ;; Well-formed header definitely not present
      (if headerless-meta
        (try-thaw-data ba headerless-meta)
        (ex "Data may be unfrozen, corrupt, compressed &/or encrypted.")))))

(comment (thaw (freeze "hello"))
         (thaw (freeze "hello" {:compressor nil}))
         (thaw (freeze "hello" {:password [:salted "p"]})) ; ex
         (thaw (freeze "hello") {:password [:salted "p"]}))

;;;; Custom types

(defmacro extend-freeze
  "Alpha - subject to change.
  Extends Nippy to support freezing of a custom type (ideally concrete) with
  id ∈[1, 128]:
  (defrecord MyType [data])
  (extend-freeze MyType 1 [x data-output]
    (.writeUTF [data-output] (:data x)))"
  [type custom-type-id [x out] & body]
  (assert (and (>= custom-type-id 1) (<= custom-type-id 128)))
  `(extend-type ~type
     Freezable
     (~'freeze-to-out* [~x ~(with-meta out {:tag 'java.io.DataOutput})]
       (write-id ~out ~(int (- custom-type-id)))
       ~@body)))

(defonce custom-readers (atom {})) ; {<custom-type-id> (fn [data-input]) ...}
(defmacro extend-thaw
  "Alpha - subject to change.
  Extends Nippy to support thawing of a custom type with id ∈[1, 128]:
  (extend-thaw 1 [data-input]
    (->MyType (.readUTF data-input)))"
  [custom-type-id [in] & body]
  (assert (and (>= custom-type-id 1) (<= custom-type-id 128)))
  `(swap! custom-readers assoc ~(int (- custom-type-id))
          (fn [~(with-meta in {:tag 'java.io.DataInput})]
            ~@body)))

(comment (defrecord MyType [data])
         (extend-freeze MyType 1 [x out] (.writeUTF out (:data x)))
         (extend-thaw 1 [in] (->MyType (.readUTF in)))
         (thaw (freeze (->MyType "Joe"))))

;;; Some useful custom types - EXPERIMENTAL

(defrecord Compressable-LZMA2 [value])
(extend-freeze Compressable-LZMA2 128 [x out]
  (let [ba (freeze (:value x) {:skip-header? true :compressor nil})
        ba-len    (alength ba)
        compress? (> ba-len 1024)]
    (.writeBoolean out compress?)
    (if-not compress? (write-bytes out ba)
      (let [ba* (compression/compress compression/lzma2-compressor ba)]
        (write-bytes out ba*)))))

(extend-thaw 128 [in]
  (let [compressed? (.readBoolean in)
        ba          (read-bytes in)]
    (thaw ba {:compressor compression/lzma2-compressor
              :headerless-meta {:version     1
                                :compressed? compressed?
                                :encrypted?  false}})))

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

(encore/defalias freezable? utils/freezable?)

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

      {:known-wrapper  known-wrapper
       :nippy2-header  nippy-header ; Nippy v1.x didn't have a header
       :thawable?      (try (thaw unwrapped-ba thaw-opts) true
                            (catch Exception _ false))
       :unwrapped-ba   unwrapped-ba
       :data-ba        data-ba
       :unwrapped-size (alength ^bytes unwrapped-ba)
       :ba-size        (alength ^bytes ba)
       :data-size      (alength ^bytes data-ba)})))

(comment (inspect-ba (freeze "hello"))
         (seq (:data-ba (inspect-ba (freeze "hello")))))

;;;; Deprecated API

(def freeze-to-stream! "DEPRECATED: Use `freeze-to-out!` instead."
  freeze-to-out!)

(def thaw-from-stream! "DEPRECATED: Use `thaw-from-in!` instead."
  thaw-from-in!)

(defn- assert-legacy-args [compressor password]
  (when password
    (throw (AssertionError. "Encryption not supported in legacy mode.")))
  (when (and compressor (not= compressor snappy-compressor))
    (throw (AssertionError. "Only Snappy compressor supported in legacy mode."))))

(defn freeze-to-bytes "DEPRECATED: Use `freeze` instead."
  ^bytes [x & {:keys [compress?]
               :or   {compress? true}}]
  (freeze x {:skip-header? true
             :compressor   (when compress? snappy-compressor)
             :password     nil}))

(defn thaw-from-bytes "DEPRECATED: Use `thaw` instead."
  [ba & {:keys [compressed?]
         :or   {compressed? true}}]
  (thaw ba {:headerless-opts {:compressed? compressed?}
            :compressor      snappy-compressor
            :password        nil}))

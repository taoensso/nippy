(ns taoensso.nippy
  "Simple, high-performance Clojure serialization library. Originally adapted
  from Deep-Freeze."
  {:author "Peter Taoussanis"}
  (:require [taoensso.nippy
             (utils       :as utils)
             (compression :as compression :refer (snappy-compressor))
             (encryption  :as encryption  :refer (aes128-encryptor))])
  (:import  [java.io DataInputStream DataOutputStream]
            [com.taoensso FastByteArrayOutputStream FastByteArrayInputStream]
            [clojure.lang Keyword BigInt Ratio PersistentQueue PersistentTreeMap
             PersistentTreeSet IPersistentList IPersistentVector IPersistentMap
             IPersistentSet IPersistentCollection]))

;;;; Nippy 2.x+ header spec (4 bytes)
(def ^:private ^:const head-version 1)
(def ^:private head-sig (.getBytes "NPY" "UTF-8"))
(def ^:private head-meta "Final byte stores version-dependent metadata."
  {(byte 0) {:version 1 :compressed? false :encrypted? false}
   (byte 1) {:version 1 :compressed? true  :encrypted? false}
   (byte 2) {:version 1 :compressed? false :encrypted? true}
   (byte 3) {:version 1 :compressed? true  :encrypted? true}})

;;;; Data type IDs

;;                              1
(def ^:const id-bytes      (int 2))
(def ^:const id-nil        (int 3))
(def ^:const id-boolean    (int 4))
(def ^:const id-reader     (int 5)) ; Fallback: *print-dup* pr-str output

(def ^:const id-char       (int 10))
;;                              11
;;                              12
(def ^:const id-string     (int 13))
(def ^:const id-keyword    (int 14))

(def ^:const id-list       (int 20))
(def ^:const id-vector     (int 21))
;;                              22
(def ^:const id-set        (int 23))
(def ^:const id-coll       (int 24)) ; Fallback: non-specific collection
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

(def ^:const id-float      (int 60))
(def ^:const id-double     (int 61))
(def ^:const id-bigdec     (int 62))

(def ^:const id-ratio      (int 70))

;;; DEPRECATED (old types will be supported only for thawing)
(def ^:const id-old-reader  (int 1))  ; as of 0.9.2, for +64k support
(def ^:const id-old-string  (int 11)) ; as of 0.9.2, for +64k support
(def ^:const id-old-map     (int 22)) ; as of 0.9.0, for more efficient thaw
(def ^:const id-old-keyword (int 12)) ; as of 2.0.0-alpha5, for str consistecy

;;;; Freezing
(defprotocol Freezable (freeze-to-stream* [this stream]))

(defmacro ^:private write-id    [s id] `(.writeByte ~s ~id))
(defmacro ^:private write-bytes [s ba]
  `(let [s# ~s ba# ~ba]
     (let [size# (alength ba#)]
       (.writeInt s# size#)
       (.write s# ba# 0 size#))))

(defmacro ^:private write-biginteger [s x] `(write-bytes ~s (.toByteArray ~x)))
(defmacro ^:private write-utf8       [s x] `(write-bytes ~s (.getBytes ~x "UTF-8")))
(defmacro ^:private freeze-to-stream
  "Like `freeze-to-stream*` but with metadata support."
  [x s]
  `(let [x# ~x s# ~s]
     (if-let [m# (meta x#)]
       (do (write-id s# ~id-meta)
           (freeze-to-stream* m# s#)))
     (freeze-to-stream* x# s#)))

(defmacro ^:private freezer
  "Helper to extend Freezable protocol."
  [type id & body]
  `(extend-type ~type
     ~'Freezable
     (~'freeze-to-stream* [~'x ~(with-meta 's {:tag 'DataOutputStream})]
       (write-id ~'s ~id)
       ~@body)))

(defmacro ^:private coll-freezer
  "Extends Freezable to simple collection types."
  [type id & body]
  `(freezer ~type ~id
    (.writeInt ~'s (count ~'x))
    (doseq [i# ~'x] (freeze-to-stream i# ~'s))))

(defmacro ^:private kv-freezer
  "Extends Freezable to key-value collection types."
  [type id & body]
  `(freezer ~type ~id
    (.writeInt ~'s (* 2 (count ~'x)))
    (doseq [[k# v#] ~'x]
      (freeze-to-stream k# ~'s)
      (freeze-to-stream v# ~'s))))

(freezer (Class/forName "[B") id-bytes   (write-bytes s ^bytes x))
(freezer nil                  id-nil)
(freezer Boolean              id-boolean (.writeBoolean s x))

(freezer Character id-char    (.writeChar s (int x)))
(freezer String    id-string  (write-utf8 s x))
(freezer Keyword   id-keyword (write-utf8 s (if-let [ns (namespace x)]
                                              (str ns "/" (name x))
                                              (name x))))

(coll-freezer PersistentQueue       id-queue)
(coll-freezer PersistentTreeSet     id-sorted-set)
(kv-freezer   PersistentTreeMap     id-sorted-map)

(coll-freezer IPersistentList       id-list)
(coll-freezer IPersistentVector     id-vector)
(coll-freezer IPersistentSet        id-set)
(kv-freezer   IPersistentMap        id-map)
(coll-freezer IPersistentCollection id-coll) ; Must be LAST collection freezer!

(freezer Byte       id-byte    (.writeByte s x))
(freezer Short      id-short   (.writeShort s x))
(freezer Integer    id-integer (.writeInt s x))
(freezer Long       id-long    (.writeLong s x))
(freezer BigInt     id-bigint  (write-biginteger s (.toBigInteger x)))
(freezer BigInteger id-bigint  (write-biginteger s x))

(freezer Float      id-float   (.writeFloat s x))
(freezer Double     id-double  (.writeDouble s x))
(freezer BigDecimal id-bigdec
         (write-biginteger s (.unscaledValue x))
         (.writeInt s (.scale x)))

(freezer Ratio id-ratio
         (write-biginteger s (.numerator   x))
         (write-biginteger s (.denominator x)))

;; Use Clojure's own reader as final fallback
(freezer Object id-reader (write-bytes s (.getBytes (pr-str x) "UTF-8")))

(def ^:private head-meta-id (reduce-kv #(assoc %1 %3 %2) {} head-meta))

(defn- wrap-header [data-ba metadata]
  (if-let [meta-id (head-meta-id (assoc metadata :version head-version))]
    (let [head-ba (utils/ba-concat head-sig (byte-array [meta-id]))]
      (utils/ba-concat head-ba data-ba))
    (throw (Exception. (str "Unrecognized header metadata: " metadata)))))

(comment (wrap-header (.getBytes "foo") {:compressed? true
                                         :encrypted?  false}))

(declare assert-legacy-args)

(defn freeze
  "Serializes arg (any Clojure data type) to a byte array. Set :legacy-mode to
  true to produce bytes readble by Nippy < 2.x."
  ^bytes [x & [{:keys [print-dup? password compressor encryptor legacy-mode]
                :or   {print-dup? true
                       compressor snappy-compressor
                       encryptor  aes128-encryptor}}]]
  (when legacy-mode (assert-legacy-args compressor password))
  (let [ba     (FastByteArrayOutputStream.)
        stream (DataOutputStream. ba)]
    (binding [*print-dup* print-dup?] (freeze-to-stream x stream))
    (let [ba (.toByteArray ba)
          ba (if compressor (compression/compress compressor ba) ba)
          ba (if password   (encryption/encrypt encryptor password ba) ba)]
      (if legacy-mode ba
        (wrap-header ba {:compressed? (boolean compressor)
                         :encrypted?  (boolean password)})))))

;;;; Thawing

(declare thaw-from-stream)

(defmacro ^:private read-bytes [s]
  `(let [s# ~s
         size# (.readInt s#)
         ba#   (byte-array size#)]
     (.read s# ba# 0 size#) ba#))

(defmacro ^:private read-biginteger [s] `(BigInteger. (read-bytes ~s)))
(defmacro ^:private read-utf8       [s] `(String. (read-bytes ~s) "UTF-8"))

(defmacro ^:private coll-thaw "Thaws simple collection types."
  [s coll]
  `(let [s# ~s]
     (utils/repeatedly-into ~coll (.readInt s#) (thaw-from-stream s#))))

(defmacro ^:private coll-thaw-kvs "Thaws key-value collection types."
  [s coll]
  `(let [s# ~s]
     (utils/repeatedly-into ~coll (/ (.readInt s#) 2)
       [(thaw-from-stream s#) (thaw-from-stream s#)])))

(defn- thaw-from-stream
  [^DataInputStream s]
  (let [type-id (.readByte s)]
    (utils/case-eval
     type-id

     id-reader  (read-string (read-utf8 s))
     id-bytes   (read-bytes s)
     id-nil     nil
     id-boolean (.readBoolean s)

     id-char    (.readChar s)
     id-string  (read-utf8 s)
     id-keyword (keyword (read-utf8 s))

     id-queue      (coll-thaw s (PersistentQueue/EMPTY))
     id-sorted-set (coll-thaw s     (sorted-set))
     id-sorted-map (coll-thaw-kvs s (sorted-map))

     id-list    (into '() (rseq (coll-thaw s [])))
     id-vector  (coll-thaw s  [])
     id-set     (coll-thaw s #{})
     id-map     (coll-thaw-kvs s  {})
     id-coll    (seq (coll-thaw s []))

     id-meta (let [m (thaw-from-stream s)] (with-meta (thaw-from-stream s) m))

     id-byte    (.readByte s)
     id-short   (.readShort s)
     id-integer (.readInt s)
     id-long    (.readLong s)
     id-bigint  (bigint (read-biginteger s))

     id-float  (.readFloat s)
     id-double (.readDouble s)
     id-bigdec (BigDecimal. (read-biginteger s) (.readInt s))

     id-ratio (/ (bigint (read-biginteger s))
                 (bigint (read-biginteger s)))

     ;;; DEPRECATED
     id-old-reader (read-string (.readUTF s))
     id-old-string (.readUTF s)
     id-old-map    (apply hash-map (utils/repeatedly-into []
                     (* 2 (.readInt s)) (thaw-from-stream s)))
     id-old-keyword (keyword (.readUTF s))

     (throw (Exception. (str "Failed to thaw unknown type ID: " type-id))))))

(defn- try-parse-header [ba]
  (when-let [[head-ba data-ba] (utils/ba-split ba 4)]
    (let [[head-sig* [meta-id]] (utils/ba-split head-ba 3)]
      (when (utils/ba= head-sig* head-sig)
        [data-ba (head-meta meta-id {:unrecognized-header? true})]))))

(defn thaw
  "Deserializes frozen bytes to their original Clojure data type. Supports data
  frozen with current and all previous versions of Nippy.

  WARNING: Enabling `:read-eval?` can lead to security vulnerabilities unless
  you are sure you know what you're doing."
  [^bytes ba & [{:keys [read-eval? password compressor encryptor legacy-opts]
                 :or   {legacy-opts {:compressed? true}
                        compressor  snappy-compressor
                        encryptor   aes128-encryptor}}]]

  (let [ex (fn [msg & [e]] (throw (Exception. (str "Thaw failed: " msg) e)))
        try-thaw-data
        (fn [data-ba {:keys [compressed? encrypted?] :as head-meta}]
          (let [password   (when encrypted? password) ; => also head-meta
                compressor (if head-meta
                             (when compressed? compressor)
                             (when (:compressed? legacy-opts) snappy-compressor))]
            (try
              (let [ba data-ba
                    ba (if password (encryption/decrypt encryptor password ba) ba)
                    ba (if compressor (compression/decompress compressor ba) ba)
                    stream (DataInputStream. (FastByteArrayInputStream. ba))]
                (binding [*read-eval* read-eval?] (thaw-from-stream stream)))
              (catch Exception e
                (cond
                 password   (ex "Wrong password/encryptor?" e)
                 compressor (if head-meta (ex "Encrypted data or wrong compressor?" e)
                                          (ex "Uncompressed data?" e))
                 :else      (if head-meta (ex "Corrupt data?" e)
                                          (ex "Compressed data?" e)))))))]

    (if-let [[data-ba {:keys [unrecognized-header? compressed? encrypted?]
                       :as   head-meta}] (try-parse-header ba)]

      (cond ; Header appears okay
       (and (not legacy-opts) unrecognized-header?) ; Conservative
       (ex "Unrecognized header. Data frozen with newer Nippy version?")
       (and compressed? (not compressor))
       (ex "Compressed data. Try again with compressor.")
       (and encrypted? (not password))
       (ex "Encrypted data. Try again with password.")
       :else (try (try-thaw-data data-ba head-meta)
                  (catch Exception _ (try-thaw-data ba nil))))

      ;; Header definitely not okay
      (try-thaw-data ba nil))))

(comment (thaw (freeze "hello"))
         (thaw (freeze "hello" {:compressor nil}))
         (thaw (freeze "hello" {:password [:salted "p"]})) ; ex
         (thaw (freeze "hello") {:password [:salted "p"]}))

;;;; Stress data

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

     :coll         (repeatedly 1000 rand)

     :byte         (byte 16)
     :short        (short 42)
     :integer      (int 3)
     :long         (long 3)
     :bigint       (bigint 31415926535897932384626433832795)

     :float        (float 3.14)
     :double       (double 3.14)
     :bigdec       (bigdec 3.1415926535897932384626433832795)

     :ratio        22/7

     ;; Clojure 1.4+ tagged literals
     :tagged-uuid  (java.util.UUID/randomUUID)
     :tagged-date  (java.util.Date.)}))

;;;; Deprecated API

(defn- assert-legacy-args [compressor password]
  (when password
    (throw (AssertionError. "Encryption not supported in legacy mode.")))
  (when (and compressor (not= compressor snappy-compressor))
    (throw (AssertionError. "Only Snappy compressor supported in legacy mode."))))

(defn freeze-to-bytes "DEPRECATED: Use `freeze` instead."
  ^bytes [x & {:keys [print-dup? compress?]
               :or   {print-dup? true
                      compress?  true}}]
  (freeze x {:legacy-mode  true
             :print-dup?   print-dup?
             :compressor   (when compress? snappy-compressor)
             :password     nil}))

(defn thaw-from-bytes "DEPRECATED: Use `thaw` instead."
  [ba & {:keys [read-eval? compressed?]
         :or   {compressed? true}}]
  (thaw ba {:legacy-opts  {:compressed? compressed?}
            :read-eval?   read-eval?
            :password     nil}))

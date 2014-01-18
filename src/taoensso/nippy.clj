(ns taoensso.nippy
  "Simple, high-performance Clojure serialization library. Originally adapted
  from Deep-Freeze."
  {:author "Peter Taoussanis"}
  (:require [clojure.tools.reader.edn :as edn]
            [taoensso.nippy
             (utils       :as utils)
             (compression :as compression :refer (snappy-compressor))
             (encryption  :as encryption  :refer (aes128-encryptor))])
  (:import  [java.io ByteArrayInputStream ByteArrayOutputStream DataInputStream
             DataOutputStream Serializable ObjectOutputStream ObjectInputStream DataOutput DataInput]
            [java.lang.reflect Method]
            [java.util Date UUID]
            [clojure.lang Keyword BigInt Ratio
             APersistentMap APersistentVector APersistentSet
             IPersistentMap ; IPersistentVector IPersistentSet IPersistentList
             PersistentQueue PersistentTreeMap PersistentTreeSet PersistentList ; LazySeq
             IRecord ISeq]
            [java.math BigDecimal BigInteger]))

;;;; Nippy 2.x+ header spec (4 bytes)
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

(def ^:const id-float      (int 60))
(def ^:const id-double     (int 61))
(def ^:const id-bigdec     (int 62))

(def ^:const id-ratio      (int 70))

(def ^:const id-record     (int 80))
;; (def ^:const id-type    (int 81)) ; TODO

(def ^:const id-date       (int 90))
(def ^:const id-uuid       (int 91))

;;; DEPRECATED (old types will be supported only for thawing)
(def ^:const id-old-reader  (int 1))  ; as of 0.9.2, for +64k support
(def ^:const id-old-string  (int 11)) ; as of 0.9.2, for +64k support
(def ^:const id-old-map     (int 22)) ; as of 0.9.0, for more efficient thaw
(def ^:const id-old-keyword (int 12)) ; as of 2.0.0-alpha5, for str consistecy

;;;; Freezing

(defprotocol Freezable
  "Be careful about extending to interfaces, Ref. http://goo.gl/6gGRlU."
 (freeze-to-stream* [this stream]))

(defmacro           write-id    [s id] `(.writeByte ~s ~id))
(defmacro ^:private write-bytes [s ba]
  `(let [s# ~s ba# ~ba]
     (let [size# (alength ba#)]
       (.writeInt s# size#)
       (.write s# ba# 0 size#))))

(defmacro ^:private write-biginteger [s x] `(write-bytes ~s (.toByteArray ~x)))
(defmacro ^:private write-utf8       [s x] `(write-bytes ~s (.getBytes ~x "UTF-8")))
(defmacro ^:private freeze-to-stream
  "Like `freeze-to-stream*` but with metadata support."
  [s x]
  `(let [x# ~x s# ~s]
     (when-let [m# (meta x#)]
       (write-id  s# ~id-meta)
       (freeze-to-stream* m# s#))
     (freeze-to-stream* x# s#)))

(defmacro ^:private freezer [type id & body]
  `(extend-type ~type
     Freezable
     (~'freeze-to-stream* [~'x ~(with-meta 's {:tag 'DataOutput})]
       (write-id ~'s ~id)
       ~@body)))

(defmacro ^:private freezer-coll [type id & body]
  `(freezer ~type ~id
     (when-debug-mode
      (when (instance? ISeq ~type)
        (println (format "DEBUG - freezer-coll: %s for %s" ~type (type ~'x)))))
     (if (counted? ~'x)
       (do (.writeInt ~'s (count ~'x))
           (doseq [i# ~'x] (freeze-to-stream ~'s i#)))
       (let [bas# (ByteArrayOutputStream.)
             s#   (DataOutputStream. bas#)
             cnt# (reduce (fn [cnt# i#]
                            (freeze-to-stream s# i#)
                            (unchecked-inc cnt#))
                          0 ~'x)
             ba# (.toByteArray bas#)]
         (.writeInt ~'s cnt#)
         (.write ~'s ba# 0 (alength ba#))))))

(defmacro ^:private freezer-kvs [type id & body]
  `(freezer ~type ~id
    (.writeInt ~'s (* 2 (count ~'x)))
    (doseq [kv# ~'x]
      (freeze-to-stream ~'s (key kv#))
      (freeze-to-stream ~'s (val kv#)))))

(freezer (Class/forName "[B") id-bytes   (write-bytes s ^bytes x))
(freezer nil                  id-nil)
(freezer Boolean              id-boolean (.writeBoolean s x))

(freezer Character id-char    (.writeChar s (int x)))
(freezer String    id-string  (write-utf8 s x))
(freezer Keyword   id-keyword (write-utf8 s (if-let [ns (namespace x)]
                                              (str ns "/" (name x))
                                              (name x))))

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
         (write-utf8 s (.getName (class x))) ; Reflect
         (freeze-to-stream s (into {} x)))

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

(freezer Date id-date (.writeLong s (.getTime x)))
(freezer UUID id-uuid
         (.writeLong s (.getMostSignificantBits x))
         (.writeLong s (.getLeastSignificantBits x)))

(def ^:dynamic *final-freeze-fallback* "Alpha - subject to change." nil)
(defn freeze-fallback-as-str "Alpha-subject to change." [x s]
  (freeze-to-stream* {:nippy/unfreezable (pr-str x) :type (type x)} s))

(comment
  (require '[clojure.core.async :as async])
  (binding [*final-freeze-fallback* freeze-fallback-as-str]
    (-> (async/chan) (freeze) (thaw))))

;; Fallbacks. Note that we'll extend *only* to (lowly) Object to prevent
;; interfering with higher-level implementations, Ref. http://goo.gl/6f7SKl
(extend-type Object
  Freezable
  (freeze-to-stream* [x ^DataOutput s]
    (cond
     (utils/serializable? x) ; Fallback #1: Java's Serializable interface
     (do (when-debug-mode
          (println (format "DEBUG - Serializable fallback: %s" (type x))))
         (write-id s id-serializable)
         (write-utf8 s (.getName (class x))) ; Reflect
         (.writeObject (ObjectOutputStream. s) x))

     (utils/readable? x) ; Fallback #2: Clojure's Reader
     (do (when-debug-mode
          (println (format "DEBUG - Reader fallback: %s" (type x))))
         (write-id s id-reader)
         (write-bytes s (.getBytes (pr-str x) "UTF-8")))

     :else ; Fallback #3: *final-freeze-fallback*
     (if-let [ffb *final-freeze-fallback*] (ffb x s)
       (throw (Exception. (format "Unfreezable type: %s %s"
                                  (type x) (str x))))))))

(def ^:private head-meta-id (reduce-kv #(assoc %1 %3 %2) {} head-meta))

(defn- wrap-header [data-ba metadata]
  (if-let [meta-id (head-meta-id (assoc metadata :version head-version))]
    (let [head-ba (utils/ba-concat head-sig (byte-array [meta-id]))]
      (utils/ba-concat head-ba data-ba))
    (throw (Exception. (str "Unrecognized header metadata: " metadata)))))

(comment (wrap-header (.getBytes "foo") {:compressed? true
                                         :encrypted?  false}))

(declare assert-legacy-args)

(defn freeze-to-stream!
  "Low-level API. Serializes arg (any Clojure data type) to a DataOutput."
  [^DataOutput data-output x & _]
  (freeze-to-stream data-output x))

(defn freeze
  "Serializes arg (any Clojure data type) to a byte array. Set :legacy-mode to
  true to produce bytes readable by Nippy < 2.x. For custom types extend the
  Clojure reader or see `extend-freeze`."
  ^bytes [x & [{:keys [password compressor encryptor legacy-mode]
                :or   {compressor snappy-compressor
                       encryptor  aes128-encryptor}}]]
  (when legacy-mode (assert-legacy-args compressor password))
  (let [bas (ByteArrayOutputStream.)
        ds  (DataOutputStream. bas)]
    (freeze-to-stream! ds x)
    (let [ba (.toByteArray bas)
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
     (.readFully s# ba#) ba#))

(defmacro ^:private read-biginteger [s] `(BigInteger. (read-bytes ~s)))
(defmacro ^:private read-utf8       [s] `(String. (read-bytes ~s) "UTF-8"))

(defmacro ^:private read-coll [s coll]
  `(let [s# ~s] (utils/repeatedly-into ~coll (.readInt s#) (thaw-from-stream s#))))

(defmacro ^:private read-kvs [s coll]
  `(let [s# ~s] (utils/repeatedly-into ~coll (/ (.readInt s#) 2)
                  [(thaw-from-stream s#) (thaw-from-stream s#)])))

(declare ^:private custom-readers)

(defn- thaw-from-stream
  [^DataInput s]
  (let [type-id (.readByte s)]
    (try
      (when-debug-mode
       (println (format "DEBUG - thawing type-id: %s" type-id)))

      (utils/case-eval type-id

        id-reader
        (let [edn (read-utf8 s)]
          (try (edn/read-string {:readers *data-readers*} edn)
               (catch Exception _ {:nippy/unthawable edn
                                   :type :reader})))

        id-serializable
        (let [class-name (read-utf8 s)]
          (try (let [;; .readObject _before_ Class/forName: it'll always read
                     ;; all data before throwing
                     object (.readObject (ObjectInputStream. s))
                     class ^Class (Class/forName class-name)]
                 (cast class object))
               (catch Exception _ {:nippy/unthawable class-name
                                   :type :serializable})))

        id-bytes   (read-bytes s)
        id-nil     nil
        id-boolean (.readBoolean s)

        id-char    (.readChar s)
        id-string  (read-utf8 s)
        id-keyword (keyword (read-utf8 s))

        id-queue      (read-coll s (PersistentQueue/EMPTY))
        id-sorted-set (read-coll s (sorted-set))
        id-sorted-map (read-kvs  s (sorted-map))

        id-list    (into '() (rseq (read-coll s [])))
        id-vector  (read-coll s  [])
        id-set     (read-coll s #{})
        id-map     (read-kvs  s  {})
        id-seq     (seq (read-coll s []))

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

        id-record
        (let [class    ^Class (Class/forName (read-utf8 s))
              meth-sig (into-array Class [IPersistentMap])
              method   ^Method (.getMethod class "create" meth-sig)]
          (.invoke method class (into-array Object [(thaw-from-stream s)])))

        id-date  (Date. (.readLong s))
        id-uuid  (UUID. (.readLong s) (.readLong s))

        ;;; DEPRECATED
        id-old-reader (edn/read-string (.readUTF s))
        id-old-string (.readUTF s)
        id-old-map    (apply hash-map (utils/repeatedly-into []
                        (* 2 (.readInt s)) (thaw-from-stream s)))
        id-old-keyword (keyword (.readUTF s))

        (if-not (neg? type-id)
          (throw (Exception. (str "Unknown type ID: " type-id)))

          ;; Custom types
          (if-let [reader (get @custom-readers type-id)]
            (try (reader s)
                 (catch Exception e
                   (throw (Exception. (str "Reader exception for custom type ID: "
                                           (- type-id)) e))))
            (throw (Exception. (str "No reader provided for custom type ID: "
                                    (- type-id)))))))

      (catch Exception e
        (throw (Exception. (format "Thaw failed against type-id: %s" type-id) e))))))

(defn thaw-from-stream!
  "Low-level API. Deserializes a frozen object from given DataInput to its
  original Clojure data type."
  [data-input & _]
  (thaw-from-stream data-input))

(defn- try-parse-header [ba]
  (when-let [[head-ba data-ba] (utils/ba-split ba 4)]
    (let [[head-sig* [meta-id]] (utils/ba-split head-ba 3)]
      (when (utils/ba= head-sig* head-sig)
        [data-ba (head-meta meta-id {:unrecognized-header? true})]))))

(defn thaw
  "Deserializes a frozen object from given byte array to its original Clojure
  data type. Supports data frozen with current and all previous versions of
  Nippy. For custom types extend the Clojure reader or see `extend-thaw`."
  [^bytes ba & [{:keys [password compressor encryptor legacy-opts]
                 :or   {legacy-opts {:compressed? true}
                        compressor  snappy-compressor
                        encryptor   aes128-encryptor}
                 :as   opts}]]

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
                    stream (DataInputStream. (ByteArrayInputStream. ba))]

                (thaw-from-stream! stream))

              (catch Exception e
                (cond
                 password   (ex "Wrong password/encryptor?" e)
                 compressor (if head-meta (ex "Encrypted data or wrong compressor?" e)
                                          (ex "Uncompressed data?" e))
                 :else      (if head-meta (ex "Corrupt data?" e)
                                          (ex "Compressed data?" e)))))))]

    (if-let [[data-ba {:keys [unrecognized-header? compressed? encrypted?]
                       :as   head-meta}] (try-parse-header ba)]

      (cond ; Header _appears_ okay
       (and (not legacy-opts) unrecognized-header?) ; Conservative
       (ex "Unrecognized header. Data frozen with newer Nippy version?")
       (and compressed? (not compressor))
       (ex "Compressed data. Try again with compressor.")
       (and encrypted? (not password))
       (if (::tools-thaw? opts) ::need-password
           (ex "Encrypted data. Try again with password."))
       :else (try (try-thaw-data data-ba head-meta)
                  (catch Exception e
                    (if legacy-opts
                      (try (try-thaw-data ba nil)
                           (catch Exception _
                             (throw e)))
                      (throw e)))))

      ;; Header definitely not okay
      (if legacy-opts
        (try-thaw-data ba nil)
        (ex "Unfrozen or corrupt data?")))))

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
  (extend-freeze MyType 1 [x data-output-stream]
    (.writeUTF [data-output-stream] (:data x)))"
  [type custom-type-id [x stream] & body]
  (assert (and (>= custom-type-id 1) (<= custom-type-id 128)))
  `(extend-type ~type
     Freezable
     (~'freeze-to-stream* [~x ~(with-meta stream {:tag 'java.io.DataOutput})]
       (write-id ~stream ~(int (- custom-type-id)))
       ~@body)))

(defonce custom-readers (atom {})) ; {<custom-type-id> (fn [data-input-stream]) ...}
(defmacro extend-thaw
  "Alpha - subject to change.
  Extends Nippy to support thawing of a custom type with id ∈[1, 128]:
  (extend-thaw 1 [data-input-stream]
    (->MyType (.readUTF data-input-stream)))"
  [custom-type-id [stream] & body]
  (assert (and (>= custom-type-id 1) (<= custom-type-id 128)))
  `(swap! custom-readers assoc ~(int (- custom-type-id))
          (fn [~(with-meta stream {:tag 'java.io.DataInput})]
            ~@body)))

(comment (defrecord MyType [data])
         (extend-freeze MyType 1 [x s] (.writeUTF s (:data x)))
         (extend-thaw 1 [s] (->MyType (.readUTF s)))
         (thaw (freeze (->MyType "Joe"))))

;;; Some useful custom types - EXPERIMENTAL

(defrecord Compressable-LZMA2 [value])
(extend-freeze Compressable-LZMA2 128 [x st]
  (let [[_ ^bytes ba] (-> (freeze (:value x) {:compressor nil})
                          (utils/ba-split 4))
        ba-len    (alength ba)
        compress? (> ba-len 1024)]
    (.writeBoolean st compress?)
    (if-not compress?
      (do (.writeLong st  ba-len)
          (.write st ba 0 ba-len))
      (let [ba*     (compression/compress compression/lzma2-compressor ba)
            ba*-len (alength ba*)]
        (.writeLong st   ba*-len)
        (.write st ba* 0 ba*-len)))))

(extend-thaw 128 [st]
  (let [compressed? (.readBoolean st)
        ba-len      (.readLong    st)
        ba          (byte-array ba-len)]
    (.readFully st ba)
    (thaw (wrap-header ba {:compressed? compressed? :encrypted? false})
          {:compressor compression/lzma2-compressor})))

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

;;;; Data recovery/analysis

(defn inspect-ba "Alpha - subject to change."
  [ba & [thaw-opts]]
  (if-not (utils/bytes? ba) :not-ba
    (let [[first2bytes nextbytes] (utils/ba-split ba 2)
          known-wrapper
          (cond
           (utils/ba= first2bytes (.getBytes "\u0000<" "UTF8")) :carmine/bin
           (utils/ba= first2bytes (.getBytes "\u0000>" "UTF8")) :carmine/clj)

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

(defn- assert-legacy-args [compressor password]
  (when password
    (throw (AssertionError. "Encryption not supported in legacy mode.")))
  (when (and compressor (not= compressor snappy-compressor))
    (throw (AssertionError. "Only Snappy compressor supported in legacy mode."))))

(defn freeze-to-bytes "DEPRECATED: Use `freeze` instead."
  ^bytes [x & {:keys [compress?]
               :or   {compress? true}}]
  (freeze x {:legacy-mode  true
             :compressor   (when compress? snappy-compressor)
             :password     nil}))

(defn thaw-from-bytes "DEPRECATED: Use `thaw` instead."
  [ba & {:keys [compressed?]
         :or   {compressed? true}}]
  (thaw ba {:legacy-opts  {:compressed? compressed?}
            :password     nil}))

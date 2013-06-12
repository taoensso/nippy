(ns taoensso.nippy
  "Simple, high-performance Clojure serialization library. Adapted from
  Deep-Freeze."
  {:author "Peter Taoussanis"}
  (:require [taoensso.nippy.utils  :as utils]
            [taoensso.nippy.crypto :as crypto])
  (:import  [java.io DataInputStream DataOutputStream ByteArrayOutputStream
             ByteArrayInputStream]
            [clojure.lang Keyword BigInt Ratio PersistentQueue PersistentTreeMap
             PersistentTreeSet IPersistentList IPersistentVector IPersistentMap
             IPersistentSet IPersistentCollection]))

;;;; Define type IDs

;;                              1
(def ^:const id-bytes      (int 2))
(def ^:const id-nil        (int 3))
(def ^:const id-boolean    (int 4))
(def ^:const id-reader     (int 5)) ; Fallback: *print-dup* pr-str output

(def ^:const id-char       (int 10))
;;                              11
(def ^:const id-keyword    (int 12))
(def ^:const id-string     (int 13))

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
(def ^:const id-old-reader (int 1))  ; as of 0.9.2, for +64k support
(def ^:const id-old-string (int 11)) ; as of 0.9.2, for +64k support
(def ^:const id-old-map    (int 22)) ; as of 0.9.0, for more efficient thaw

;;;; Shared low-level stream stuff

(defn- write-id! [^DataOutputStream stream ^Integer id] (.writeByte stream id))

(defn- write-bytes!
  "Writes arbitrary byte data, preceded by its length."
  [^DataOutputStream stream ^bytes ba]
  (let [size (alength ba)]
    (.writeInt stream size) ; Encode size of byte array
    (.write stream ba 0 size)))

(defn- write-biginteger!
  "Wrapper around `write-bytes!` for common case of writing a BigInteger."
  [^DataOutputStream stream ^BigInteger x]
  (write-bytes! stream (.toByteArray x)))

(defn- read-bytes!
  "Reads arbitrary byte data, preceded by its length."
  ^bytes [^DataInputStream stream]
  (let [size (.readInt stream)
        ba   (byte-array size)]
    (.read stream ba 0 size) ba))

(defn- read-biginteger!
  "Wrapper around `read-bytes!` for common case of reading a BigInteger.
  Note that as of Clojure 1.3, java.math.BigInteger ≠ clojure.lang.BigInt."
  ^BigInteger [^DataInputStream stream]
  (BigInteger. (read-bytes! stream)))

;;;; Freezing

(defprotocol Freezable (freeze [this stream]))

(defmacro freezer
  "Helper to extend Freezable protocol."
  [type id & body]
  `(extend-type ~type
     ~'Freezable
     (~'freeze [~'x ~(with-meta 's {:tag 'DataOutputStream})]
       (write-id! ~'s ~id)
       ~@body)))

(defmacro coll-freezer
  "Extends Freezable to simple collection types."
  [type id & body]
  `(freezer ~type ~id
    (.writeInt ~'s (count ~'x)) ; Encode collection length
    (doseq [i# ~'x] (freeze-to-stream!* ~'s i#))))

(defmacro kv-freezer
  "Extends Freezable to key-value collection types."
  [type id & body]
  `(freezer ~type ~id
    (.writeInt ~'s (* 2 (count ~'x))) ; Encode num kvs
    (doseq [[k# v#] ~'x]
      (freeze-to-stream!* ~'s k#)
      (freeze-to-stream!* ~'s v#))))

(freezer (Class/forName "[B") id-bytes   (write-bytes! s x))
(freezer nil                  id-nil)
(freezer Boolean              id-boolean (.writeBoolean s x))

(freezer Character id-char    (.writeChar s (int x)))
(freezer String    id-string  (write-bytes! s (.getBytes x "UTF-8")))
(freezer Keyword   id-keyword (.writeUTF s (if-let [ns (namespace x)]
                                             (str ns "/" (name x))
                                             (name x))))

(declare freeze-to-stream!*)

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
(freezer BigInt     id-bigint  (write-biginteger! s (.toBigInteger x)))
(freezer BigInteger id-bigint  (write-biginteger! s x))

(freezer Float      id-float   (.writeFloat s x))
(freezer Double     id-double  (.writeDouble s x))
(freezer BigDecimal id-bigdec
         (write-biginteger! s (.unscaledValue x))
         (.writeInt s (.scale x)))

(freezer Ratio id-ratio
         (write-biginteger! s (.numerator   x))
         (write-biginteger! s (.denominator x)))

;; Use Clojure's own reader as final fallback
(freezer Object id-reader (write-bytes! s (.getBytes (pr-str x) "UTF-8")))

(defn- freeze-to-stream!* [^DataOutputStream s x]
  (if-let [m (meta x)]
    (do (write-id! s id-meta)
        (freeze-to-stream!* s m)))
  (freeze x s))

(defn freeze-to-stream!
  "Serializes x to given output stream."
  ([data-output-stream x] ; For <= 1.0.1 compatibility
     (freeze-to-stream! data-output-stream x true))
  ([data-output-stream x print-dup?]
     (binding [*print-dup* print-dup?] ; For `pr-str`
       (freeze-to-stream!* data-output-stream x))))

(defn freeze-to-bytes
  "Serializes x to a byte array and returns the array."
  ^bytes [x & {:keys [compress? print-dup? password]
               :or   {compress?  true
                      print-dup? true}}]
  (let [ba     (ByteArrayOutputStream.)
        stream (DataOutputStream. ba)]
    (freeze-to-stream! stream x print-dup?)
    (let [ba (.toByteArray ba)
          ba (if compress? (utils/compress-snappy ba) ba)
          ba (if password  (crypto/encrypt-aes128 password ba) ba)]
      ba)))

;;;; Thawing

(declare thaw-from-stream!*)

(defn coll-thaw!
  "Thaws simple collection types."
  [^DataInputStream s]
  (repeatedly (.readInt s) #(thaw-from-stream!* s)))

(defn coll-thaw-kvs!
  "Thaws key-value collection types."
  [^DataInputStream s]
  (repeatedly (/ (.readInt s) 2)
              (fn [] [(thaw-from-stream!* s) (thaw-from-stream!* s)])))

(defn- thaw-from-stream!*
  [^DataInputStream s]
  (let [type-id (.readByte s)]
    (utils/case-eval
     type-id

     id-reader  (read-string (String. (read-bytes! s) "UTF-8"))
     id-bytes   (read-bytes! s)
     id-nil     nil
     id-boolean (.readBoolean s)

     id-char    (.readChar s)
     id-string  (String. (read-bytes! s) "UTF-8")
     id-keyword (keyword (.readUTF s))

     id-queue      (into (PersistentQueue/EMPTY) (coll-thaw! s))
     id-sorted-set (into (sorted-set) (coll-thaw! s))
     id-sorted-map (into (sorted-map) (coll-thaw-kvs! s))

     id-list    (into '() (reverse (coll-thaw! s)))
     id-vector  (into  [] (coll-thaw! s))
     id-set     (into #{} (coll-thaw! s))
     id-map     (into  {} (coll-thaw-kvs! s))
     id-coll    (doall (coll-thaw! s))

     id-meta (let [m (thaw-from-stream!* s)] (with-meta (thaw-from-stream!* s) m))

     id-byte    (.readByte s)
     id-short   (.readShort s)
     id-integer (.readInt s)
     id-long    (.readLong s)
     id-bigint  (bigint (read-biginteger! s))

     id-float  (.readFloat s)
     id-double (.readDouble s)
     id-bigdec (BigDecimal. (read-biginteger! s) (.readInt s))

     id-ratio (/ (bigint (read-biginteger! s))
                 (bigint (read-biginteger! s)))

     ;;; DEPRECATED
     id-old-reader (read-string (.readUTF s))
     id-old-string (.readUTF s)
     id-old-map    (apply hash-map (repeatedly (* 2 (.readInt s))
                                               #(thaw-from-stream!* s)))

     (throw (Exception. (str "Failed to thaw unknown type ID: " type-id))))))

(defn thaw-from-stream!
  "Deserializes an object from given input stream."
  [data-input-stream read-eval?]
  (binding [*read-eval* read-eval?]
    (let [;; Support older versions of Nippy that wrote a version header
          maybe-schema-header (thaw-from-stream!* data-input-stream)]
      (if (and (string? maybe-schema-header)
               (.startsWith ^String maybe-schema-header "\u0000~"))
        (thaw-from-stream!* data-input-stream)
        maybe-schema-header))))

(defn thaw-from-bytes
  "Deserializes an object from given byte array."
  [ba & {:keys [compressed? read-eval? password]
         :or   {compressed? true
                read-eval?  false ; For `read-string` injection safety - NB!!!
                }}]
  (try
    (-> (let [ba (if password    (crypto/decrypt-aes128 password ba) ba)
              ba (if compressed? (utils/uncompress-snappy ba) ba)]
          ba)
        (ByteArrayInputStream.)
        (DataInputStream.)
        (thaw-from-stream! read-eval?))
    (catch Exception e
      (throw (Exception.
              (cond password    "Thaw failed. Unencrypted data or bad password?"
                    compressed? "Thaw failed. Encrypted or uncompressed data?"
                    :else       "Thaw failed. Encrypted and/or compressed data?")
              e)))))

(comment
  (-> (freeze-to-bytes "my data" :password [:salted "password"])
      (thaw-from-bytes))
  (-> (freeze-to-bytes "my data" :compress? true)
      (thaw-from-bytes :compressed? false)))

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
(ns taoensso.nippy
  "High-performance serialization library for Clojure"
  {:author "Peter Taoussanis (@ptaoussanis)"}
  (:require
   [clojure.java.io :as jio]
   [taoensso.encore :as enc :refer [cond*]]
   [taoensso.nippy
    [utils       :as utils]
    [compression :as compression]
    [encryption  :as encryption]])

  (:import
   [java.io ByteArrayInputStream ByteArrayOutputStream DataInputStream
    DataOutputStream Serializable ObjectOutputStream ObjectInputStream
    DataOutput DataInput]
   [java.lang.reflect Method Field Constructor]
   [java.net URI]
   [java.util Date UUID]
   [java.util.regex Pattern]
   [clojure.lang Keyword Symbol BigInt Ratio
    APersistentMap APersistentVector APersistentSet
    IPersistentMap ; IPersistentVector IPersistentSet IPersistentList
    PersistentQueue PersistentTreeMap PersistentTreeSet PersistentList
    LazySeq IRecord ISeq IType]))

(if (vector? enc/encore-version)
  (enc/assert-min-encore-version [2 67 1])
  (enc/assert-min-encore-version  2.67))

(comment
  (set! *unchecked-math* :warn-on-boxed)
  (set! *unchecked-math* false)
  (thaw (freeze stress-data)))

;;;; TODO
;; - Performance would benefit from ^:static support / direct linking / etc.
;; - Ability to compile out metadata support?
;; - Auto cache keywords? When map keys? Configurable? Per-map
;;   (`cache-keys`)? Just rely on compression?

;;;; Nippy data format
;; * 4-byte header (Nippy v2.x+) (may be disabled but incl. by default) [1]
;; { * 1-byte type id
;;   * Arb-length payload determined by freezer for this type [2] } ...
;;
;; [1] Inclusion of header is *strongly* recommended. Purpose:
;;   * Sanity check (confirm that data appears to be Nippy data)
;;   * Nippy version check (=> supports changes to data schema over time)
;;   * Supports :auto thaw compressor, encryptor
;;   * Supports :auto freeze compressor (since this depends on :auto thaw
;;     compressor)
;;
;; [2] See `IFreezable1` protocol for type-specific payload formats,
;;     `thaw-from-in!` for reference type-specific thaw implementations
;;
(def ^:private ^:const charset "UTF-8")
(def ^:private head-sig "First 3 bytes of Nippy header" (.getBytes "NPY" charset))
(def ^:private ^:const head-version "Current Nippy header format version" 1)
(def ^:private ^:const head-meta
  "Final byte of 4-byte Nippy header stores version-dependent metadata"
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

(defmacro ^:private when-debug [& body] (when #_true false `(do ~@body)))

(def ^:private type-ids
  "{<byte-id> <type-name-kw>}, ~random ordinal ids for historical reasons.
  -ive ids reserved for custom (user-defined) types.

  Size-optimized suffixes:
    -0  (empty       => 0-sized)
    -sm (small       => byte-sized)
    -md (medium      => short-sized)
    -lg (large       => int-sized)   ; Default when no suffix
    -xl (extra large => long-sized)"

  {82  :prefixed-custom

   47  :reader-sm
   51  :reader-md
   52  :reader-lg
   5   :reader-lg2 ; == :reader-lg, used only for back-compatible thawing

   46  :serializable-sm
   50  :serializable-md
   6   :serializable-lg ; Used only for back-compatible thawing

   48  :record-sm
   49  :record-md
   80  :record-lg ; Used only for back-compatible thawing

   81  :type

   3   :nil
   8   :true
   9   :false
   10  :char

   34  :str-0
   105 :str-sm
   16  :str-md
   13  :str-lg

   106 :kw-sm
   14  :kw-lg

   56  :sym-sm
   57  :sym-lg

   58  :regex
   71  :uri

   53  :bytes-0
   7   :bytes-sm
   15  :bytes-md
   2   :bytes-lg

   17  :vec-0
   113 :vec-2
   114 :vec-3
   110 :vec-sm
   69  :vec-md
   21  :vec-lg

   115 :objects-lg ; TODO Could include md, sm, 0 later if there's demand

   18  :set-0
   111 :set-sm
   32  :set-md
   23  :set-lg

   19  :map-0
   112 :map-sm
   33  :map-md
   30  :map-lg

   35  :list-0
   36  :list-sm
   54  :list-md
   20  :list-lg

   37  :seq-0
   38  :seq-sm
   39  :seq-md
   24  :seq-lg

   28  :sorted-set
   31  :sorted-map
   26  :queue
   25  :meta

   40  :byte
   41  :short
   42  :integer

   0   :long-zero
   100 :long-sm
   101 :long-md
   102 :long-lg
   43  :long-xl

   44  :bigint
   45  :biginteger

   60  :float
   55  :double-zero
   61  :double
   62  :bigdec
   70  :ratio

   90  :date
   91  :uuid

   59  :cached-0
   63  :cached-1
   64  :cached-2
   65  :cached-3
   66  :cached-4
   72  :cached-5
   73  :cached-6
   74  :cached-7
   67  :cached-sm
   68  :cached-md

   ;;; DEPRECATED (old types are supported only for thawing)
   1   :reader-depr1       ; v0.9.2+ for +64k support
   11  :str-depr1          ; v0.9.2+ for +64k support
   22  :map-depr1          ; v0.9.0+ for more efficient thaw
   12  :kw-depr1           ; v2.0.0-alpha5+ for str consistecy
   27  :map-depr2          ; v2.11+ for count/2
   29  :sorted-map-depr1   ; v2.11+ for count/2
   4   :boolean-depr1      ; v2.12+ for switch to true/false ids
   })

(comment
  (defn- get-free-byte-ids [ids-map]
    (reduce (fn [acc in] (if-not (ids-map in) (conj acc in) acc))
      [] (range 0 Byte/MAX_VALUE)))

  (count (get-free-byte-ids type-ids)))

(defmacro ^:private defids []
  `(do
     ~@(map
         (fn [[id# name#]]
           (let [name# (str "id-" (name name#))
                 sym#  (with-meta (symbol name#)
                         {:const true :private true})]
             `(def ~sym# (byte ~id#))))
         type-ids)))

(comment (macroexpand '(defids)))

(defids)

;;;; Ns imports (for convenience of lib consumers)

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

;;;; Dynamic config
;; See also `nippy.tools` ns for further dynamic config support

;; TODO Switch to thread-local proxies?

(enc/defonce ^:dynamic *freeze-fallback* "(fn [data-output x]), nil => default" nil)
(enc/defonce ^:dynamic *custom-readers* "{<hash-or-byte-id> (fn [data-input])}" nil)
(enc/defonce ^:dynamic *auto-freeze-compressor*
  "(fn [byte-array])->compressor used by `(freeze <x> {:compressor :auto}),
  nil => default"
  nil)

(defn set-freeze-fallback!        [x] (alter-var-root #'*freeze-fallback*        (constantly x)))
(defn set-auto-freeze-compressor! [x] (alter-var-root #'*auto-freeze-compressor* (constantly x)))
(defn swap-custom-readers!        [f] (alter-var-root #'*custom-readers* f))

(declare ^:dynamic *final-freeze-fallback*) ; DEPRECATED

;;;; Freezing

#_(do
  (defmacro write-id [out id] `(.writeByte ~out ~id))

  (defmacro ^:private sm-count? [n] `(<= ~n 255))   #_(- Byte/MAX_VALUE  Byte/MIN_VALUE)
  (defmacro ^:private md-count? [n] `(<= ~n 65535)) #_(- Short/MAX_VALUE Short/MIN_VALUE)

  (defmacro ^:private write-sm-count [out n]
    `(if (<= ~n 127)
       (.writeByte ~out ~n)
       (.writeByte ~out (unchecked-subtract 127 ~n))))

  (defmacro ^:private write-md-count [out n]
    `(if (<= ~n 32767)
       (.writeShort ~out ~n)
       (.writeShort ~out (unchecked-subtract 32767 ~n))))

  (defmacro ^:private write-lg-count [out n] `(.writeInt ~out ~n))

  (defmacro ^:private read-sm-count [in]
    `(let [n# (.readByte ~in)]
       (if (pos? n#)
         n#
         (unchecked-subtract 127 n#))))

  (defmacro ^:private read-md-count [in]
    `(let [n# (.readShort ~in)]
       (if (pos? n#)
         n#
         (unchecked-subtract 32767 n#))))

  (defmacro ^:private read-lg-count [in] `(.readInt ~in)))

(do
  (defmacro write-id [out id] `(.writeByte ~out ~id))

  (defmacro ^:private sm-count? [n] `(<= ~n 127))
  (defmacro ^:private md-count? [n] `(<= ~n 32767))

  (defmacro ^:private write-sm-count [out n] `(.writeByte  ~out ~n))
  (defmacro ^:private write-md-count [out n] `(.writeShort ~out ~n))
  (defmacro ^:private write-lg-count [out n] `(.writeInt   ~out ~n))

  (defmacro ^:private read-sm-count [in] `(.readByte  ~in))
  (defmacro ^:private read-md-count [in] `(.readShort ~in))
  (defmacro ^:private read-lg-count [in] `(.readInt   ~in)))

 ; We extend `IFreezable1` to supported types:
(defprotocol     IFreezable1 (-freeze-without-meta! [x data-output]))
(defprotocol     IFreezable2 (-freeze-with-meta!    [x data-output]))
(extend-protocol IFreezable2 ; Must be a separate protocol
  clojure.lang.IMeta
  (-freeze-with-meta! [x ^DataOutput data-output]
    (let [m (.meta x)]
      (when m
        (write-id data-output id-meta)
        (-freeze-without-meta! m data-output)))
    (-freeze-without-meta!     x data-output))

  nil
  (-freeze-with-meta! [x data-output]
    (-freeze-without-meta! x data-output))

  Object
  (-freeze-with-meta! [x data-output]
    (-freeze-without-meta! x data-output)))

(defn- write-bytes-sm [^DataOutput out ^bytes ba]
  (let [len (alength ba)]
    ;; (byte len)
    (write-sm-count out len)
    (.write         out ba 0 len)))

(defn- write-bytes-md [^DataOutput out ^bytes ba]
  (let [len (alength ba)]
    ;; (short len)
    (write-md-count out len)
    (.write         out ba 0 len)))

(defn- write-bytes-lg [^DataOutput out ^bytes ba]
  (let [len (alength ba)]
    (write-lg-count out len)
    (.write         out ba 0 len)))

(defn- write-bytes [^DataOutput out ^bytes ba]
  (let [len (alength ba)]
    (if (zero? len)
      (write-id out id-bytes-0)
      (do
        (cond*
          (sm-count? len)
          (do (write-id       out id-bytes-sm)
              (write-sm-count out len))

          (md-count? len)
          (do (write-id       out id-bytes-md)
              (write-md-count out len))

          :else
          (do (write-id       out id-bytes-lg)
              (write-lg-count out len)))

        (.write out ba 0 len)))))

;; (defn- str->bytes [s] (.getBytes s charset))
(defn- write-utf8-sm    [out ^String s]     (write-bytes-sm out (.getBytes s charset)))
(defn- write-utf8-md    [out ^String s]     (write-bytes-md out (.getBytes s charset)))
(defn- write-utf8-lg    [out ^String s]     (write-bytes-lg out (.getBytes s charset)))
(defn- write-utf8       [out ^String s]     (write-bytes    out (.getBytes s charset)))
(defn- write-biginteger [out ^BigInteger n] (write-bytes-lg out (.toByteArray n)))

(defn- write-str [^DataOutput out ^String s]
  (if (identical? s "")
    (write-id out id-str-0)
    (let [ba  (.getBytes s charset)
          len (alength ba)]
      (cond*
        (sm-count? len)
        (do (write-id       out id-str-sm)
            (write-sm-count out len))

        (md-count? len)
        (do (write-id       out id-str-md)
            (write-md-count out len))

        :else
        (do (write-id       out id-str-lg)
            (write-lg-count out len)))

      (.write out ba 0 len))))

(defn- write-kw [^DataOutput out kw]
  (let [s   (if-let [ns (namespace kw)] (str ns "/" (name kw)) (name kw))
        ba  (.getBytes s charset)
        len (alength ba)]
    (cond*
      (sm-count? len)
      (do (write-id       out id-kw-sm)
          (write-sm-count out len))

      :else ; Rare!
      (do (write-id       out id-kw-lg)
          (write-lg-count out len)))

    (.write out ba 0 len)))

(defn- write-sym [^DataOutput out s]
  (let [s   (if-let [ns (namespace s)] (str ns "/" (name s)) (name s))
        ba  (.getBytes s charset)
        len (alength ba)]
    (cond*
      (sm-count? len)
      (do (write-id       out id-sym-sm)
          (write-sm-count out len))

      :else ; Rare!
      (do (write-id       out id-sym-lg)
          (write-lg-count out len)))

    (.write out ba 0 len)))

(defn- write-long [^DataOutput out ^long n]
  (cond*
    (zero? n)
    (write-id out id-long-zero)

    (> n 0)
    (cond*
      (<= n 127 #_Byte/MAX_VALUE)
      (do (write-id   out id-long-sm)
          (.writeByte out n))

      (<= n 32767 #_Short/MAX_VALUE)
      (do (write-id    out id-long-md)
          (.writeShort out n))

      (<= n 2147483647 #_Integer/MAX_VALUE)
      (do (write-id  out id-long-lg)
          (.writeInt out n))

      :else
      (do (write-id   out id-long-xl)
          (.writeLong out n)))

    :else
    (cond*
      (>= n -128 #_Byte/MIN_VALUE)
      (do (write-id   out id-long-sm)
          (.writeByte out n))

      (>= n -32768 #_Short/MIN_VALUE)
      (do (write-id    out id-long-md)
          (.writeShort out n))

      (>= n -2147483648 #_Integer/MIN_VALUE)
      (do (write-id   out id-long-lg)
          (.writeInt  out n))

      :else
      (do (write-id   out id-long-xl)
          (.writeLong out n)))))

(defmacro ^:private -run!    [proc coll] `(do (reduce    #(~proc %2)    nil ~coll) nil))
(defmacro ^:private -run-kv! [proc    m] `(do (reduce-kv #(~proc %2 %3) nil    ~m) nil))

(defn- write-vec [^DataOutput out v]
  (let [cnt (count v)]
    (if (zero? cnt)
      (write-id out id-vec-0)
      (do
        (cond*
          (sm-count? cnt)
          (cond*
            (== cnt 2) (write-id out id-vec-2)
            (== cnt 3) (write-id out id-vec-3)
            :else
            (do (write-id       out id-vec-sm)
                (write-sm-count out cnt)))

          (md-count? cnt)
          (do (write-id       out id-vec-md)
              (write-md-count out cnt))

          :else
          (do (write-id       out id-vec-lg)
              (write-lg-count out cnt)))

        (-run! (fn [in] (-freeze-with-meta! in out)) v)))))

(defn- write-kvs
  ([^DataOutput out id-lg coll]
   (let [cnt (count coll)]
     (write-id       out id-lg)
     (write-lg-count out cnt)
     (-run-kv!
       (fn [k v]
         (-freeze-with-meta! k out)
         (-freeze-with-meta! v out))
       coll)))

  ([^DataOutput out id-empty id-sm id-md id-lg coll]
   (let [cnt (count coll)]
     (if (zero? cnt)
       (write-id out id-empty)
       (do
         (cond*
           (sm-count? cnt)
           (do (write-id       out id-sm)
               (write-sm-count out cnt))

           (md-count? cnt)
           (do (write-id       out id-md)
               (write-md-count out cnt))

           :else
           (do (write-id       out id-lg)
               (write-lg-count out cnt)))

         (-run-kv!
           (fn [k v]
             (-freeze-with-meta! k out)
             (-freeze-with-meta! v out))
           coll))))))

(defn- write-counted-coll
  ([^DataOutput out id-lg coll]
   (let [cnt (count coll)]
     ;; (assert (counted? coll))
     (write-id       out id-lg)
     (write-lg-count out cnt)
     (-run! (fn [in] (-freeze-with-meta! in out)) coll)))

  ([^DataOutput out id-empty id-sm id-md id-lg coll]
   (let [cnt (count coll)]
     ;; (assert (counted? coll))
     (if (zero? cnt)
       (write-id out id-empty)
       (do
         (cond*
           (sm-count? cnt)
           (do (write-id       out id-sm)
               (write-sm-count out cnt))

           (md-count? cnt)
           (do (write-id       out id-md)
               (write-md-count out cnt))

           :else
           (do (write-id       out id-lg)
               (write-lg-count out cnt)))

         (-run! (fn [in] (-freeze-with-meta! in out)) coll))))))

(defn- write-uncounted-coll
  ([^DataOutput out id-lg coll]
   ;; (assert (not (counted? coll)))
   (let [bas  (ByteArrayOutputStream. 32)
         sout (DataOutputStream. bas)
         ^long cnt (reduce (fn [^long cnt in] (-freeze-with-meta! in sout) (unchecked-inc cnt)) 0 coll)
         ba   (.toByteArray bas)]

     (write-id       out id-lg)
     (write-lg-count out cnt)
     (.write         out ba)))

  ([^DataOutput out id-empty id-sm id-md id-lg coll]
   (let [bas  (ByteArrayOutputStream. 32)
         sout (DataOutputStream. bas)
         ^long cnt (reduce (fn [^long cnt in] (-freeze-with-meta! in sout) (unchecked-inc cnt)) 0 coll)
         ba   (.toByteArray bas)]

     (if (zero? cnt)
       (write-id out id-empty)
       (do
         (cond*
           (sm-count? cnt)
           (do (write-id       out id-sm)
               (write-sm-count out cnt))

           (md-count? cnt)
           (do (write-id       out id-md)
               (write-md-count out cnt))

           :else
           (do (write-id       out id-lg)
               (write-lg-count out cnt)))

         (.write out ba))))))

(defn- write-coll
  ([out id-lg coll]
   (if (counted? coll)
     (write-counted-coll   out id-lg coll)
     (write-uncounted-coll out id-lg coll)))

  ([out id-empty id-sm id-md id-lg coll]
   (if (counted? coll)
     (write-counted-coll   out id-empty id-sm id-md id-lg coll)
     (write-uncounted-coll out id-empty id-sm id-md id-lg coll))))

;; Micro-optimization:
;; As (write-kvs out id-map-0 id-map-sm id-map-md id-map-lg x)
(defn- write-map [^DataOutput out m]
  (let [cnt (count m)]
    (if (zero? cnt)
      (write-id out id-map-0)
      (do
        (cond*
          (sm-count? cnt)
          (do (write-id       out id-map-sm)
              (write-sm-count out cnt))

          (md-count? cnt)
          (do (write-id       out id-map-md)
              (write-md-count out cnt))

          :else
          (do (write-id      out id-map-lg)
              (write-lg-count out cnt)))

        (-run-kv!
          (fn [k v]
            (-freeze-with-meta! k out)
            (-freeze-with-meta! v out))
          m)))))

;; Micro-optimization:
;; As (write-counted-coll out id-set-0 id-set-sm id-set-md id-set-lg x)
(defn- write-set [^DataOutput out s]
  (let [cnt (count s)]
    (if (zero? cnt)
      (write-id out id-set-0)
      (do
        (cond*
          (sm-count? cnt)
          (do (write-id       out id-set-sm)
              (write-sm-count out cnt))

          (md-count? cnt)
          (do (write-id       out id-set-md)
              (write-md-count out cnt))

          :else
          (do (write-id       out id-set-lg)
              (write-lg-count out cnt)))

        (-run! (fn [in] (-freeze-with-meta! in out)) s)))))

(defn- write-objects [^DataOutput out ^objects ary]
  (let [len (alength ary)]
    (write-id       out id-objects-lg)
    (write-lg-count out len)
    (-run! (fn [in] (-freeze-with-meta! in out)) ary)))

(defn- write-serializable [^DataOutput out x]
  (when-debug (println (str "write-serializable: " (type x))))
  (let [cname    (.getName (class x)) ; Reflect
        cname-ba (.getBytes cname charset)
        len      (alength cname-ba)]
    (cond*
      (sm-count? len)
      (do (write-id       out id-serializable-sm)
          (write-bytes-sm out cname-ba))

      ;; Note no :serializable-lg freeze support (unrealistic)

      :else
      (do (write-id       out id-serializable-md)
          (write-bytes-md out cname-ba)))

    (.writeObject (ObjectOutputStream. out) x)))

(defn- write-readable [^DataOutput out x]
  (when-debug (println (str "write-readable: " (type x))))
  (let [edn    (enc/pr-edn x)
        edn-ba (.getBytes ^String edn charset)
        len    (alength edn-ba)]
    (cond*
      (sm-count? len)
      (do (write-id       out id-reader-sm)
          (write-bytes-sm out edn-ba))

      (md-count? len)
      (do (write-id       out id-reader-md)
          (write-bytes-md out edn-ba))

      :else
      (do (write-id       out id-reader-lg)
          (write-bytes-lg out edn-ba)))))

(defn try-write-serializable [out x]
  (when (utils/serializable? x)
    (try
      (write-serializable out x)
      true
      (catch Throwable _ nil))))

(defn try-write-readable [out x]
  (when (utils/readable? x)
    (try
      (write-readable out x)
      true
      (catch Throwable _ nil))))

(defn- try-pr-edn [x]
  (try
    (enc/pr-edn x)
    (catch Throwable _
      (try
        (str x)
        (catch Throwable _ :nippy/unprintable)))))

(defn write-unfreezable [out x]
  (-freeze-without-meta!
    {:type (type x)
     :nippy/unfreezable (try-pr-edn x)}
    out))

(defn throw-unfreezable [x]
  (throw
    (ex-info (str "Unfreezable type: " (type x))
      {:type   (type x)
       :as-str (try-pr-edn x)})))

;; Public `-freeze-with-meta!` with different arg order
(defn freeze-to-out!
  "Serializes arg (any Clojure data type) to a DataOutput.
  This is a low-level util: in most cases you'll want `freeze` instead."
  [^DataOutput data-output x] (-freeze-with-meta! x data-output))

(defmacro ^:private freezer [type & body]
  `(extend-type ~type IFreezable1
     (~'-freeze-without-meta! [~'x ~(with-meta 'out {:tag 'DataOutput})]
       ~@body)))

(defmacro ^:private id-freezer [type id & body]
  `(extend-type ~type IFreezable1
     (~'-freeze-without-meta! [~'x ~(with-meta 'out {:tag 'DataOutput})]
      (write-id ~'out ~id)
      ~@body)))

;;;; Caching ; Experimental

;; Nb: don't use an auto initialValue; can cause thread-local state to
;; accidentally hang around with the use of `freeze-to-out!`, etc.
;; Safer to require explicit activation through `with-cache`.
(def ^ThreadLocal -cache-proxy
  "{[<x> <meta>] <idx>} for freezing, {<idx> <x-with-meta>} for thawing."
  (proxy [ThreadLocal] []))

(defmacro ^:private with-cache
  "Experimental, subject to change.
  Executes body with support for freezing/thawing cached values.

  This is a low-level util: you won't need to use this yourself unless
  you're using `freeze-to-out!` or `thaw-from-in!` (also low-level utils).

  See also `cache`."
  [& body]
  `(try
     (.set -cache-proxy (enc/-vol! nil))
     (do ~@body)
     (finally (.remove -cache-proxy))))

(deftype Cached [val])
(defn cache
  "Experimental, subject to change.

  Wraps value so that future writes of the same wrapped value with same
  metadata will be efficiently encoded as references to this one.

  (freeze [(cache \"foo\") (cache \"foo\") (cache \"foo\")])
    will incl. a single \"foo\", plus 2x single-byte references to \"foo\"."
  [x]
  (if (instance? Cached x) x (Cached. x)))

(comment (cache "foo"))

(freezer Cached
  (let [x-val (.-val x)]
    (if-let [cache_ (.get -cache-proxy)]
      (let [cache    @cache_
            k        #_x-val [x-val (meta x-val)]
            ?idx     (get cache k)
            ^int idx (or ?idx
                       (let [idx (count cache)]
                         (enc/-vol-swap! cache_ assoc k idx)
                         idx))

            first-occurance? (nil? ?idx)]

        (cond*
          (sm-count? idx)
          (case (int idx)
            0 (do (write-id out id-cached-0) (when first-occurance? (-freeze-with-meta! x-val out)))
            1 (do (write-id out id-cached-1) (when first-occurance? (-freeze-with-meta! x-val out)))
            2 (do (write-id out id-cached-2) (when first-occurance? (-freeze-with-meta! x-val out)))
            3 (do (write-id out id-cached-3) (when first-occurance? (-freeze-with-meta! x-val out)))
            4 (do (write-id out id-cached-4) (when first-occurance? (-freeze-with-meta! x-val out)))
            5 (do (write-id out id-cached-5) (when first-occurance? (-freeze-with-meta! x-val out)))
            6 (do (write-id out id-cached-6) (when first-occurance? (-freeze-with-meta! x-val out)))
            7 (do (write-id out id-cached-7) (when first-occurance? (-freeze-with-meta! x-val out)))

            (do
              (write-id       out id-cached-sm)
              (write-sm-count out idx)
              (when first-occurance? (-freeze-with-meta! x-val out))))

          (md-count? idx)
          (do
            (write-id       out id-cached-md)
            (write-md-count out idx)
            (when first-occurance? (-freeze-with-meta! x-val out)))

          :else
          ;; (throw (ex-info "Max cache size exceeded" {:idx idx}))
          (-freeze-with-meta! x-val out) ; Just freeze uncached
          ))

      (-freeze-with-meta! x-val out))))

(declare thaw-from-in!)
(def ^:private thaw-cached
  (let [not-found (Object.)]
    (fn [idx in]
      (if-let [cache_ (.get -cache-proxy)]
        (let [v (get @cache_ idx not-found)]
          (if (identical? v not-found)
            (let [x (thaw-from-in! in)]
              (enc/-vol-swap! cache_ assoc idx x)
              x)
            v))
        (throw (ex-info "No cache_ established, can't thaw. See `with-cache`."
                 {}))))))

(comment
  (thaw (freeze [(cache "foo") (cache "foo") (cache "foo")]))
  (let [v1 (with-meta [] {:id :v1})
        v2 (with-meta [] {:id :v2})]
    (mapv meta
      (thaw (freeze [(cache v1) (cache v2) (cache v1) (cache v2)])))))

;;;;

(id-freezer nil        id-nil)
(id-freezer (type '()) id-list-0)
(id-freezer Character  id-char       (.writeChar       out (int x)))
(id-freezer Byte       id-byte       (.writeByte       out x))
(id-freezer Short      id-short      (.writeShort      out x))
(id-freezer Integer    id-integer    (.writeInt        out x))
(id-freezer BigInt     id-bigint     (write-biginteger out (.toBigInteger x)))
(id-freezer BigInteger id-biginteger (write-biginteger out x))
(id-freezer Pattern    id-regex      (write-str        out (str x)))
(id-freezer Float      id-float      (.writeFloat      out x))
(id-freezer BigDecimal id-bigdec
  (write-biginteger out (.unscaledValue x))
  (.writeInt out (.scale x)))

(id-freezer Ratio id-ratio
  (write-biginteger out (.numerator   x))
  (write-biginteger out (.denominator x)))

(id-freezer Date id-date (.writeLong out (.getTime x)))

(id-freezer URI id-uri
  (write-str out (.toString x)))

(id-freezer UUID id-uuid
  (.writeLong out (.getMostSignificantBits  x))
  (.writeLong out (.getLeastSignificantBits x)))

(freezer Boolean              (if x (write-id out id-true) (write-id out id-false)))
(freezer (Class/forName "[B")                  (write-bytes   out x))
(freezer (Class/forName "[Ljava.lang.Object;") (write-objects out x))
(freezer String               (write-str   out x))
(freezer Keyword              (write-kw    out x))
(freezer Symbol               (write-sym   out x))
(freezer Long                 (write-long  out x))
(freezer Double
  (if (zero? ^double x)
    (write-id         out id-double-zero)
    (do (write-id     out id-double)
        (.writeDouble out x))))

(freezer PersistentQueue      (write-counted-coll   out id-queue      x))
(freezer PersistentTreeSet    (write-counted-coll   out id-sorted-set x))
(freezer PersistentTreeMap    (write-kvs            out id-sorted-map x))
(freezer APersistentVector    (write-vec            out               x))
(freezer APersistentSet       (write-set            out               x))
(freezer APersistentMap       (write-map            out               x))
(freezer PersistentList       (write-counted-coll   out id-list-0 id-list-sm id-list-md id-list-lg x))
(freezer LazySeq              (write-uncounted-coll out  id-seq-0  id-seq-sm  id-seq-md  id-seq-lg x))
(freezer ISeq                 (write-coll           out  id-seq-0  id-seq-sm  id-seq-md  id-seq-lg x))
(freezer IRecord
  (let [cname    (.getName (class x)) ; Reflect
        cname-ba (.getBytes cname charset)
        len      (alength cname-ba)]
    (cond*
      (sm-count? len)
      (do (write-id       out id-record-sm)
          (write-bytes-sm out cname-ba))

      ;; Note no :record-lg freeze support (unrealistic)

      :else
      (do (write-id       out id-record-md)
          (write-bytes-md out cname-ba)))

    (-freeze-without-meta! (into {} x) out)))

(freezer IType
   (let [aclass (class x)
         cname  (.getName aclass)]
     (write-id  out id-type)
     (write-str out cname)
     (let [basis-method (.getMethod aclass "getBasis" nil)
           basis        (.invoke basis-method nil nil)]
       (-run!
         (fn [b]
           (let [^Field cfield (.getField aclass (name b))]
             (let [fvalue (.get cfield x)]
               (-freeze-without-meta! fvalue out))))
         basis))))

(freezer Object
  (when-debug (println (str "freeze-fallback: " (type x))))
  (if-let [ff *freeze-fallback*]
    (if (identical? ff :write-unfreezable)
      (or
        (try-write-serializable out x)
        (try-write-readable     out x)
        (write-unfreezable      out x))
      (ff out x))

    (or
      (try-write-serializable out x)
      (try-write-readable     out x)

      ;; For back compatibility (nb Timbre's Carmine appender)
      (when-let [fff *final-freeze-fallback*]
        (fff out x)
        true)

      (throw-unfreezable x))))

;;;;

(def ^:private head-meta-id (reduce-kv #(assoc %1 %3 %2) {} head-meta))
(def ^:private get-head-ba
  (enc/memoize_
    (fn [head-meta]
      (when-let [meta-id (get head-meta-id (assoc head-meta :version head-version))]
        (enc/ba-concat head-sig (byte-array [meta-id]))))))

(defn- wrap-header [data-ba head-meta]
  (if-let [head-ba (get-head-ba head-meta)]
    (enc/ba-concat head-ba data-ba)
    (throw (ex-info (str "Unrecognized header meta: " head-meta)
             {:head-meta head-meta}))))

(comment (wrap-header (.getBytes "foo") {:compressor-id :lz4
                                         :encryptor-id  nil}))

(defn fast-freeze
  "Like `freeze` but:
    - Writes data without a Nippy header
    - Drops all support for compression and encryption
    - Must be thawed with `fast-thaw`

  Equivalent to (but a little faster than):
    `(freeze x {:compressor nil :encryptor nil :no-header? true})"
  [x]
  (let [baos (ByteArrayOutputStream. 64)
        dos  (DataOutputStream. baos)]
    (with-cache (-freeze-with-meta! x dos))
    (.toByteArray baos)))

(defn freeze
  "Serializes arg (any Clojure data type) to a byte array. To freeze custom
  types, extend the Clojure reader or see `extend-freeze`."
  ([x] (freeze x nil))
  ([x {:keys [compressor encryptor password]
       :or   {compressor :auto
              encryptor  aes128-encryptor}
       :as   opts}]
    (let [;; Intentionally undocumented:
          no-header? (or (get opts :no-header?)
                         (get opts :skip-header?))
          encryptor  (when password encryptor)
          baos (ByteArrayOutputStream. 64)
          dos  (DataOutputStream. baos)]

      (if (and (nil? compressor) (nil? encryptor))
        (do ; Optimized case
          (when-not no-header? ; Avoid `wrap-header`'s array copy:
            (let [head-ba (get-head-ba {:compressor-id nil :encryptor-id nil})]
              (.write dos head-ba 0 4)))
          (with-cache (-freeze-with-meta! x dos))
          (.toByteArray baos))

        (do
          (with-cache (-freeze-with-meta! x dos))
          (let [ba (.toByteArray baos)

                compressor
                (if (identical? compressor :auto)
                  (if no-header?
                    lz4-compressor
                    (if-let [fc *auto-freeze-compressor*]
                      (fc ba)
                      ;; Intelligently enable compression only if benefit
                      ;; is likely to outweigh cost:
                      (when (> (alength ba) 8192) lz4-compressor)))

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

(defn- read-bytes [^DataInput in len]
  (let [ba (byte-array len)]
    (.readFully in ba 0 len)
    ba))

(defn- read-bytes-sm [^DataInput in] (read-bytes in (read-sm-count in)))
(defn- read-bytes-md [^DataInput in] (read-bytes in (read-md-count in)))
(defn- read-bytes-lg [^DataInput in] (read-bytes in (read-lg-count in)))

(defn- read-utf8    [in len]        (String. ^bytes (read-bytes in len)                charset))
(defn- read-utf8-sm [^DataInput in] (String. ^bytes (read-bytes in (read-sm-count in)) charset))
(defn- read-utf8-md [^DataInput in] (String. ^bytes (read-bytes in (read-md-count in)) charset))
(defn- read-utf8-lg [^DataInput in] (String. ^bytes (read-bytes in (read-lg-count in)) charset))

(defn- read-biginteger [^DataInput in] (BigInteger. ^bytes (read-bytes in (.readInt in))))

(defmacro ^:private editable? [coll] `(instance? clojure.lang.IEditableCollection ~coll))

(defn- read-into [to ^DataInput in ^long n]
  (if (and (editable? to) (> n 10))
    (persistent!
      (enc/reduce-n (fn [acc _] (conj! acc (thaw-from-in! in)))
        (transient to) n))

    (enc/reduce-n (fn [acc _] (conj acc (thaw-from-in! in))) to n)))

(defn- read-objects [^objects ary ^DataInput in]
  (enc/reduce-n
    (fn [^objects ary i]
      (aset ary i (thaw-from-in! in))
      ary)
    ary (alength ary)))

(defn- read-kvs-into [to ^DataInput in ^long n]
  (if (and (editable? to) (> n 10))
    (persistent!
      (enc/reduce-n (fn [acc _] (assoc! acc (thaw-from-in! in) (thaw-from-in! in)))
        (transient to) n))

    (enc/reduce-n (fn [acc _] (assoc acc (thaw-from-in! in) (thaw-from-in! in)))
      to n)))

(defn- read-kvs-depr1 [to ^DataInput in] (read-kvs-into to in (quot (.readInt in) 2)))

(def ^:private class-method-sig (into-array Class [IPersistentMap]))

(defn- read-custom! [in prefixed? type-id]
  (if-let [custom-reader (get *custom-readers* type-id)]
    (try
      (custom-reader in)
      (catch Exception e
        (throw
          (ex-info
            (str "Reader exception for custom type id: " type-id)
            {:type-id type-id
             :prefixed? prefixed?} e))))
    (throw
      (ex-info
        (str "No reader provided for custom type id: " type-id)
        {:type-id type-id
         :prefixed? prefixed?}))))

(defn- read-edn [edn]
  (try
    (enc/read-edn {:readers *data-readers*} edn)
    (catch Exception e
      {:type :reader
       :throwable e
       :nippy/unthawable edn})))

(defn- read-serializable [^DataInput in class-name]
  (try
    (let [content (.readObject (ObjectInputStream. in))]
      (try
        (let [class (Class/forName class-name)] (cast class content))
        (catch Exception e
          {:type :serializable
           :throwable e
           :nippy/unthawable {:class-name class-name :content content}})))
    (catch Exception e
      {:type :serializable
       :throwable e
       :nippy/unthawable {:class-name class-name :content nil}})))

(defn- read-record [in class-name]
  (let [content (thaw-from-in! in)]
    (try
      (let [class  (clojure.lang.RT/classForName class-name)
            method (.getMethod class "create" class-method-sig)]
        (.invoke method class (into-array Object [content])))
      (catch Exception e
        {:type :record
         :throwable e
         :nippy/unthawable {:class-name class-name :content content}}))))

(defn- read-type [in class-name]
  (try
    (let [aclass (clojure.lang.RT/classForName class-name)
          nbasis
          (let [basis-method (.getMethod aclass "getBasis" nil)
                basis        (.invoke basis-method nil nil)]
            (count basis))

          cvalues (object-array nbasis)]

      (enc/reduce-n
        (fn [_ i] (aset cvalues i (thaw-from-in! in)))
        nil nbasis)

      (let [ctors (.getConstructors aclass)
            ^Constructor ctor (aget ctors 0) ; Impl. detail? Ref. https://goo.gl/XWmckR
            ]
        (.newInstance ctor cvalues)))

    (catch Exception e
      {:type             :type
       :throwable        e
       :nippy/unthawable {:class-name class-name}})))

(defn thaw-from-in!
  "Deserializes a frozen object from given DataInput to its original Clojure
  data type.

  This is a low-level util: in most cases you'll want `thaw` instead."
  [^DataInput data-input]
  (let [in      data-input
        type-id (.readByte in)]
    (when-debug (println (str "thaw-from-in!: " type-id)))
    (try
      (enc/case-eval type-id

        id-reader-sm       (read-edn             (read-utf8 in (read-sm-count in)))
        id-reader-md       (read-edn             (read-utf8 in (read-md-count in)))
        id-reader-lg       (read-edn             (read-utf8 in (read-lg-count in)))
        id-reader-lg2      (read-edn             (read-utf8 in (read-lg-count in)))
        id-serializable-sm (read-serializable in (read-utf8 in (read-sm-count in)))
        id-serializable-md (read-serializable in (read-utf8 in (read-md-count in)))
        id-serializable-lg (read-serializable in (read-utf8 in (read-lg-count in)))
        id-record-sm       (read-record       in (read-utf8 in (read-sm-count in)))
        id-record-md       (read-record       in (read-utf8 in (read-md-count in)))
        id-record-lg       (read-record       in (read-utf8 in (read-lg-count in)))

        id-type        (read-type in (thaw-from-in! in))

        id-nil         nil
        id-true        true
        id-false       false
        id-char        (.readChar in)
        id-meta        (let [m (thaw-from-in! in)]
                         (with-meta (thaw-from-in! in) m))

        id-cached-0    (thaw-cached 0 in)
        id-cached-1    (thaw-cached 1 in)
        id-cached-2    (thaw-cached 2 in)
        id-cached-3    (thaw-cached 3 in)
        id-cached-4    (thaw-cached 4 in)
        id-cached-5    (thaw-cached 5 in)
        id-cached-6    (thaw-cached 6 in)
        id-cached-7    (thaw-cached 7 in)
        id-cached-sm   (thaw-cached (read-sm-count in) in)
        id-cached-md   (thaw-cached (read-md-count in) in)

        id-bytes-0     (byte-array 0)
        id-bytes-sm    (read-bytes in (read-sm-count in))
        id-bytes-md    (read-bytes in (read-md-count in))
        id-bytes-lg    (read-bytes in (read-lg-count in))

        id-objects-lg  (read-objects (object-array (read-lg-count in)) in)

        id-str-0       ""
        id-str-sm               (read-utf8 in (read-sm-count in))
        id-str-md               (read-utf8 in (read-md-count in))
        id-str-lg               (read-utf8 in (read-lg-count in))
        id-kw-sm       (keyword (read-utf8 in (read-sm-count in)))
        id-kw-lg       (keyword (read-utf8 in (read-lg-count in)))
        id-sym-sm      (symbol  (read-utf8 in (read-sm-count in)))
        id-sym-lg      (symbol  (read-utf8 in (read-lg-count in)))
        id-regex       (re-pattern (thaw-from-in! in))

        id-vec-0       []
        id-vec-2       [(thaw-from-in! in) (thaw-from-in! in)]
        id-vec-3       [(thaw-from-in! in) (thaw-from-in! in) (thaw-from-in! in)]
        id-vec-sm      (read-into [] in (read-sm-count in))
        id-vec-md      (read-into [] in (read-md-count in))
        id-vec-lg      (read-into [] in (read-lg-count in))

        id-set-0       #{}
        id-set-sm      (read-into    #{} in (read-sm-count in))
        id-set-md      (read-into    #{} in (read-md-count in))
        id-set-lg      (read-into    #{} in (read-lg-count in))

        id-map-0       {}
        id-map-sm      (read-kvs-into {} in (read-sm-count in))
        id-map-md      (read-kvs-into {} in (read-md-count in))
        id-map-lg      (read-kvs-into {} in (read-lg-count in))

        id-queue       (read-into (PersistentQueue/EMPTY) in (read-lg-count in))
        id-sorted-set  (read-into     (sorted-set)        in (read-lg-count in))
        id-sorted-map  (read-kvs-into (sorted-map)        in (read-lg-count in))

        id-list-0      '()
        id-list-sm     (into '() (rseq (read-into [] in (read-sm-count in))))
        id-list-md     (into '() (rseq (read-into [] in (read-md-count in))))
        id-list-lg     (into '() (rseq (read-into [] in (read-lg-count in))))

        id-seq-0       (lazy-seq nil)
        id-seq-sm      (or (seq (read-into [] in (read-sm-count in))) (lazy-seq nil))
        id-seq-md      (or (seq (read-into [] in (read-md-count in))) (lazy-seq nil))
        id-seq-lg      (or (seq (read-into [] in (read-lg-count in))) (lazy-seq nil))

        id-byte              (.readByte  in)
        id-short             (.readShort in)
        id-integer           (.readInt   in)
        id-long-zero   0
        id-long-sm     (long (.readByte  in))
        id-long-md     (long (.readShort in))
        id-long-lg     (long (.readInt   in))
        id-long-xl           (.readLong  in)

        id-bigint      (bigint (read-biginteger in))
        id-biginteger          (read-biginteger in)

        id-float       (.readFloat  in)
        id-double-zero 0.0
        id-double      (.readDouble in)
        id-bigdec      (BigDecimal. ^BigInteger (read-biginteger in) (.readInt in))

        id-ratio       (clojure.lang.Ratio.
                         (read-biginteger in)
                         (read-biginteger in))

        id-date        (Date. (.readLong in))
        id-uri         (URI. (thaw-from-in! in))
        id-uuid        (UUID. (.readLong in) (.readLong in))

        ;; Deprecated ------------------------------------------------------
        id-boolean-depr1    (.readBoolean in)
        id-sorted-map-depr1 (read-kvs-depr1 (sorted-map) in)
        id-map-depr2        (read-kvs-depr1 {} in)
        id-reader-depr1     (read-edn (.readUTF in))
        id-str-depr1                  (.readUTF in)
        id-kw-depr1         (keyword  (.readUTF in))
        id-map-depr1        (apply hash-map
                              (enc/repeatedly-into [] (* 2 (.readInt in))
                                (fn [] (thaw-from-in! in))))
        ;; -----------------------------------------------------------------

        id-prefixed-custom (read-custom! in :prefixed (.readShort in))

        (if (neg? type-id)
          (read-custom! in nil type-id) ; Unprefixed custom type
          (throw
            (ex-info
              (str "Unrecognized type id (" type-id "). Data frozen with newer Nippy version?")
              {:type-id type-id}))))

      (catch Exception e
        (throw (ex-info (str "Thaw failed against type-id: " type-id)
                 {:type-id type-id} e))))))

(let [head-sig head-sig] ; Not ^:const
  (defn- try-parse-header [^bytes ba]
    (let [len (alength ba)]
      (when (> len 4)
        (let [-head-sig (java.util.Arrays/copyOf ba 3)]
          (when (java.util.Arrays/equals -head-sig ^bytes head-sig)
            ;; Header appears to be well-formed
            (let [meta-id (aget ba 3)
                  data-ba (java.util.Arrays/copyOfRange ba 4 len)]
              [data-ba (get head-meta meta-id {:unrecognized-meta? true})])))))))

(defn- get-auto-compressor [compressor-id]
  (case compressor-id
    nil        nil
    :snappy    snappy-compressor
    :lzma2     lzma2-compressor
    :lz4       lz4-compressor
    :no-header (throw (ex-info ":auto not supported on headerless data." {}))
    :else (throw (ex-info ":auto not supported for non-standard compressors." {}))
    (throw (ex-info (str "Unrecognized :auto compressor id: " compressor-id)
             {:compressor-id compressor-id}))))

(defn- get-auto-encryptor [encryptor-id]
  (case encryptor-id
    nil            nil
    :aes128-sha512 aes128-encryptor
    :no-header     (throw (ex-info ":auto not supported on headerless data." {}))
    :else (throw (ex-info ":auto not supported for non-standard encryptors." {}))
    (throw (ex-info (str "Unrecognized :auto encryptor id: " encryptor-id)
             {:encryptor-id encryptor-id}))))

(def ^:private err-msg-unknown-thaw-failure
  "Decryption/decompression failure, or data unfrozen/damaged.")

(def ^:private err-msg-unrecognized-header
  "Unrecognized (but apparently well-formed) header. Data frozen with newer Nippy version?")

(defn fast-thaw
  "Like `thaw` but:
    - Drops all support for compression and encryption
    - Supports only data frozen with `fast-freeze`

  Equivalent to (but a little faster than):
    `(thaw x {:compressor nil :encryptor nil :no-header? true})"
  [^bytes ba]
  (let [dis (DataInputStream. (ByteArrayInputStream. ba))]
    (with-cache (thaw-from-in! dis))))

(defn thaw
  "Deserializes a frozen Nippy byte array to its original Clojure data type.
  To thaw custom types, extend the Clojure reader or see `extend-thaw`.

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

   (assert (not (get opts :headerless-meta))
     ":headerless-meta `thaw` opt removed in Nippy v2.7+")

   (let [v2+?       (not v1-compatibility?)
         no-header? (get opts :no-header?) ; Intentionally undocumented
         ex (fn ex
              ([  msg] (ex nil msg))
              ([e msg] (throw (ex-info (str "Thaw failed: " msg)
                                {:opts (assoc opts
                                         :compressor compressor
                                         :encryptor  encryptor)}
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

                 (with-cache (thaw-from-in! dis)))

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

  NB: be careful about extending to interfaces, Ref. http://goo.gl/6gGRlU.

  (defrecord MyRec [data])
  (extend-freeze MyRec :foo/my-type [x data-output] ; Keyword id
    (.writeUTF [data-output] (:data x)))
  ;; or
  (extend-freeze MyRec 1 [x data-output] ; Byte id
    (.writeUTF [data-output] (:data x)))"
  [type custom-type-id [x out] & body]
  (assert-custom-type-id custom-type-id)
  (let [write-id-form
        (if (keyword? custom-type-id)
          ;; Prefixed [const byte id][cust hash id][payload]:
          `(do (write-id    ~out ~id-prefixed-custom)
               (.writeShort ~out ~(coerce-custom-type-id custom-type-id)))
          ;; Unprefixed [cust byte id][payload]:
          `(write-id ~out ~(coerce-custom-type-id custom-type-id)))]

    `(extend-type ~type IFreezable1
       (~'-freeze-without-meta! [~x ~(with-meta out {:tag 'java.io.DataOutput})]
        ~write-id-form
        ~@body))))

(defmacro extend-thaw
  "Extends Nippy to support thawing of a custom type with given id:
  (extend-thaw :foo/my-type [data-input] ; Keyword id
    (MyRec. (.readUTF data-input)))
  ;; or
  (extend-thaw 1 [data-input] ; Byte id
    (MyRec. (.readUTF data-input)))"
  [custom-type-id [in] & body]
  (assert-custom-type-id custom-type-id)
  `(do
     (when (contains? *custom-readers* ~(coerce-custom-type-id custom-type-id))
       (println (str "Warning: resetting Nippy thaw for custom type with id: "
                  ~custom-type-id)))
     (swap-custom-readers!
       (fn [m#]
         (assoc m#
           ~(coerce-custom-type-id custom-type-id)
           (fn [~(with-meta in {:tag 'java.io.DataInput})]
             ~@body))))))

(comment
  *custom-readers*
  (defrecord MyRec [data])
  (extend-freeze MyRec 1 [x out] (.writeUTF out (:data x)))
  (extend-thaw 1 [in] (MyRec. (.readUTF in)))
  (thaw (freeze (MyRec. "Joe"))))

;;;; Stress data

(defrecord StressRecord [data])
(deftype StressType [data]
  Object
  (equals [a b] (= (.-data a) (.-data ^StressType b))))
(def stress-data "Reference data used for tests & benchmarks"
  {:bytes     (byte-array [(byte 1) (byte 2) (byte 3)])
   :nil       nil
   :true      true
   :false     false
   :char      \ಬ
   :str-short "ಬಾ ಇಲ್ಲಿ ಸಂಭವಿಸ"
   :str-long  (apply str (range 1000))
   :kw        :keyword
   :kw-ns     ::keyword
   :sym       'foo
   :sym-ns    'foo/bar
   :regex     #"^(https?:)?//(www\?|\?)?"

   ;;; Try reflect real-world data:
   :lotsa-small-numbers  (vec (range 200))
   :lotsa-small-keywords (->> (java.util.Locale/getISOLanguages)
                              (mapv keyword))
   :lotsa-small-strings  (->> (java.util.Locale/getISOCountries)
                              (mapv #(.getDisplayCountry (java.util.Locale. "en" %))))

   :queue        (enc/queue [:a :b :c :d :e :f :g])
   :queue-empty  (enc/queue)
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
   :nested       [#{{1 [:a :b] 2 [:c :d] 3 [:e :f]} [] #{:a :b}}
                  #{{1 [:a :b] 2 [:c :d] 3 [:e :f]} [] #{:a :b}}
                  [1 [1 2 [1 2 3 [1 2 3 4 [1 2 3 4 5]]]]]]

   :lazy-seq       (repeatedly 1000 rand)
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
   :uri          (URI. "https://clojure.org/reference/data_structures")
   :uuid         (java.util.UUID/randomUUID)
   :date         (java.util.Date.)
   :objects      (object-array [1 "two" {:data "data"}])

   :stress-record (StressRecord. "data")
   :stress-type   (StressType. "data")

   ;; Serializable
   :throwable    (Throwable. "Yolo")
   :exception    (try (/ 1 0) (catch Exception e e))
   :ex-info      (ex-info "ExInfo" {:data "data"})})

(def stress-data-comparable
  "Reference data with stuff removed that breaks roundtrip equality"
  (dissoc stress-data :bytes :throwable :exception :ex-info :regex :objects))

(def stress-data-benchable
  "Reference data with stuff removed that breaks reader or other utils we'll
  be benching against"
  (dissoc stress-data
    :bytes :throwable :exception :ex-info :queue :queue-empty
    :byte :stress-record :stress-type :regex :objects))

;;;; Tools

(defn inspect-ba "Alpha - subject to change"
  ([ba          ] (inspect-ba ba nil))
  ([ba thaw-opts]
   (when (enc/bytes? ba)
     (let [[first2bytes nextbytes] (enc/ba-split ba 2)
           ?known-wrapper
           (cond*
             (enc/ba= first2bytes (.getBytes "\u0000<" charset)) :carmine/bin
             (enc/ba= first2bytes (.getBytes "\u0000>" charset)) :carmine/clj)

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

(defn freeze-to-file
  "Convenience util: writes `(freeze x freeze-opts)` byte array to
  `(clojure.java.io/file file)` and returns the byte array.

  (freeze-to-file \"my-filename.npy\" my-val) => Serialized byte array"

  ([file x            ] (freeze-to-file file x nil))
  ([file x freeze-opts]
   (let [^bytes ba (freeze x freeze-opts)]
     (with-open [out (jio/output-stream (jio/file file))]
       (.write out ba))
     ba)))

(defn thaw-from-file
  "Convenience util: returns `(thaw ba thaw-opts)` Clojure value for the
  byte array read from `(clojure.java.io/file file)`.

  (thaw-from-file \"my-filename.npy\") => Deserialized Clojure value

  To thaw from a resource on classpath (e.g in Leiningen `resources` dir):
    (thaw-from-file (clojure.java.io/resource \"my-resource-name.npy\"))"

  ([file          ] (thaw-from-file file nil))
  ([file thaw-opts]
   (let [file (jio/file file),
         ba (byte-array (.length file))]
     (with-open [in (DataInputStream. (jio/input-stream file))]
       (.readFully in ba))

     (thaw ba thaw-opts))))

(comment
  (freeze-to-file "foo.npy" "hello, world!")
  (thaw-from-file "foo.npy")
  (thaw-from-file (jio/resource "foo.npy")))

;;;; Deprecated

(enc/deprecated
  (enc/defonce ^:dynamic *final-freeze-fallback* "DEPRECATED" nil)
  (def freeze-fallback-as-str "DEPRECATED" write-unfreezable))

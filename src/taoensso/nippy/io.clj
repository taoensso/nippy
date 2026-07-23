(ns taoensso.nippy.io
  "Private low-level utils for reading/writing data, don't use."
  (:require
   [taoensso.truss  :as truss]
   [taoensso.encore :as enc]
   [taoensso.nippy
    [impl   :as impl]
    [schema :as sc]])

  (:import
   [taoensso.nippy.impl Cached CacheState]
   [java.nio.charset StandardCharsets]
   [java.nio ByteBuffer BufferOverflowException]
   [java.io
    DataOutput       DataInput
    DataOutputStream DataInputStream
    ByteArrayOutputStream ByteArrayInputStream]))

;;;;

(defn- bb-advance   [^ByteBuffer bb ^long n] (.position bb (+ (.position bb) n)) bb)
(defn- bb-readable! [^ByteBuffer bb ^long n] (when (> n (.remaining bb)) (throw (java.io.EOFException. (str "ByteBuffer underflow: need " n " bytes, have " (.remaining bb) ".")))))
(defn- bb-writable! [^ByteBuffer bb ^long n] (when (> n (.remaining bb)) (throw (BufferOverflowException.))))
(defn  bb-big-endian!
  ^ByteBuffer [^ByteBuffer bb]
  (when-not (= (.order bb) java.nio.ByteOrder/BIG_ENDIAN)
    (throw
      (IllegalArgumentException.
        (str "ByteBuffer must use BIG_ENDIAN order for DataInput/DataOutput semantics (have " (.order bb) ")."))))
  bb)

;;;; Writing

(defprotocol IWriteTypedNoMeta    (write-typed      [_ ^ByteBuffer bb dout_] "Writes given object as type-prefixed bytes. Excludes IObj meta."))
(defprotocol IWriteTypedWithMeta  (write-typed+meta [_ ^ByteBuffer bb dout_] "Writes given object as type-prefixed bytes. Includes IObj meta when present."))
(defprotocol IWriteTypedNoMetaDin (write-typed-din  [_ ^DataOutput    dout ] "Writes given object as type-prefixed bytes. Excludes IObj meta. Takes legacy `DataInput`, used for custom extensions."))

(defmacro write-id        [bb id] `(.put      ~bb (unchecked-byte  ~id)))
(defmacro write-sm-ucount [bb  n] `(.put      ~bb (unchecked-byte (+ ~n Byte/MIN_VALUE)))) ; Unsigned
(defmacro write-sm-count  [bb  n] `(.put      ~bb (unchecked-byte    ~n)))
(defmacro write-md-count  [bb  n] `(.putShort ~bb (unchecked-short   ~n)))
(defmacro write-lg-count  [bb  n] `(.putInt   ~bb (int               ~n)))

(defn write-bytes-sm* [^ByteBuffer bb ^bytes ba] (let [len (alength ba)] (write-sm-ucount bb len) (.put bb ba 0 len))) ; Unsigned
(defn write-bytes-sm  [^ByteBuffer bb ^bytes ba] (let [len (alength ba)] (write-sm-count  bb len) (.put bb ba 0 len)))
(defn write-bytes-md  [^ByteBuffer bb ^bytes ba] (let [len (alength ba)] (write-md-count  bb len) (.put bb ba 0 len)))
(defn write-bytes-lg  [^ByteBuffer bb ^bytes ba] (let [len (alength ba)] (write-lg-count  bb len) (.put bb ba 0 len)))
(defn write-bytes     [^ByteBuffer bb ^bytes ba]
  (let [len (alength ba)]
    (if (zero? len)
      (write-id bb sc/id-byte-array-0)
      (do
        (enc/cond
          (impl/sm-count? len) (do (write-id bb sc/id-byte-array-sm) (write-sm-count bb len))
          (impl/md-count? len) (do (write-id bb sc/id-byte-array-md) (write-md-count bb len))
          :else                (do (write-id bb sc/id-byte-array-lg) (write-lg-count bb len)))

        (.put bb ba 0 len)))))

(defmacro write-dyn-array-lg
  "Writes an array of dynamic (individually type-prefixed) elements."
  [^ByteBuffer bb dout_ arr alen id]
  `(do
     (write-id       ~bb ~id)
     (write-lg-count ~bb ~alen)
     (enc/reduce-n (fn [_# idx#] (write-typed+meta (aget ~arr idx#) ~bb ~dout_)) nil ~alen)))

(defn write-biginteger [^ByteBuffer bb ^BigInteger n] (write-bytes-lg bb (.toByteArray n)))

(defn write-str-sm* [^ByteBuffer bb ^String s] (write-bytes-sm* bb (.getBytes s StandardCharsets/UTF_8)))
(defn write-str-sm  [^ByteBuffer bb ^String s] (write-bytes-sm  bb (.getBytes s StandardCharsets/UTF_8)))
(defn write-str-md  [^ByteBuffer bb ^String s] (write-bytes-md  bb (.getBytes s StandardCharsets/UTF_8)))
(defn write-str-lg  [^ByteBuffer bb ^String s] (write-bytes-lg  bb (.getBytes s StandardCharsets/UTF_8)))
(defn write-str     [^ByteBuffer bb ^String s]
  (if (identical? s "")
    (write-id bb sc/id-str-0)
    (let [ba  (.getBytes s StandardCharsets/UTF_8)
          len (alength ba)]
      (enc/cond
        (when     impl/pack-unsigned? (impl/sm-ucount? len)) (do (write-id bb sc/id-str-sm*) (write-sm-ucount bb len))
        (when-not impl/pack-unsigned? (impl/sm-count?  len)) (do (write-id bb sc/id-str-sm_) (write-sm-count  bb len))
                                      (impl/md-count?  len)  (do (write-id bb sc/id-str-md)  (write-md-count  bb len))
        :else                                                (do (write-id bb sc/id-str-lg)  (write-lg-count  bb len)))
      (.put bb ba 0 len))))

(defn write-kw [^ByteBuffer bb kw]
  (let [s   (if-let [ns (namespace kw)] (str ns "/" (name kw)) (name kw))
        ba  (.getBytes s StandardCharsets/UTF_8)
        len (alength ba)]
    (enc/cond
      (impl/sm-count? len) (do (write-id bb sc/id-kw-sm) (write-sm-count bb len))
      (impl/md-count? len) (do (write-id bb sc/id-kw-md) (write-md-count bb len))
      :else                (truss/ex-info! "Keyword too long" {:name s}))
    (.put bb ba 0 len)))

(defn write-sym [^ByteBuffer bb s]
  (let [s   (if-let [ns (namespace s)] (str ns "/" (name s)) (name s))
        ba  (.getBytes s StandardCharsets/UTF_8)
        len (alength ba)]
    (enc/cond
      (impl/sm-count? len) (do (write-id bb sc/id-sym-sm) (write-sm-count bb len))
      (impl/md-count? len) (do (write-id bb sc/id-sym-md) (write-md-count bb len))
      :else                (truss/ex-info! "Symbol too long" {:name s}))
    (.put bb ba 0 len)))

(defn write-long-legacy [^ByteBuffer bb ^long n]
  (enc/cond
    (zero? n) (write-id bb sc/id-long-0)
    (pos?  n)
    (enc/cond
      (<= n    Byte/MAX_VALUE) (do (write-id bb sc/id-long-sm_) (.put      bb (unchecked-byte  n)))
      (<= n   Short/MAX_VALUE) (do (write-id bb sc/id-long-md_) (.putShort bb (unchecked-short n)))
      (<= n Integer/MAX_VALUE) (do (write-id bb sc/id-long-lg_) (.putInt   bb (int n)))
      :else                    (do (write-id bb sc/id-long-xl)  (.putLong  bb      n)))

    :else
    (enc/cond
      (>= n    Byte/MIN_VALUE) (do (write-id bb sc/id-long-sm_) (.put      bb (unchecked-byte  n)))
      (>= n   Short/MIN_VALUE) (do (write-id bb sc/id-long-md_) (.putShort bb (unchecked-short n)))
      (>= n Integer/MIN_VALUE) (do (write-id bb sc/id-long-lg_) (.putInt   bb (int n)))
      :else                    (do (write-id bb sc/id-long-xl)  (.putLong  bb      n)))))

(defn write-long [^ByteBuffer bb ^long n]
  (enc/cond
    (not impl/pack-unsigned?) (write-long-legacy bb n)
    (zero? n)                 (write-id          bb sc/id-long-0)
    (pos?  n)
    (enc/cond
      (<= n impl/range-ubyte)  (do (write-id bb sc/id-long-pos-sm) (.put      bb (unchecked-byte  (+ n    Byte/MIN_VALUE))))
      (<= n impl/range-ushort) (do (write-id bb sc/id-long-pos-md) (.putShort bb (unchecked-short (+ n   Short/MIN_VALUE))))
      (<= n impl/range-uint)   (do (write-id bb sc/id-long-pos-lg) (.putInt   bb (int             (+ n Integer/MIN_VALUE))))
      :else                    (do (write-id bb sc/id-long-xl)     (.putLong  bb                     n)))

    :else
    (let [y (- n)]
      (enc/cond
        (<= y impl/range-ubyte)  (do (write-id bb sc/id-long-neg-sm) (.put      bb (unchecked-byte  (+ y    Byte/MIN_VALUE))))
        (<= y impl/range-ushort) (do (write-id bb sc/id-long-neg-md) (.putShort bb (unchecked-short (+ y   Short/MIN_VALUE))))
        (<= y impl/range-uint)   (do (write-id bb sc/id-long-neg-lg) (.putInt   bb (int             (+ y Integer/MIN_VALUE))))
        :else                    (do (write-id bb sc/id-long-xl)     (.putLong  bb                     n))))))

;; Coll headers (id + count) are written by BOTH the buffered writers below and
;; the streaming writer (see `write-typed+meta-to-out!`). They're macros so that
;; there's a single source of truth while the buffered writers keep emitting
;; the header logic inline, i.e. without adding a call on the hot path.

(defmacro write-coll-header*
  "Writes coll id + count, packing sm counts as unsigned when enabled."
  [bb id-0 id-sm* id-sm_ id-md id-lg cnt]
  `(let [cnt# ~cnt]
     (enc/cond
       (do                           (zero?           cnt#)) (do (write-id ~bb ~id-0))
       (when     impl/pack-unsigned? (impl/sm-ucount? cnt#)) (do (write-id ~bb ~id-sm*) (write-sm-ucount ~bb cnt#))
       (when-not impl/pack-unsigned? (impl/sm-count?  cnt#)) (do (write-id ~bb ~id-sm_) (write-sm-count  ~bb cnt#))
                                     (impl/md-count?  cnt#)  (do (write-id ~bb ~id-md)  (write-md-count  ~bb cnt#))
       :else                                                 (do (write-id ~bb ~id-lg)  (write-lg-count  ~bb cnt#)))))

(defmacro write-coll-header
  "Writes coll id + count."
  [bb id-0 id-sm id-md id-lg cnt]
  `(let [cnt# ~cnt]
     (enc/cond
       (zero?          cnt#) (do (write-id ~bb ~id-0))
       (impl/sm-count? cnt#) (do (write-id ~bb ~id-sm) (write-sm-count ~bb cnt#))
       (impl/md-count? cnt#) (do (write-id ~bb ~id-md) (write-md-count ~bb cnt#))
       :else                 (do (write-id ~bb ~id-lg) (write-lg-count ~bb cnt#)))))

(defn write-vec [^ByteBuffer bb dout_ v]
  (let [cnt (count v)]
    (write-coll-header* bb sc/id-vec-0 sc/id-vec-sm* sc/id-vec-sm_ sc/id-vec-md sc/id-vec-lg cnt)
    (when-not (zero? cnt)
      (run! (fn [el] (write-typed+meta el bb dout_)) v))))

(defn write-kvs
  ([^ByteBuffer bb dout_ id-lg coll]
   (let [cnt (count coll)]
     (write-id       bb id-lg)
     (write-lg-count bb cnt)
     (enc/run-kv!
       (fn [k v]
         (write-typed+meta k bb dout_)
         (write-typed+meta v bb dout_))
       coll)))

  ([^ByteBuffer bb dout_ id-empty id-sm id-md id-lg coll]
   (let [cnt (count coll)]
     (write-coll-header bb id-empty id-sm id-md id-lg cnt)
     (when-not (zero? cnt)
       (enc/run-kv!
         (fn [k v]
           (write-typed+meta k bb dout_)
           (write-typed+meta v bb dout_))
         coll)))))

(defn write-counted-coll
  ([^ByteBuffer bb dout_ id-lg coll]
   (let [cnt (count coll)]
     (write-id       bb id-lg)
     (write-lg-count bb cnt)
     (reduce (fn [_ in] (write-typed+meta in bb dout_)) nil coll)))

  ([^ByteBuffer bb dout_ id-empty id-sm id-md id-lg coll]
   (let [cnt (count coll)]
     (write-coll-header bb id-empty id-sm id-md id-lg cnt)
     (when-not (zero? cnt)
       (reduce (fn [_ in] (write-typed+meta in bb dout_)) nil coll)))))

(defn write-uncounted-coll
  ([^ByteBuffer bb dout_ id-empty id-sm id-md id-lg coll] (write-counted-coll bb dout_ id-empty id-sm id-md id-lg coll)) ; Extra O(n) count
  ([^ByteBuffer bb dout_                      id-lg coll]
   ;; (assert (not (counted? coll)))
   (write-id bb id-lg)
   (let [cnt-idx   (.position bb)
         _         (.putInt   bb 0) ; lg-count placeholder
         ^long cnt (reduce (fn [^long cnt in] (write-typed+meta in bb dout_) (unchecked-inc cnt)) 0 coll)]
     (.putInt bb cnt-idx cnt))))

(defn write-coll
  ([^ByteBuffer bb dout_ id-lg coll]
   (if (counted? coll)
     (write-counted-coll   bb dout_ id-lg coll)
     (write-uncounted-coll bb dout_ id-lg coll)))

  ([^ByteBuffer bb dout_ id-empty id-sm id-md id-lg coll]
   (if (counted? coll)
     (write-counted-coll   bb dout_   id-empty   id-sm   id-md id-lg coll)
     (write-uncounted-coll bb dout_ #_id-empty #_id-sm #_id-md id-lg coll))))

(defn write-map
  "Micro-optimized `write-kvs` w/ id-map-0 id-map-sm id-map-md id-map-lg."
  [^ByteBuffer bb dout_ m is-metadata?]
  (let [cnt (count m)]
    (write-coll-header* bb sc/id-map-0 sc/id-map-sm* sc/id-map-sm_ sc/id-map-md sc/id-map-lg cnt)
    (when-not (zero? cnt)
      (reduce-kv
          (fn [_ k v]
            (if (enc/and? is-metadata? (fn? v) (qualified-symbol? k))
              (do
                (if (impl/target-release>= 340)
                  (write-id bb sc/id-meta-protocol-key)
                  (write-typed  impl/meta-protocol-key bb dout_))
                (write-id bb sc/id-nil))
              (do
                (write-typed+meta k bb dout_)
                (write-typed+meta v bb dout_))))
        nil
        m))))

(defn write-set
  "Micro-optimized `write-counted-coll` w/ id-set-0 id-set-sm id-set-md id-set-lg."
  [^ByteBuffer bb dout_ s]
  (let [cnt (count s)]
    (write-coll-header* bb sc/id-set-0 sc/id-set-sm* sc/id-set-sm_ sc/id-set-md sc/id-set-lg cnt)
    (when-not (zero? cnt)
      (reduce (fn [_ in] (write-typed+meta in bb dout_)) nil s))))

(defn write-sz
  "Writes given arg using Java `Serializable`.
  Returns true iff allowed."
  [^ByteBuffer bb x]
  (impl/when-debug (println (str "write-sz: " (type x))))
  (when (and (instance? java.io.Serializable x) (not (fn? x)))
    (let [class-name (.getName (class x))] ; Reflect
      (when (impl/freeze-serializable-allowed? class-name)
        (let [class-name-ba (.getBytes class-name StandardCharsets/UTF_8)
              len           (alength   class-name-ba)

              ;; Unrealistic, there's no `id-sz-lg`:
              _ (when-not (impl/md-count? len)
                  (truss/ex-info! "Serializable class name too long" {:name class-name}))

              ;; Serialize object to isolated ba, then write the length-prefixed ba to stream.
              ;; Can therefore later choose to skip OR deserialize with `readObject`.
              ;;
              ;; Note that we serialize BEFORE touching `bb`: `writeObject` often throws
              ;; (e.g. `NotSerializableException` on a nested field), and callers may then
              ;; try another writer - so we mustn't leave orphan header bytes behind.
              sz-ba
              (let [baos (ByteArrayOutputStream.)
                    dos  (DataOutputStream. baos)]
                (.writeObject (java.io.ObjectOutputStream. dos) x)
                (.toByteArray baos))]

          (if (impl/sm-count? len)
            (do (write-id bb sc/id-sz-sm) (write-bytes-sm bb class-name-ba))
            (do (write-id bb sc/id-sz-md) (write-bytes-md bb class-name-ba)))

          (write-bytes bb sz-ba)
          true)))))

(defn write-readable [^ByteBuffer bb x]
  (impl/when-debug (println (str "write-readable: " (type x))))
  (when (impl/seems-readable? x)
    (let [edn    (enc/pr-edn  x)
          edn-ba (.getBytes ^String edn StandardCharsets/UTF_8)
          len    (alength edn-ba)]
      (enc/cond
        (impl/sm-count? len) (do (write-id bb sc/id-reader-sm) (write-bytes-sm bb edn-ba))
        (impl/md-count? len) (do (write-id bb sc/id-reader-md) (write-bytes-md bb edn-ba))
        :else                (do (write-id bb sc/id-reader-lg) (write-bytes-lg bb edn-ba)))
      true)))

(defn write-cached-ref
  "Writes cache ref id (+ idx payload when idx > 7) for given idx <= 32767."
  [^ByteBuffer bb ^long idx]
  (if (impl/sm-count? idx)
    (case (int idx)
      0 (do (bb-writable! bb 1) (write-id bb sc/id-cached-0))
      1 (do (bb-writable! bb 1) (write-id bb sc/id-cached-1))
      2 (do (bb-writable! bb 1) (write-id bb sc/id-cached-2))
      3 (do (bb-writable! bb 1) (write-id bb sc/id-cached-3))
      4 (do (bb-writable! bb 1) (write-id bb sc/id-cached-4))
      5 (do (bb-writable! bb 1) (write-id bb sc/id-cached-5))
      6 (do (bb-writable! bb 1) (write-id bb sc/id-cached-6))
      7 (do (bb-writable! bb 1) (write-id bb sc/id-cached-7))
      (do (bb-writable! bb 2) (write-id bb sc/id-cached-sm) (write-sm-count bb idx)))
    (if (impl/md-count? idx)
      (do (bb-writable! bb 3) (write-id bb sc/id-cached-md) (write-md-count bb idx))
      (truss/ex-info! "Cache ref idx out of range" {:idx idx}))))

(defn write-cached-header!
  "Registers `x-val` in the cache and writes its cache ref id.
  Returns true iff `x-val` itself must still be written after the id.

  Shared by the buffered and streaming writers, so both agree on both the
  emitted bytes and the cache idxs they imply. NB the cache mutation happens
  here, alongside the id write, so a caller that discards the written bytes
  (see `stream-leaf!`) can undo both together."
  [^ByteBuffer bb x-val ^CacheState state]
  (let [^java.util.HashMap m (.-freeze-idxs state)
        k    [x-val (meta x-val)] ; Also check meta for equality
        ?idx (.get m k)]

    (if-let [idx ?idx]
      (do (write-cached-ref bb idx) false) ; Ref to previously written value
      (let [idx (.size m)]
        (when (impl/md-count? idx) ; Else cache full: no id, just freeze uncached
          (.put m k idx)
          (write-cached-ref bb idx))
        true))))

(defn write-cached [^ByteBuffer bb dout_ x-val ^CacheState state]
  (when (write-cached-header! bb x-val state)
    (write-typed+meta x-val bb dout_)))

;;;;

(enc/declare-remote ^:dynamic taoensso.nippy/*incl-metadata?*)

(extend-protocol IWriteTypedWithMeta
  clojure.lang.IObj ; IMeta => `meta` will work, IObj => `with-meta` will work
  (write-typed+meta [x ^ByteBuffer bb dout_]
    (when-let [m (when taoensso.nippy/*incl-metadata?* (not-empty (meta x)))]
      (write-id  bb sc/id-meta)
      (write-map bb dout_ m :is-metadata))
    (write-typed x bb dout_))

  nil    (write-typed+meta [x bb dout_] (write-typed x bb dout_))
  Object (write-typed+meta [x bb dout_] (write-typed x bb dout_)))

(defmacro ^:private writer
  "Convenience util / short-hand."
  [atype id & impl-body]
  (let [aclass     (if (string? atype) (Class/forName atype) atype)
        x          (with-meta 'x    {:tag atype})
        bb         (with-meta 'bb   {:tag 'ByteBuffer})
        id-form    (when id `(write-id ~'bb ~id))
        freezable? (if (= atype 'Object) nil true)]

    `(extend ~aclass
       impl/INativeFreezable {:native-freezable? (fn [~x            ] ~freezable?) }
       IWriteTypedNoMeta     {:write-typed       (fn [~x ~bb ~'dout_] ~id-form ~@impl-body)})))

(writer nil       sc/id-nil    nil)
(writer (type ()) sc/id-list-0 nil)

(writer Boolean              nil (if (.booleanValue x) (write-id bb sc/id-true) (write-id bb sc/id-false)))
(writer String               nil (write-str bb x))
(writer clojure.lang.Keyword nil (write-kw  bb x))
(writer clojure.lang.Symbol  nil (write-sym bb x))

(writer Character sc/id-char    (.putChar   bb (unchecked-char (int x))))
(writer Byte      sc/id-byte    (.put       bb (unchecked-byte      x)))
(writer Short     sc/id-short   (.putShort  bb (unchecked-short     x)))
(writer Integer   sc/id-integer (.putInt    bb                 (int x)))
(writer Float     sc/id-float   (.putFloat  bb                      x))
(writer Long      nil           (write-long bb                      x))
(writer Double    nil
  (if (zero? ^double x)
    (do (write-id bb sc/id-double-0))
    (do (write-id bb sc/id-double) (.putDouble bb x))))

(writer BigInteger sc/id-biginteger (write-biginteger bb x))
(writer BigDecimal sc/id-bigdec
  (write-biginteger bb (.unscaledValue x))
  (.putInt          bb (.scale         x)))

(writer clojure.lang.BigInt sc/id-bigint (write-biginteger bb (.toBigInteger x)))
(writer clojure.lang.Ratio  sc/id-ratio
  (write-biginteger bb (.numerator   x))
  (write-biginteger bb (.denominator x)))

(writer java.util.Date          sc/id-util-date (.putLong  bb (.getTime  x)))
(writer java.sql.Date           sc/id-sql-date  (.putLong  bb (.getTime  x)))
(writer java.net.URI            sc/id-uri       (write-str bb (.toString x)))
(writer java.util.regex.Pattern sc/id-regex     (write-str bb (.toString x)))
(writer java.util.UUID          sc/id-uuid
  (.putLong bb (.getMostSignificantBits  x))
  (.putLong bb (.getLeastSignificantBits x)))

(writer Cached nil
  (let [x-val (.-val x)]
    (enc/cond
      (nil? x-val) (write-id bb sc/id-nil) ; Nothing to gain by caching nil

      :if-let [state (.get impl/tl:cache)]
      (write-cached bb dout_ x-val state)

      :else (write-typed+meta x-val bb dout_))))

(writer "[B"                    nil (write-bytes        bb x))
(writer "[Ljava.lang.Object;"   nil (write-dyn-array-lg bb dout_ x (alength x) sc/id-object-array-lg))

(cond ; String arrays
  (impl/target-release>= 350)
  (writer "[Ljava.lang.String;" nil (write-dyn-array-lg bb dout_ x (alength x) sc/id-string-array-lg))
  :else nil ; Handle via Serializable
  )

(cond ; Numeric arrays
  (impl/target-release>= 370)
  (do
    (writer "[I"    sc/id-int-array-lg (let [alen (alength x)] (.putInt bb alen) (.put (.asIntBuffer    bb) x) (bb-advance bb (* alen Integer/BYTES))))
    (writer "[J"   sc/id-long-array-lg (let [alen (alength x)] (.putInt bb alen) (.put (.asLongBuffer   bb) x) (bb-advance bb (* alen    Long/BYTES))))
    (writer "[F"  sc/id-float-array-lg (let [alen (alength x)] (.putInt bb alen) (.put (.asFloatBuffer  bb) x) (bb-advance bb (* alen   Float/BYTES))))
    (writer "[D" sc/id-double-array-lg (let [alen (alength x)] (.putInt bb alen) (.put (.asDoubleBuffer bb) x) (bb-advance bb (* alen  Double/BYTES)))))

  (impl/target-release>= 350)
  (do
    (writer "[I" nil (write-dyn-array-lg bb dout_ x (alength x)    sc/id-int-array-lg_))
    (writer "[J" nil (write-dyn-array-lg bb dout_ x (alength x)   sc/id-long-array-lg_))
    (writer "[F" nil (write-dyn-array-lg bb dout_ x (alength x)  sc/id-float-array-lg_))
    (writer "[D" nil (write-dyn-array-lg bb dout_ x (alength x) sc/id-double-array-lg_)))

  :else nil ; Handle via Serializable
  )

(writer clojure.lang.MapEntry sc/id-map-entry
  (write-typed+meta (key x) bb dout_)
  (write-typed+meta (val x) bb dout_))

(writer clojure.lang.PersistentQueue    nil (write-counted-coll   bb dout_ sc/id-queue-lg      x))
(writer clojure.lang.PersistentTreeSet  nil (write-counted-coll   bb dout_ sc/id-sorted-set-lg x))
(writer clojure.lang.PersistentTreeMap  nil (write-kvs            bb dout_ sc/id-sorted-map-lg x))
(writer clojure.lang.APersistentVector  nil (write-vec            bb dout_                     x))
(writer clojure.lang.APersistentSet     nil (write-set            bb dout_                     x))
(writer clojure.lang.APersistentMap     nil (write-map            bb dout_                     x false))
(writer clojure.lang.PersistentList     nil (write-counted-coll   bb dout_  sc/id-list-0   sc/id-list-sm   sc/id-list-md sc/id-list-lg x))
(writer clojure.lang.LazySeq            nil (write-uncounted-coll bb dout_ #_sc/id-seq-0  #_sc/id-seq-sm  #_sc/id-seq-md  sc/id-seq-lg x))
(writer clojure.lang.ISeq               nil (write-coll           bb dout_   sc/id-seq-0    sc/id-seq-sm    sc/id-seq-md  sc/id-seq-lg x))
(writer clojure.lang.IRecord            nil
  (if (impl/custom-freezable? x)
    (write-typed-din x (dout_))
    (let [class-name    (.getName (class x)) ; Reflect
          class-name-ba (.getBytes class-name StandardCharsets/UTF_8)
          len           (alength   class-name-ba)]
      (enc/cond
        (impl/sm-count? len) (do (write-id bb sc/id-record-sm) (write-bytes-sm bb class-name-ba))
        (impl/md-count? len) (do (write-id bb sc/id-record-md) (write-bytes-md bb class-name-ba))
        ;; :else             (do (write-id bb sc/id-record-lg) (write-bytes-md bb class-name-ba)) ; Unrealistic
        :else                (truss/ex-info! "Record class name too long" {:name class-name}))

      (write-typed (into {} x) bb dout_))))

(writer clojure.lang.IType nil
  (if (impl/custom-freezable? x)
    (write-typed-din x (dout_))
    (let [c      (class x)
          fields (impl/get-basis-fields c)
          wf     (fn [^java.lang.reflect.Field f] (write-typed (.get f x) bb dout_))]
      (if (impl/target-release>= 370)
        (do ; With field count prefix
          (write-id        bb sc/id-deftype)
          (write-str       bb (.getName c))
          (write-sm-ucount bb (count fields))
          (run! wf fields))
        (do ; Without field count prefix
          (write-id  bb sc/id-deftype_)
          (write-str bb (.getName c))
          (run! wf fields))))))

(enc/compile-if java.time.Instant
  (writer       java.time.Instant sc/id-time-instant
    (.putLong bb (.getEpochSecond x))
    (.putInt  bb (.getNano        x))))

(enc/compile-if java.time.Duration
  (writer       java.time.Duration sc/id-time-duration
    (.putLong bb (.getSeconds x))
    (.putInt  bb (.getNano    x))))

(enc/compile-if java.time.Period
  (writer       java.time.Period sc/id-time-period
    (.putInt bb (.getYears  x))
    (.putInt bb (.getMonths x))
    (.putInt bb (.getDays   x))))

(enc/declare-remote
  ^:dynamic taoensso.nippy/*freeze-fallback*
  ^:dynamic taoensso.nippy/*final-freeze-fallback*)

(writer Object nil
  (impl/when-debug (println (str "freeze-fallback: " (type x))))
  (enc/cond
    (impl/custom-freezable? x)
    (write-typed-din x (dout_))

    :if-let [ff taoensso.nippy/*freeze-fallback*]
    (if-not (identical? ff :write-unfreezable)
      (ff (dout_) x) ; Modern approach with ff
      (or            ; Legacy approach with ff
        (try (write-sz       bb x) (catch BufferOverflowException e (throw e)) (catch Throwable _ nil))
        (try (write-readable bb x) (catch BufferOverflowException e (throw e)) (catch Throwable _ nil))
        (write-typed (impl/wrap-unfreezable x) bb dout_)))

    ;; Without ff
    :let [[r1 e1] (try [(write-sz       bb x)] (catch BufferOverflowException e (throw e)) (catch Throwable t [nil t]))], r1 r1
    :let [[r2 e2] (try [(write-readable bb x)] (catch BufferOverflowException e (throw e)) (catch Throwable t [nil t]))], r2 r2
    :if-let [fff taoensso.nippy/*final-freeze-fallback*] (fff (dout_) x) ; Deprecated
    :else
    (let [t (type x)]
      (truss/ex-info! (str "Failed to freeze type: " t)
        (enc/assoc-some
          {:type   t
           :as-str (impl/try-pr-edn x)}
          {:serializable-error e1
           :readable-error     e2})
        (or e1 e2)))))

;;;; Reading

(declare read-typed) ; Main read fn, type determined by prefix

(definterface IByteReader
  ;; Common read interface implemented by both `DataInput` and `ByteBuffer`.
  ;; Uses `definterface` rather than `defprotocol` so the Clojure compiler
  ;; can emit typed invokevirtual/invokeinterface bytecode when the parameter
  ;; has ^IByteReader hint, letting the JIT devirtualize the hot read path.
  (^byte   readByte   [])
  (^short  readShort  [])
  (^int    readInt    [])
  (^long   readLong   [])
  (^float  readFloat  [])
  (^double readDouble [])
  (^char   readChar   [])
  (readFully     [^bytes ba ^int off ^int len])
  (skipBytes     [^int n])
  (toDataInput   [])
  (toInputStream [])
  (toByteBuffer  []))

(declare bb->din)

(deftype ByteBufferReader [^ByteBuffer bb]
  IByteReader
  (readByte      [_] (.get       bb))
  (readShort     [_] (.getShort  bb))
  (readInt       [_] (.getInt    bb))
  (readLong      [_] (.getLong   bb))
  (readFloat     [_] (.getFloat  bb))
  (readDouble    [_] (.getDouble bb))
  (readChar      [_] (.getChar   bb))
  (readFully     [_ ^bytes ba ^int off ^int len] (.get bb ba off len))
  (skipBytes     [_ ^int n] (bb-advance bb n))
  (toDataInput   [_] (bb->din bb))
  (toByteBuffer  [_]          bb)
  (toInputStream [_]
    (proxy [java.io.InputStream] []
      (available [] (.remaining bb))
      (skip [n]
        (let [n (long (min (max (long n) 0) (.remaining bb)))]
          (bb-advance bb n)
          n))

      (read
        ([         ] (if (.hasRemaining bb) (bit-and 0xff (int (.get bb))) -1))
        ([^bytes ba]
         (let [len (alength ba)]
           (cond
             (zero? len)               0
             (not (.hasRemaining bb)) -1
             :else
             (let [n (min len (.remaining bb))]
               (.get bb ba 0 n)
               n))))

        ([^bytes ba off len]
         (let [ba-len (alength ba)]
           (cond
             (or (neg? off) (neg? len) (> off ba-len) (> len (- ba-len off)))
             (throw (IndexOutOfBoundsException.))

             (zero? len)               0
             (not (.hasRemaining bb)) -1
             :else
             (let [n (min len (.remaining bb))]
               (.get bb ba off n)
               n))))))))

(deftype DataInputReader [^DataInput din]
  IByteReader
  (readByte      [_] (.readByte   din))
  (readShort     [_] (.readShort  din))
  (readInt       [_] (.readInt    din))
  (readLong      [_] (.readLong   din))
  (readFloat     [_] (.readFloat  din))
  (readDouble    [_] (.readDouble din))
  (readChar      [_] (.readChar   din))
  (readFully     [_ ^bytes ba ^int off ^int len] (.readFully din ba off len))
  (skipBytes     [_ ^int n] (.skipBytes din n))
  (toDataInput   [_] din)
  (toInputStream [_] din)
  (toByteBuffer  [_] (throw (IllegalArgumentException. (str "DataInputReader cannot be used as ByteBuffer")))))

(defmacro read-sm-ucount [ibr] `(- (int (.readByte  ~ibr)) Byte/MIN_VALUE))
(defmacro read-sm-count  [ibr]    `(int (.readByte  ~ibr)))
(defmacro read-md-count  [ibr]    `(int (.readShort ~ibr)))
(defmacro read-lg-count  [ibr]         `(.readInt   ~ibr))

(defn- ensure-readable-length!
  "Returns given length `n`, or throws `EOFException` if `n` items obviously
  cannot be read from `ibr`. Used to reject malformed length prefixes BEFORE
  they're used to size an allocation.

  `bytes-per-item` is a lower bound on each item's SERIALIZED size (1 for
  type-prefixed items, which always cost >= their type prefix).

  Note that only buffered input has a known remaining length. Stream input
  can be checked only for negative lengths, so a forged positive length may
  still trigger a large allocation there before the underlying stream is
  found to be short.

  Bounding stream input too was considered and intentionally rejected: it'd
  cost extra copying on large reads, and wouldn't make thaw resource-safe
  against hostile input anyway since decompression allocates upstream of
  this check. See commit message for details."
  [^IByteReader ibr ^long n ^long bytes-per-item]
  (when (neg? n)
    (throw (java.io.EOFException. (str "Negative length: " n "."))))
  (when (instance? ByteBufferReader ibr)
    (let [required (* n bytes-per-item)
          remaining (.remaining ^ByteBuffer (.toByteBuffer ibr))]
      (when (> required remaining)
        (throw
          (java.io.EOFException.
            (str "ByteBuffer underflow: need at least " required
              " bytes, have " remaining "."))))))
  n)

(defn- ensure-non-negative-count!
  "Returns given count `n`, or throws `EOFException` if `n` is negative.

  Used on paths that build their result incrementally, so unlike
  `ensure-readable-length!` there's no allocation to guard here. The risk is
  instead that `enc/reduce-n` treats a negative count as zero, so a damaged
  count prefix would otherwise quietly yield an EMPTY coll rather than an error."
  ^long [^long n]
  (if (neg? n)
    (throw (java.io.EOFException. (str "Negative count: " n ".")))
    n))

(declare read-bytes)
(defn    read-bytes-sm* [^IByteReader ibr] (read-bytes ibr (read-sm-ucount ibr)))
(defn    read-bytes-sm  [^IByteReader ibr] (read-bytes ibr (read-sm-count  ibr)))
(defn    read-bytes-md  [^IByteReader ibr] (read-bytes ibr (read-md-count  ibr)))
(defn    read-bytes-lg  [^IByteReader ibr] (read-bytes ibr (read-lg-count  ibr)))
(defn    read-bytes
  ([^IByteReader ibr len]
   (let [len (int (ensure-readable-length! ibr len 1))
         ba  (byte-array  len)]
     (.readFully ibr ba 0 len)
     ba))

  ([^IByteReader ibr]
   (enc/case-eval (int (.readByte ibr))
     sc/id-byte-array-0  (byte-array 0)
     sc/id-byte-array-sm (read-bytes ibr (read-sm-count ibr))
     sc/id-byte-array-md (read-bytes ibr (read-md-count ibr))
     sc/id-byte-array-lg (read-bytes ibr (read-lg-count ibr)))))

(defn read-str-sm* [^IByteReader ibr] (String. ^bytes (read-bytes ibr (read-sm-ucount ibr)) StandardCharsets/UTF_8))
(defn read-str-sm  [^IByteReader ibr] (String. ^bytes (read-bytes ibr (read-sm-count  ibr)) StandardCharsets/UTF_8))
(defn read-str-md  [^IByteReader ibr] (String. ^bytes (read-bytes ibr (read-md-count  ibr)) StandardCharsets/UTF_8))
(defn read-str-lg  [^IByteReader ibr] (String. ^bytes (read-bytes ibr (read-lg-count  ibr)) StandardCharsets/UTF_8))
(defn read-str
  ([^IByteReader ibr len]
   (let [len (int len)]
     (if (instance? ByteBufferReader ibr)
       (let [^ByteBuffer bb (.toByteBuffer ibr)]
         (if (and (.hasArray bb) (<= 0 len (.remaining bb)))
           (let [pos (.position bb)
                 s   (String. ^bytes (.array bb) (+ (.arrayOffset bb) pos) len StandardCharsets/UTF_8)]
             (bb-advance bb len)
             s)
           (String. ^bytes (read-bytes ibr len) StandardCharsets/UTF_8)))
       (String. ^bytes (read-bytes ibr len) StandardCharsets/UTF_8))))

  ([^IByteReader ibr]
   (enc/case-eval (int (.readByte ibr))
     sc/id-str-0   ""
     sc/id-str-sm* (read-str ibr (read-sm-ucount ibr))
     sc/id-str-sm_ (read-str ibr (read-sm-count  ibr))
     sc/id-str-md  (read-str ibr (read-md-count  ibr))
     sc/id-str-lg  (read-str ibr (read-lg-count  ibr)))))

(defn read-biginteger [^IByteReader ibr] (BigInteger. ^bytes (read-bytes ibr (.readInt ibr))))

(defmacro ^:private read-dyn-array
  "Reads an array of individually type-prefixed elements."
  [ibr thaw-type array-type array]
  (let [thawed-sym (with-meta 'thawed-sym {:tag thaw-type})
        array-sym  (with-meta 'array-sym  {:tag array-type})]
    `(let [~array-sym ~array]
       (enc/reduce-n
         (fn [_# idx#]
           (let [~thawed-sym (read-typed ~ibr)]
             (aset ~'array-sym idx# ~'thawed-sym)))
         nil (alength ~'array-sym))
       ~'array-sym)))

(defmacro ^:private read-prim-array
  "Reads a primitive array of homogeneous elements."
  [ibr array-type array-fn as-buffer bytes read-el]
  (let [array-sym (with-meta 'array-sym {:tag array-type})]
    `(let [alen#      (read-lg-count ~ibr)
           _#         (ensure-readable-length! ~ibr alen# ~bytes)
           ~array-sym (~array-fn alen#)]
       (if (instance? ByteBufferReader ~ibr) ; Fast bulk read
         (let [^ByteBuffer bb# (.toByteBuffer ~ibr)]
           (.get (~as-buffer bb#) ~'array-sym)
           (bb-advance bb# (* alen# ~bytes)))
         (enc/reduce-n ; Element-wise read for legacy `DataInput` support
           (fn [_# idx#] (aset ~'array-sym idx# (~read-el ~ibr)))
           nil alen#))
       ~'array-sym)))

(enc/declare-remote ^:dynamic taoensso.nippy/*thaw-xform*)

(let [rf! (fn rf! ([x] (persistent! x)) ([acc x] (conj! acc x)))
      rf* (fn rf* ([x]              x)  ([acc x] (conj  acc x)))]

  (defn read-into [to ^IByteReader ibr ^long n]
    (let [n          (ensure-non-negative-count! n)
          transient? (when (impl/editable? to) (> n 10))
          init       (if transient? (transient to) to)
          rf         (if transient? rf! rf*)
          rf         (if-let [xf taoensso.nippy/*thaw-xform*] ((impl/xform* xf) rf) rf)]
      (rf (enc/reduce-n (fn [acc _] (rf acc (read-typed ibr))) init n)))))

(defn read-vec [^IByteReader ibr ^long n]
  (if (or taoensso.nippy/*thaw-xform* (> n 32))
    (read-into [] ibr n) ; Checks count
    (let [items (object-array (ensure-non-negative-count! n))]
      (loop [idx 0]
        (when (< idx n)
          (aset items idx (read-typed ibr))
          (recur (unchecked-inc-int idx))))
      (clojure.lang.LazilyPersistentVector/createOwning items))))

(let [rf1! (fn rf1! ([x] (persistent! x)) ([acc kv ] (assoc! acc (key kv) (val kv))))
      rf2! (fn rf2! ([x] (persistent! x)) ([acc k v] (assoc! acc      k         v)))
      rf1* (fn rf1* ([x]              x)  ([acc kv ] (assoc  acc (key kv) (val kv))))
      rf2* (fn rf2* ([x]              x)  ([acc k v] (assoc  acc      k         v)))]

  (defn read-kvs-into [to ^IByteReader ibr ^long n]
    (let [n          (ensure-non-negative-count! n)
          transient? (when (impl/editable? to) (> n 10))
          init       (if transient? (transient to) to)
          rf1        (if transient? rf1! rf1*)
          rf2        (if transient? rf2! rf2*)]

      (if-let [xf taoensso.nippy/*thaw-xform*]
        (let [rf ((impl/xform* xf) rf1)] (rf (enc/reduce-n (fn [acc _] (rf acc (enc/map-entry (read-typed ibr) (read-typed ibr)))) init n)))
        (let [rf                   rf2 ] (rf (enc/reduce-n (fn [acc _] (rf acc                (read-typed ibr) (read-typed ibr)))  init n)))))))

(defn- read-kvs-via-rt-map [^IByteReader ibr ^long n]
  (let [kvs (object-array (* 2 n))]
    (enc/reduce-n
      (fn [_ idx]
        (let [offset (* 2 idx)]
          (aset kvs      offset  (read-typed ibr))
          (aset kvs (inc offset) (read-typed ibr))))
      nil n)

    (try
      (clojure.lang.RT/map kvs)
      (catch IllegalArgumentException _ ; Duplicate thawed keys: preserve assoc semantics
        (persistent!
          (enc/reduce-n
            (fn [m idx]
              (let [offset (* 2 idx)]
                (assoc! m (aget kvs offset) (aget kvs (inc offset)))))
            (transient {}) n))))))

(defn read-map [^IByteReader ibr ^long n]
  ;; Bound buffering to Clojure v1.13's maximum possible PAM size. The existing
  ;; paths remain faster for tiny maps and avoid temporary arrays for large maps.
  (if (or taoensso.nippy/*thaw-xform* (<= n 10) (> n 64))
    (read-kvs-into {}    ibr n)
    (read-kvs-via-rt-map ibr n)))

(defn read-kvs-depr [to ^IByteReader ibr] (read-kvs-into to ibr (quot (.readInt ibr) 2)))

(enc/declare-remote ^:dynamic taoensso.nippy/*custom-readers*)

(defn read-custom [^IByteReader ibr prefixed? type-id]
  (if-let [custom-reader (get taoensso.nippy/*custom-readers* type-id)]
    (try
      (custom-reader (.toDataInput ibr))
      (catch Exception e
        (truss/ex-info!
          (str "Reader exception for custom type id: " type-id)
          {:type-id type-id, :prefixed? prefixed?} e)))

    (truss/ex-info!
      (str "No reader provided for custom type id: " type-id)
      {:type-id type-id, :prefixed? prefixed?})))

(defn read-sz!!
  "Reads object using Java `Serializable`. May be unsafe!"
  [^java.io.InputStream input-stream class-name]
  (try
    (let [obj (.readObject (java.io.ObjectInputStream. input-stream))] ; May be unsafe!
      (try
        (cast (Class/forName class-name) obj)
        (catch Exception e
          {:nippy/unthawable
           {:type  :serializable
            :cause :exception

            :class-name class-name
            :content    obj
            :exception  e}})))

    (catch Exception e
      {:nippy/unthawable
       {:type  :serializable
        :cause :exception

        :class-name class-name
        :content    nil
        :exception  e}})))

(defn read-sz [^IByteReader ibr class-name legacy?]
  (if legacy?

    ;; Serialized object directly to stream WITHOUT length prefix
    (if (impl/thaw-serializable-allowed? class-name)
      (read-sz!! (.toInputStream ibr)    class-name)
      (truss/ex-info! ; No way to skip bytes, so best we can do is throw
        "Cannot thaw object: `taoensso.nippy/*thaw-serializable-allowlist*` check failed. This is a security feature. See `*thaw-serializable-allowlist*` docstring or https://github.com/ptaoussanis/nippy/issues/130 for details!"
        {:class-name class-name}))

    (let [sz-ba (read-bytes ibr)]
      (if (impl/thaw-serializable-allowed?       class-name)
        (read-sz!! (ByteArrayInputStream. sz-ba) class-name)
        {:nippy/unthawable
         {:type  :serializable
          :cause :quarantined

          :class-name class-name
          :content    sz-ba}}))))

(defn ^:public read-quarantined-serializable-object-unsafe!
  "Given a quarantined Serializable object like
  {:nippy/unthawable {:class-name <> :content <quarantined-ba>}}, reads and
  returns the object WITHOUT regard for `*thaw-serializable-allowlist*`.

  **MAY BE UNSAFE!** Don't call this unless you absolutely trust the payload
  to not contain any malicious code.

  See `*thaw-serializable-allowlist*` for more info."
  [m]
  (when-let [m (get m :nippy/unthawable)]
    (let [{:keys [class-name content]} m]
      (when  (and class-name content)
        (let [sz-ba content]
          (read-sz!! (ByteArrayInputStream. sz-ba) class-name))))))

(let [class-method-sig (into-array Class [clojure.lang.IPersistentMap])]
  (defn read-record [^IByteReader ibr class-name]
    (let [content (read-typed ibr)]
      (try
        (let [c   (clojure.lang.RT/classForName class-name)
              ctr (.getMethod c "create" class-method-sig)]
          (.invoke ctr c (into-array Object [content])))

        (catch Exception e
          {:nippy/unthawable
           {:type  :record
            :cause :exception
            :class-name class-name
            :content    content
            :exception  e}})))))

(defn- read-deftype-fields ^objects [^IByteReader ibr n]
  (let [vals (object-array n)]
    (enc/reduce-n (fn [_ i] (aset vals i (read-typed ibr))) nil n)
    vals))

(defn read-deftype [^IByteReader ibr class-name legacy?]
  (if legacy? ; No field count in payload
    (try
      (let [c   (clojure.lang.RT/classForName class-name)
            ctr (aget (.getConstructors c) 0)
            num-fields-exp (count (impl/get-basis-fields c))]

        (.newInstance ^java.lang.reflect.Constructor ctr
          (read-deftype-fields ibr num-fields-exp)))

      (catch Exception e
        {:nippy/unthawable
         {:type       :deftype
          :class-name class-name
          :cause      :exception
          :exception  e}}))

    (let [num-fields (read-sm-ucount      ibr)
          field-vals (read-deftype-fields ibr num-fields) ; Always read all fields first
          ]
      (try
        (let [c   (clojure.lang.RT/classForName class-name)
              ctr (aget (.getConstructors c) 0)
              num-fields-exp (count (impl/get-basis-fields c))]

          (if (== num-fields num-fields-exp)
            (.newInstance ^java.lang.reflect.Constructor ctr field-vals)
            {:nippy/unthawable
             {:type       :deftype
              :class-name class-name
              :cause      :field-num-mismatch
              :field-num  {:expected num-fields-exp, :actual num-fields}
              :content    (vec field-vals)}}))

        (catch Exception e ; e.g. class not found, constructor failure
          {:nippy/unthawable
           {:type       :deftype
            :class-name class-name
            :cause      :exception
            :content    (vec field-vals)
            :exception  e}})))))

(enc/declare-remote ^:dynamic taoensso.nippy/*incl-metadata?*)

(defn read-typed
  "Reads one object as type-prefixed bytes from given `IByteReader`."
  [^IByteReader  ibr]
  (let [type-id (int (.readByte ibr))]
    (impl/when-debug (println (str "read-typed: " type-id)))
    (try
      (enc/case-eval type-id

        sc/id-nil               nil
        sc/id-true              true
        sc/id-false             false
        sc/id-meta-protocol-key impl/meta-protocol-key

        sc/id-reader-sm  (impl/read-edn   (read-str ibr (read-sm-count ibr)))
        sc/id-reader-md  (impl/read-edn   (read-str ibr (read-md-count ibr)))
        sc/id-reader-lg  (impl/read-edn   (read-str ibr (read-lg-count ibr)))
        sc/id-reader-lg_ (impl/read-edn   (read-str ibr (read-lg-count ibr)))
        sc/id-record-sm  (read-record ibr (read-str ibr (read-sm-count ibr)))
        sc/id-record-md  (read-record ibr (read-str ibr (read-md-count ibr)))
        sc/id-record-lg_ (read-record ibr (read-str ibr (read-lg-count ibr)))

        sc/id-sz-sm  (read-sz ibr (read-str ibr (read-sm-count ibr)) false)
        sc/id-sz-md  (read-sz ibr (read-str ibr (read-md-count ibr)) false)
        sc/id-sz-sm_ (read-sz ibr (read-str ibr (read-sm-count ibr)) :legacy)
        sc/id-sz-md_ (read-sz ibr (read-str ibr (read-md-count ibr)) :legacy)
        sc/id-sz-lg_ (read-sz ibr (read-str ibr (read-lg-count ibr)) :legacy)

        sc/id-deftype  (read-deftype ibr (read-typed ibr) false)
        sc/id-deftype_ (read-deftype ibr (read-typed ibr) :legacy)
        sc/id-char     (.readChar    ibr)

        sc/id-meta
        (let [m (read-typed ibr) ; Always consume from stream
              x (read-typed ibr)]
          (if-let [m (when taoensso.nippy/*incl-metadata?* (not-empty (dissoc m impl/meta-protocol-key)))]
            (with-meta x m)
            (do        x)))

        sc/id-cached-0  (impl/read-cached read-typed 0 ibr)
        sc/id-cached-1  (impl/read-cached read-typed 1 ibr)
        sc/id-cached-2  (impl/read-cached read-typed 2 ibr)
        sc/id-cached-3  (impl/read-cached read-typed 3 ibr)
        sc/id-cached-4  (impl/read-cached read-typed 4 ibr)
        sc/id-cached-5  (impl/read-cached read-typed 5 ibr)
        sc/id-cached-6  (impl/read-cached read-typed 6 ibr)
        sc/id-cached-7  (impl/read-cached read-typed 7 ibr)
        sc/id-cached-sm (impl/read-cached read-typed (read-sm-count ibr) ibr)
        sc/id-cached-md (impl/read-cached read-typed (read-md-count ibr) ibr)

        sc/id-byte-array-0    (byte-array 0)
        sc/id-byte-array-sm   (read-bytes ibr (read-sm-count ibr))
        sc/id-byte-array-md   (read-bytes ibr (read-md-count ibr))
        sc/id-byte-array-lg   (read-bytes ibr (read-lg-count ibr))

        sc/id-string-array-lg  (read-dyn-array ibr String "[Ljava.lang.String;" (make-array String (ensure-readable-length! ibr (read-lg-count ibr) 1)))
        sc/id-object-array-lg  (read-dyn-array ibr Object "[Ljava.lang.Object;" (object-array      (ensure-readable-length! ibr (read-lg-count ibr) 1)))
        sc/id-int-array-lg_    (read-dyn-array ibr int    "[I"                  (int-array         (ensure-readable-length! ibr (read-lg-count ibr) 1)))
        sc/id-long-array-lg_   (read-dyn-array ibr long   "[J"                  (long-array        (ensure-readable-length! ibr (read-lg-count ibr) 1)))
        sc/id-float-array-lg_  (read-dyn-array ibr float  "[F"                  (float-array       (ensure-readable-length! ibr (read-lg-count ibr) 1)))
        sc/id-double-array-lg_ (read-dyn-array ibr double "[D"                  (double-array      (ensure-readable-length! ibr (read-lg-count ibr) 1)))

        sc/id-int-array-lg     (read-prim-array ibr "[I" int-array    .asIntBuffer    Integer/BYTES .readInt)
        sc/id-long-array-lg    (read-prim-array ibr "[J" long-array   .asLongBuffer      Long/BYTES .readLong)
        sc/id-float-array-lg   (read-prim-array ibr "[F" float-array  .asFloatBuffer    Float/BYTES .readFloat)
        sc/id-double-array-lg  (read-prim-array ibr "[D" double-array .asDoubleBuffer  Double/BYTES .readDouble)

        sc/id-str-0       ""
        sc/id-str-sm*              (read-str ibr (read-sm-ucount ibr))
        sc/id-str-sm_              (read-str ibr (read-sm-count  ibr))
        sc/id-str-md               (read-str ibr (read-md-count  ibr))
        sc/id-str-lg               (read-str ibr (read-lg-count  ibr))

        sc/id-kw-sm       (keyword (read-str ibr (read-sm-count ibr)))
        sc/id-kw-md       (keyword (read-str ibr (read-md-count ibr)))
        sc/id-kw-md_      (keyword (read-str ibr (read-lg-count ibr)))
        sc/id-kw-lg_      (keyword (read-str ibr (read-lg-count ibr)))

        sc/id-sym-sm      (symbol  (read-str ibr (read-sm-count ibr)))
        sc/id-sym-md      (symbol  (read-str ibr (read-md-count ibr)))
        sc/id-sym-md_     (symbol  (read-str ibr (read-lg-count ibr)))
        sc/id-sym-lg_     (symbol  (read-str ibr (read-lg-count ibr)))
        sc/id-regex       (re-pattern            (read-typed    ibr))

        sc/id-vec-0       []
        sc/id-vec-2       (read-vec ibr 2)
        sc/id-vec-3       (read-vec ibr 3)
        sc/id-vec-sm*     (read-vec ibr (read-sm-ucount ibr))
        sc/id-vec-sm_     (read-vec ibr (read-sm-count  ibr))
        sc/id-vec-md      (read-vec ibr (read-md-count  ibr))
        sc/id-vec-lg      (read-vec ibr (read-lg-count  ibr))

        sc/id-set-0       #{}
        sc/id-set-sm*     (read-into    #{} ibr (read-sm-ucount ibr))
        sc/id-set-sm_     (read-into    #{} ibr (read-sm-count  ibr))
        sc/id-set-md      (read-into    #{} ibr (read-md-count  ibr))
        sc/id-set-lg      (read-into    #{} ibr (read-lg-count  ibr))

        sc/id-map-0       {}
        sc/id-map-sm*     (read-map ibr (read-sm-ucount ibr))
        sc/id-map-sm_     (read-map ibr (read-sm-count  ibr))
        sc/id-map-md      (read-map ibr (read-md-count  ibr))
        sc/id-map-lg      (read-map ibr (read-lg-count  ibr))
        sc/id-pam-sm*_    (read-map ibr (read-sm-ucount ibr)) ; Retired encoding, see schema

        sc/id-queue-lg      (read-into     clojure.lang.PersistentQueue/EMPTY ibr (read-lg-count ibr))
        sc/id-sorted-set-lg (read-into     (sorted-set)                       ibr (read-lg-count ibr))
        sc/id-sorted-map-lg (read-kvs-into (sorted-map)                       ibr (read-lg-count ibr))

        sc/id-list-0            ()
        sc/id-list-sm     (into () (rseq (read-into [] ibr (read-sm-count ibr))))
        sc/id-list-md     (into () (rseq (read-into [] ibr (read-md-count ibr))))
        sc/id-list-lg     (into () (rseq (read-into [] ibr (read-lg-count ibr))))

        sc/id-seq-0       (lazy-seq nil)
        sc/id-seq-sm      (or (seq (read-into [] ibr (read-sm-count ibr))) (lazy-seq nil))
        sc/id-seq-md      (or (seq (read-into [] ibr (read-md-count ibr))) (lazy-seq nil))
        sc/id-seq-lg      (or (seq (read-into [] ibr (read-lg-count ibr))) (lazy-seq nil))

        sc/id-byte              (.readByte  ibr)
        sc/id-short             (.readShort ibr)
        sc/id-integer           (.readInt   ibr)
        sc/id-long-0      0
        sc/id-long-sm_    (long (.readByte  ibr))
        sc/id-long-md_    (long (.readShort ibr))
        sc/id-long-lg_    (long (.readInt   ibr))
        sc/id-long-xl           (.readLong  ibr)

        sc/id-long-pos-sm    (- (long (.readByte  ibr))    Byte/MIN_VALUE)
        sc/id-long-pos-md    (- (long (.readShort ibr))   Short/MIN_VALUE)
        sc/id-long-pos-lg    (- (long (.readInt   ibr)) Integer/MIN_VALUE)

        sc/id-long-neg-sm (- (- (long (.readByte  ibr))    Byte/MIN_VALUE))
        sc/id-long-neg-md (- (- (long (.readShort ibr))   Short/MIN_VALUE))
        sc/id-long-neg-lg (- (- (long (.readInt   ibr)) Integer/MIN_VALUE))

        sc/id-bigint      (bigint (read-biginteger ibr))
        sc/id-biginteger          (read-biginteger ibr)

        sc/id-float       (.readFloat  ibr)
        sc/id-double-0    0.0
        sc/id-double      (.readDouble ibr)

        sc/id-bigdec      (BigDecimal. ^BigInteger (read-biginteger ibr) (.readInt        ibr))
        sc/id-ratio       (clojure.lang.Ratio.     (read-biginteger ibr) (read-biginteger ibr))

        sc/id-map-entry   (enc/map-entry (read-typed ibr) (read-typed ibr))

        sc/id-util-date   (java.util.Date. (.readLong ibr))
        sc/id-sql-date    (java.sql.Date.  (.readLong ibr))
        sc/id-uuid        (java.util.UUID. (.readLong ibr) (.readLong ibr))
        sc/id-uri         (java.net.URI.   (read-typed ibr))

        sc/id-prefixed-custom-md (read-custom ibr :prefixed (.readShort ibr))

        sc/id-time-instant
        (let [secs  (.readLong ibr)
              nanos (.readInt  ibr)]

          (enc/compile-if java.time.Instant
            (java.time.Instant/ofEpochSecond secs nanos)
            {:nippy/unthawable
             {:type  :class
              :cause :class-not-found
              :class-name "java.time.Instant"
              :content    {:epoch-second secs :nano nanos}}}))

        sc/id-time-duration
        (let [secs  (.readLong ibr)
              nanos (.readInt  ibr)]

          (enc/compile-if java.time.Duration
            (java.time.Duration/ofSeconds secs nanos)
            {:nippy/unthawable
             {:type       :class
              :cause      :class-not-found
              :class-name "java.time.Duration"
              :content    {:seconds secs :nanos nanos}}}))

        sc/id-time-period
        (let [years  (.readInt ibr)
              months (.readInt ibr)
              days   (.readInt ibr)]

          (enc/compile-if java.time.Period
            (java.time.Period/of years months days)
            {:nippy/unthawable
             {:type       :class
              :cause      :class-not-found
              :class-name "java.time.Period"
              :content    {:years years :months months :days days}}}))

        ;; Deprecated ------------------------------------------------------
        sc/id-boolean_    (not (zero? (int (.readByte ibr))))
        sc/id-sorted-map_ (read-kvs-depr (sorted-map) ibr)
        sc/id-map__       (read-kvs-depr {} ibr)
        sc/id-reader_     (impl/read-edn (.readUTF ^DataInput (.toDataInput ibr)))
        sc/id-str_                       (.readUTF ^DataInput (.toDataInput ibr))
        sc/id-kw_               (keyword (.readUTF ^DataInput (.toDataInput ibr)))
        sc/id-map_
        (apply hash-map
          (enc/repeatedly-into [] (* 2 (.readInt ibr))
            (fn [] (read-typed ibr))))
        ;; -----------------------------------------------------------------

        ;; else
        (if (neg? type-id)
          (read-custom ibr false type-id) ; Unprefixed custom type
          (truss/ex-info!
            (str "Unrecognized type id (" type-id "). Data frozen with newer Nippy version?")
            {:type-id type-id})))

      (catch Throwable t
        (truss/ex-info! (str "Thaw failed against type-id: " type-id)
          {:type-id type-id} t)))))

;;;; ByteBuffer -> DataOutput/Input adapters

(defn- write-modified-utf [^ByteBuffer bb ^String s]
  (let [strlen (.length s)
        utf-len
        (loop [idx 0 utf-len 0]
          (if (< idx strlen)
            (let [c (int (.charAt s idx))]
              (recur
                (inc idx)
                (long
                  (+ (long utf-len)
                     (long
                       (cond
                         (<= 0x0001 c 0x007F) 1
                         (> c 0x07FF)         3
                         :else                2))))))
            utf-len))]

    (when (> ^long utf-len 65535) (throw (java.io.UTFDataFormatException. (str "Encoded string too long: " utf-len " bytes"))))

    (bb-writable! bb (+ 2       ^long utf-len))
    (.putShort         bb (unchecked-short utf-len))
    (loop [idx 0]
      (when (< idx strlen)
        (let [c (int (.charAt s idx))]
          (cond
            (<= 0x0001 c 0x007F) (do (.put bb (unchecked-byte c)) (recur (inc idx)))
            (>         c 0x07FF)
            (do (.put bb (unchecked-byte (bit-or 0xE0          (unsigned-bit-shift-right c 12))))
                (.put bb (unchecked-byte (bit-or 0x80 (bit-and (unsigned-bit-shift-right c  6) 0x3F))))
                (.put bb (unchecked-byte (bit-or 0x80 (bit-and                           c     0x3F))))
                (recur (inc idx)))

            :else
            (do (.put bb (unchecked-byte (bit-or 0xC0 (unsigned-bit-shift-right c 6))))
                (.put bb (unchecked-byte (bit-or 0x80 (bit-and c 0x3F))))
                (recur (inc idx)))))))))

(defn bb->dout
  "Returns a `DataOutput` adapter over given `ByteBuffer`.
  Writes at the buffer's current position and advances it."
  ^DataOutput [^ByteBuffer bb]
  (let [bb (bb-big-endian! bb)]
    (reify DataOutput
      (^void write [_ ^bytes ba]
       (let [len (alength ba)]
         (bb-writable! bb len)
         (.put bb ba 0 len)
         nil))

      (^void write [_ ^bytes ba ^int off ^int len]
       (bb-writable! bb len)
       (.put bb ba off len)
       nil)

      (^void write        [_ ^int     b] (bb-writable! bb 1) (.put       bb (unchecked-byte  b))         nil)
      (^void writeBoolean [_ ^boolean b] (bb-writable! bb 1) (.put       bb (unchecked-byte (if b 1 0))) nil)
      (^void writeByte    [_ ^int     b] (bb-writable! bb 1) (.put       bb (unchecked-byte  b))         nil)
      (^void writeShort   [_ ^int     n] (bb-writable! bb 2) (.putShort  bb (unchecked-short n))         nil)
      (^void writeChar    [_ ^int     c] (bb-writable! bb 2) (.putChar   bb (unchecked-char  c))         nil)
      (^void writeInt     [_ ^int     n] (bb-writable! bb 4) (.putInt    bb n) nil)
      (^void writeLong    [_ ^long    n] (bb-writable! bb 8) (.putLong   bb n) nil)
      (^void writeFloat   [_ ^float   n] (bb-writable! bb 4) (.putFloat  bb n) nil)
      (^void writeDouble  [_ ^double  n] (bb-writable! bb 8) (.putDouble bb n) nil)
      (^void writeUTF     [_ ^String  s] (write-modified-utf bb s) nil)
      (^void writeBytes   [_ ^String  s]
       (let [len (.length s)]
         (bb-writable! bb len)
         (loop [idx 0]
           (when (< idx len)
             (.put bb (unchecked-byte (int (.charAt s idx))))
             (recur (inc idx))))
         nil))

      (^void writeChars [_ ^String s]
       (let [len (.length s)]
         (bb-writable! bb (* 2 len))
         (loop [idx 0]
           (when (< idx len)
             (.putChar bb (.charAt s idx))
             (recur (inc idx))))
         nil)))))

(defn bb->din
  "Returns a `DataInput` adapter over given `ByteBuffer`.
  Reads from the buffer's current position and advances it."
  ^DataInput  [^ByteBuffer bb]
  (let [bb (bb-big-endian! bb)]
    (reify DataInput
      (^void readFully [_ ^bytes ba]
       (let [len (alength ba)]
         (bb-readable! bb len)
         (.get bb ba 0 len)
         nil))

      (^void readFully [_ ^bytes ba ^int off ^int len]
       (bb-readable! bb len)
       (.get bb ba off len)
       nil)

      (^int skipBytes [_ ^int n]
       (let [n       (max n 0)
             skipped (min n (.remaining bb))]
         (bb-advance bb skipped)
         (do            skipped)))

      (^boolean   readBoolean [_] (bb-readable! bb 1) (not (zero?   (int (.get bb)))))
      (^byte         readByte [_] (bb-readable! bb 1)                    (.get bb))
      (^int  readUnsignedByte [_] (bb-readable! bb 1) (bit-and 0xFF (int (.get bb))))

      (^short       readShort [_] (bb-readable! bb 2)                      (.getShort bb))
      (^int readUnsignedShort [_] (bb-readable! bb 2) (bit-and 0xFFFF (int (.getShort bb))))

      (^char     readChar [_] (bb-readable! bb 2) (.getChar   bb))
      (^int       readInt [_] (bb-readable! bb 4) (.getInt    bb))
      (^long     readLong [_] (bb-readable! bb 8) (.getLong   bb))
      (^float   readFloat [_] (bb-readable! bb 4) (.getFloat  bb))
      (^double readDouble [_] (bb-readable! bb 8) (.getDouble bb))

      (^String readUTF  [this] (DataInputStream/readUTF this))
      (^String readLine [_]
       (when (.hasRemaining bb)
         (let [sb (StringBuilder.)]
           (loop []
             (if-not (.hasRemaining bb)
               (.toString sb)
               (let   [b (bit-and 0xFF (int (.get bb)))]
                 (case b
                   10 (.toString sb)
                   13
                   (do
                     (when (and (.hasRemaining bb) (= 10 (bit-and 0xFF (int (.get bb (.position bb))))))
                       (bb-advance bb 1))
                     (.toString sb))

                   ;; else
                   (do (.append sb (char b)) (recur))))))))))))

(def ^:private ^:const max-cached-bb-capacity (* 1024 1024))
(def ^:private ^:const max-bb-capacity
  "Max safe JVM array size, so max serialized size of a single frozen value."
  (- Integer/MAX_VALUE 8))

(defn- grown-bb
  "Returns a new `ByteBuffer` with double the capacity of the given one,
  capped at `max-bb-capacity`. Throws a clear error when already at cap."
  ^ByteBuffer [^ByteBuffer bb]
  (let [capacity (long (.capacity bb))]
    (when (>= capacity (long max-bb-capacity))
      (truss/ex-info! "Serialized value too large to freeze"
        {:max-size max-bb-capacity}))

    (ByteBuffer/allocate
      (int (min (long max-bb-capacity) (* 2 capacity))))))

(let [^ThreadLocal tl:bb    (enc/threadlocal (java.nio.ByteBuffer/allocate 512))
      ^ThreadLocal tl:depth (enc/threadlocal 0)
      copy-bb              (fn [^ByteBuffer bb] (java.util.Arrays/copyOf (.array bb) (.position bb)))
      grow-sentinel        (Object.)]

  ;; @LATER: Consider using a simple pool here, with auto GC

  (defn with-bb
    "Executes `(f bb dout_)` and returns ?ba of bb when `f` returns truthy.
      `bb` ---- Auto-expanding `ByteBuffer`. Will reuse ThreadLocal when possible,
                retaining up to 1 MiB per thread for reuse.
      `dout_` - Call (dout_) to get a `DataOutput` view on `bb`.

    `finalize` is called on the final (settled) `bb` to produce the return
    value, and defaults to copying `bb`'s written bytes to a new ba. Callers
    that only need to consume the bytes (e.g. write them to a `DataOutput`)
    can pass a custom `finalize` to avoid that copy."
    ([          f] (with-bb 512 f))
    ([init-size f] (with-bb init-size f copy-bb))
    ([init-size f finalize]
     (let [init-depth (long (.get      tl:depth))
           state            (.get impl/tl:cache)
           mark       (if state (impl/cache-mark state) 0)]

       (.set tl:depth (inc init-depth))
       (try
         (enc/cond
           (zero? init-depth) ; Unnested call
           (let [^ByteBuffer bb (.get tl:bb)]
             (when-let [[ba final-bb] (with-bb bb state mark f finalize)]
               (let [^ByteBuffer cached-bb
                     (cond
                       (<= (.capacity ^ByteBuffer final-bb) max-cached-bb-capacity) final-bb
                       (<= (.capacity                   bb) max-cached-bb-capacity)       bb
                       :else (ByteBuffer/allocate 512))]

                 (when-not (identical? bb cached-bb)
                   (.set tl:bb cached-bb)))
               ba))

           :else ; Nested call
           (let [private-bb (ByteBuffer/allocate (.capacity ^ByteBuffer (.get tl:bb)))] ; Isolate from parent bb
             (when-let [[ba _bb] (with-bb private-bb state mark f finalize)]
               ba)))

         (catch Throwable t
           ;; Cache entries from the abandoned write would poison later writes
           ;; in a shared (`with-cache`) session
           (when state (impl/cache-restore! state mark))
           (throw t))

         (finally (.set tl:depth init-depth)))))

    ([bb state mark f] (with-bb bb state mark f copy-bb))
    ([bb state mark f finalize]
     (loop [^ByteBuffer bb bb]
       (.clear bb)                                    ; Reset buffer before (re)use
       (when state (impl/cache-restore! state mark))  ; Reset cache  before (re)use
       (let [dout_  (let [v_ (volatile! nil)] (fn [] (or @v_ (vreset! v_ (bb->dout bb)))))
             write-result
             (try
               (f bb dout_)
               (catch java.nio.BufferOverflowException _ grow-sentinel))]

         (if (identical? write-result grow-sentinel)
           (recur (grown-bb bb))
           ;; NB `finalize` runs outside the retry loop: it may write `bb`'s
           ;; bytes onward, and must never run for a write we'll discard
           (if write-result
             [(finalize bb) bb]
             (do
               ;; Written bytes are being discarded, restore cache to match
               (when state (impl/cache-restore! state mark))
               [nil bb]))))))))

;;;; Streaming writes

;; `with-bb` buffers a WHOLE value before writing it, so each value is capped
;; at ~2 GiB and costs heap proportional to its size. That's fine for `freeze`
;; (which needs the full ba anyway), but not `freeze-to-out!`.
;;
;; The writer below restores incremental writing. It streams counted colls
;; element-wise through a small fixed chunk, reusing the SAME leaf writers
;; `freeze` uses, so the two always emit identical bytes. Uncounted colls need
;; their count up-front, so they stay buffered.

(def ^:private ^:const stream-chunk-size (* 64 1024))

(def ^:private stream-kinds
  "Maps a native `IWriteTypedNoMeta` impl -> streaming strategy.

  NB keyed on the impl that Clojure's protocol dispatch actually resolves to,
  NOT on `instance?` checks. Dispatch prefers superclasses over interfaces, so
  hand-mirroring it silently corrupts output for types like:
    - `MapEntry`, which extends `APersistentVector`
    - `()`, an `EmptyList` that does NOT extend `PersistentList`
    - a `deftype` implementing `ISeq`, which resolves to the `IType` writer

  Anything not found here is written as a leaf, which is always correct.

  NB built eagerly, at load time: every native writer above is already
  registered, so this snapshot can only ever hold NATIVE impls. A lookup hit
  therefore proves dispatch resolved to a writer we emulate byte-for-byte,
  even if `extend-freeze` later replaces one of these classes' writers."
  (let [impls (:impls IWriteTypedNoMeta)]
    (reduce-kv
      (fn [m c kind] (if-let [i (get impls c)] (assoc m i kind) m))
      {}
      {clojure.lang.MapEntry          :map-entry
       clojure.lang.PersistentQueue   :queue
       clojure.lang.PersistentTreeSet :sorted-set
       clojure.lang.PersistentTreeMap :sorted-map
       clojure.lang.APersistentVector :vec
       clojure.lang.APersistentSet    :set
       clojure.lang.APersistentMap    :map
       clojure.lang.PersistentList    :list
       clojure.lang.ISeq              :seq
       Cached                         :cached})))

(def ^:private ^java.util.concurrent.ConcurrentHashMap stream-kinds-cache
  "(class -> ?kind) memoization of the `stream-kinds` lookup, which is on the
  hot path (once per value) and otherwise walks the class hierarchy each time.

  Sound because dispatch is a pure fn of `(class x)`, and because
  `stream-kinds` holds only native impls (see there), so a cached kind can
  never be a disguised `extend-freeze` writer."
  (java.util.concurrent.ConcurrentHashMap.))

(defn- stream-kind
  "Returns the streaming strategy for `x`, or nil to write it as a leaf."
  [x]
  (let [c (class x)
        v (.get stream-kinds-cache c)]
    (enc/cond
      (nil? v)
      (let [kind (get stream-kinds (find-protocol-impl IWriteTypedNoMeta x))]
        (.put stream-kinds-cache c (or kind ::nil))
        kind)

      (identical? v ::nil) nil
      :else v)))

(defn- stream-flush!
  "Writes `bb`'s pending bytes onward to `dout`, then clears `bb`."
  [^DataOutput dout ^ByteBuffer bb]
  (let [n (.position bb)]
    (when (pos? n) (.write dout (.array bb) (.arrayOffset bb) n))
    (.clear bb)))

(defn- stream-leaf!
  "Calls `(f x bb dout_)` to write a single value into the current chunk.

  On overflow: discards the value's partial bytes, makes room, and retries.
  Retrying is safe because (a) leaf writers mutate only `bb`'s position and
  contents plus the cache state, both of which we restore, and (b) no
  flush ever happens partway through a leaf, so its bytes are always still
  in the chunk and above `pos`.

  NB `f` takes `x` rather than closing over it, so callers can pass a constant
  fn instead of allocating a closure per value written."
  [^DataOutput dout bb_ dout_ state f x]
  (let [mark (if state (impl/cache-mark state) 0)]
    (loop [retried? false]
      (let [^ByteBuffer bb @bb_
            pos  (.position bb)
            overflowed?
            (try (f x bb dout_) false
              (catch BufferOverflowException _ true))]

        (when overflowed?
          (.position bb pos)                            ; Discard the value's partial bytes
          (when state (impl/cache-restore! state mark)) ; And any cache entries it made
          (if (or retried? (zero? pos))
            (vreset! bb_ (grown-bb bb)) ; Value alone exceeds chunk, need a bigger one
            (stream-flush! dout bb))    ; Chunk had prior bytes, flushing may suffice
          (recur true))))))

;; Constant leaf writers, to avoid allocating a closure per value written
(def ^:private leaf-typed       (fn [x   ^ByteBuffer bb dout_] (write-typed x      bb dout_)))
(def ^:private leaf-map-entry   (fn [_   ^ByteBuffer bb _]     (write-id           bb sc/id-map-entry)))
(def ^:private leaf-queue       (fn [cnt ^ByteBuffer bb _]     (write-id           bb sc/id-queue-lg)      (write-lg-count bb cnt)))
(def ^:private leaf-sorted-set  (fn [cnt ^ByteBuffer bb _]     (write-id           bb sc/id-sorted-set-lg) (write-lg-count bb cnt)))
(def ^:private leaf-sorted-map  (fn [cnt ^ByteBuffer bb _]     (write-id           bb sc/id-sorted-map-lg) (write-lg-count bb cnt)))
(def ^:private leaf-vec-header  (fn [cnt ^ByteBuffer bb _]     (write-coll-header* bb sc/id-vec-0  sc/id-vec-sm*  sc/id-vec-sm_  sc/id-vec-md  sc/id-vec-lg  cnt)))
(def ^:private leaf-set-header  (fn [cnt ^ByteBuffer bb _]     (write-coll-header* bb sc/id-set-0  sc/id-set-sm*  sc/id-set-sm_  sc/id-set-md  sc/id-set-lg  cnt)))
(def ^:private leaf-map-header  (fn [cnt ^ByteBuffer bb _]     (write-coll-header* bb sc/id-map-0  sc/id-map-sm*  sc/id-map-sm_  sc/id-map-md  sc/id-map-lg  cnt)))
(def ^:private leaf-list-header (fn [cnt ^ByteBuffer bb _]     (write-coll-header  bb sc/id-list-0 sc/id-list-sm                 sc/id-list-md sc/id-list-lg cnt)))
(def ^:private leaf-seq-header  (fn [cnt ^ByteBuffer bb _]     (write-coll-header  bb sc/id-seq-0  sc/id-seq-sm                  sc/id-seq-md  sc/id-seq-lg  cnt)))
(def ^:private leaf-meta        (fn [m   ^ByteBuffer bb dout_] (write-id           bb sc/id-meta) (write-map bb dout_ m :is-metadata)))

(declare stream-write+meta!)

(defn- stream-write-typed!
  "Streaming counterpart to `write-typed`."
  [^DataOutput dout bb_ dout_ state x]
  ;; Cheap pre-filter (only colls and `Cached` are ever streamed), then ask
  ;; Clojure which impl it would actually dispatch to. `custom-freezable?`
  ;; types are excluded since `extend-freeze` takes precedence.
  (let [kind
        (when (and
                (or
                  (instance? clojure.lang.IPersistentCollection x)
                  (instance? Cached x))
                (not (impl/custom-freezable? x)))
          (stream-kind x))]

    (if (nil? kind)
      (stream-leaf! dout bb_ dout_ state leaf-typed x)

      (let [el! (fn [el]  (stream-write+meta! dout bb_ dout_ state el))
            hd! (fn [f v] (stream-leaf!       dout bb_ dout_ state f v))
            kv! (fn [k v] (el! k) (el! v))]

        (case kind
          :map-entry (let [^clojure.lang.MapEntry me x]
                       (hd! leaf-map-entry nil) (el! (key me)) (el! (val me)))

          :queue      (do (hd! leaf-queue       (count x)) (run! el! x))
          :sorted-set (do (hd! leaf-sorted-set  (count x)) (run! el! x))
          :sorted-map (do (hd! leaf-sorted-map  (count x)) (enc/run-kv! kv! x))
          :vec        (do (hd! leaf-vec-header  (count x)) (run! el! x))
          :set        (do (hd! leaf-set-header  (count x)) (run! el! x))
          :map        (do (hd! leaf-map-header  (count x)) (enc/run-kv! kv! x))
          :list       (do (hd! leaf-list-header (count x)) (run! el! x))

          ;; Uncounted seqs need their count written up-front, so must be
          ;; buffered in full - exactly as in Nippy <v3.7
          :seq
          (if (counted? x)
            (do (hd! leaf-seq-header (count x)) (run! el! x))
            (stream-leaf! dout bb_ dout_ state leaf-typed x))

          :cached
          (let [x-val (.-val ^Cached x)]
            (if (or (nil? state) (nil? x-val))
              ;; Nothing to cache against: no `with-cache` session, or a nil val
              ;; (which the buffered writer also writes plain, since caching nil
              ;; has no benefit)
              (el! x-val)
              ;; NB the cache id write and its cache mutation happen together
              ;; inside the leaf, so an overflow rolls back both. On retry the
              ;; idx is recomputed identically, so the flag we read here is
              ;; always the successful attempt's.
              (let [write-val?_ (volatile! false)]
                (stream-leaf! dout bb_ dout_ state
                  (fn [_ ^ByteBuffer bb _]
                    (vreset! write-val?_ (write-cached-header! bb x-val state)))
                  nil)
                (when @write-val?_ (el! x-val))))))))))

(defn- stream-write+meta!
  "Streaming counterpart to `write-typed+meta`."
  [^DataOutput dout bb_ dout_ state x]
  (when (instance? clojure.lang.IObj x)
    (when-let [m (when taoensso.nippy/*incl-metadata?* (not-empty (meta x)))]
      (stream-leaf!    dout bb_ dout_ state leaf-meta m)))
  (stream-write-typed! dout bb_ dout_ state x))

;; Chunk is reused between (unnested) calls: `freeze-to-out!` is often called
;; in a tight loop, and a fresh 64 KiB alloc per call dominates its cost for
;; small values. Nested calls take a private chunk, since `extend-freeze` impls
;; may themselves call `freeze-to-out!` (see wiki), which would otherwise clear
;; the chunk out from under the write in progress.
(def ^:private ^ThreadLocal tl:stream-bb    (enc/threadlocal (ByteBuffer/allocate stream-chunk-size)))
(def ^:private ^ThreadLocal tl:stream-depth (enc/threadlocal 0))

(defn write-typed+meta-to-out!
  "Streams given object to given `DataOutput`, holding only a small chunk of
  its serialized bytes in memory at a time.

  Counted colls and `cache`d values stream at every nesting depth, so their
  total serialized size is unbounded. Individual values are still buffered in
  full, so each is capped at ~2 GiB: strings, byte arrays, uncounted/lazy
  seqs, records, deftypes, custom (`extend-freeze`) types, and metadata maps.

  NB writes are NOT atomic: on error `dout` may have received partial bytes.
  A shared `with-cache` session's cache IS restored though, so a failed write
  that flushed nothing (the usual case, since a chunk holds 64 KiB) leaves the
  session fully intact."
  [^DataOutput dout x]
  (let [init-depth (long (.get tl:stream-depth))
        state      (.get impl/tl:cache)
        mark       (if state (impl/cache-mark state) 0)]

    (.set tl:stream-depth (inc init-depth)) ; NB nothing between this and `try`
    (try
      (let [bb_
            (volatile!
              (if (zero? init-depth)
                (doto ^ByteBuffer (.get tl:stream-bb) (.clear))
                (ByteBuffer/allocate stream-chunk-size)))

            ;; `DataOutput` view on the CURRENT chunk, for the rare writers
            ;; that need one. Allocated once per stream, and re-derived if
            ;; the chunk is replaced by a grow.
            v_ (volatile! nil)
            dout_
            (fn []
              (let [bb @bb_, cached @v_]
                (if (and cached (identical? (nth cached 0) bb))
                  (nth cached 1)
                  (let [d (bb->dout bb)] (vreset! v_ [bb d]) d))))]

        (stream-write+meta! dout bb_ dout_ state x)
        (stream-flush! dout ^ByteBuffer @bb_)

        ;; NB only on success: on failure the thread-local still holds a valid
        ;; (bounded) chunk, and the next call will clear it before reuse
        (when (zero? init-depth)
          (let [^ByteBuffer bb @bb_] ; A big leaf may have grown it, don't retain if huge
            (.set tl:stream-bb
              (if (<= (.capacity bb) max-cached-bb-capacity)
                bb
                (ByteBuffer/allocate stream-chunk-size))))))

      (catch Throwable t
        ;; Cache entries from the abandoned write would poison later writes in
        ;; a shared (`with-cache`) session: the next write would emit a bare
        ;; ref to a value whose bytes were never emitted
        (when state (impl/cache-restore! state mark))
        (throw t))

      (finally (.set tl:stream-depth init-depth))))
  nil)

(comment
  (enc/qb 1e6 ; [115.8 125.5]
    (with-bb 512 (fn [_ _] false))
    (with-bb 512 (fn [_ _] true))))

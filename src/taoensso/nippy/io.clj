(ns taoensso.nippy.io
  "Private low-level utils for reading/writing data, don't use."
  (:require
   [taoensso.truss  :as truss]
   [taoensso.encore :as enc]
   [taoensso.nippy
    [impl   :as impl]
    [schema :as sc]])

  (:import
   [taoensso.nippy.impl Cached]
   [java.nio.charset StandardCharsets]
   [java.nio ByteBuffer]
   [java.io
    DataOutput       DataInput
    DataOutputStream DataInputStream
    ByteArrayOutputStream ByteArrayInputStream]))

;;;;

(defn assert-big-endian-bb
  ^ByteBuffer [^ByteBuffer bb]
  (when-not (= (.order bb) java.nio.ByteOrder/BIG_ENDIAN)
    (throw
      (IllegalArgumentException.
        (str "ByteBuffer must use BIG_ENDIAN order for DataInput/DataOutput semantics (have " (.order bb) ")."))))
  bb)

(defn- require-readable! [^ByteBuffer bb ^long n] (when (> n (.remaining bb)) (throw (java.io.EOFException. (str "ByteBuffer underflow: need " n " bytes, have " (.remaining bb) ".")))))
(defn- require-writable! [^ByteBuffer bb ^long n] (when (> n (.remaining bb)) (throw (java.nio.BufferOverflowException.))))

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

(defn write-array-lg [^ByteBuffer bb dout_ array array-len id]
  (write-id       bb id)
  (write-lg-count bb array-len)
  (enc/reduce-n (fn [_ idx] (write-typed+meta (aget array idx) bb dout_)) nil array-len))

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

(defn write-vec [^ByteBuffer bb dout_ v]
  (let [cnt (count v)]
    (if (zero? cnt)
      (write-id bb sc/id-vec-0)
      (do
        (enc/cond
          (when     impl/pack-unsigned? (impl/sm-ucount? cnt)) (do (write-id bb sc/id-vec-sm*) (write-sm-ucount bb cnt))
          (when-not impl/pack-unsigned? (impl/sm-count?  cnt)) (do (write-id bb sc/id-vec-sm_) (write-sm-count  bb cnt))
                                        (impl/md-count?  cnt)  (do (write-id bb sc/id-vec-md)  (write-md-count  bb cnt))
          :else                                                (do (write-id bb sc/id-vec-lg)  (write-lg-count  bb cnt)))

        (run! (fn [el] (write-typed+meta el bb dout_)) v)))))

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
     (if (zero? cnt)
       (write-id bb id-empty)
       (do
         (enc/cond
           (impl/sm-count? cnt) (do (write-id bb id-sm) (write-sm-count bb cnt))
           (impl/md-count? cnt) (do (write-id bb id-md) (write-md-count bb cnt))
           :else                (do (write-id bb id-lg) (write-lg-count bb cnt)))

         (enc/run-kv!
           (fn [k v]
             (write-typed+meta k bb dout_)
             (write-typed+meta v bb dout_))
           coll))))))

(defn write-counted-coll
  ([^ByteBuffer bb dout_ id-lg coll]
   (let [cnt (count coll)]
     (write-id       bb id-lg)
     (write-lg-count bb cnt)
     (reduce (fn [_ in] (write-typed+meta in bb dout_)) nil coll)))

  ([^ByteBuffer bb dout_ id-empty id-sm id-md id-lg coll]
   (let [cnt (count coll)]
     (if (zero? cnt)
       (write-id bb id-empty)
       (do
         (enc/cond
           (impl/sm-count? cnt) (do (write-id bb id-sm) (write-sm-count bb cnt))
           (impl/md-count? cnt) (do (write-id bb id-md) (write-md-count bb cnt))
           :else                (do (write-id bb id-lg) (write-lg-count bb cnt)))
         (reduce (fn [_ in] (write-typed+meta in bb dout_)) nil coll))))))

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
    (if (zero? cnt)
      (write-id bb sc/id-map-0)
      (do
        (enc/cond
          (when     impl/pack-unsigned? (impl/sm-ucount? cnt)) (do (write-id bb sc/id-map-sm*) (write-sm-ucount bb cnt))
          (when-not impl/pack-unsigned? (impl/sm-count?  cnt)) (do (write-id bb sc/id-map-sm_) (write-sm-count  bb cnt))
                                        (impl/md-count?  cnt)  (do (write-id bb sc/id-map-md)  (write-md-count  bb cnt))
          :else                                                (do (write-id bb sc/id-map-lg)  (write-lg-count  bb cnt)))

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
          m)))))

(defn write-set
  "Micro-optimized `write-counted-coll` w/ id-set-0 id-set-sm id-set-md id-set-lg."
  [^ByteBuffer bb dout_ s]
  (let [cnt (count s)]
    (if (zero? cnt)
      (write-id bb sc/id-set-0)
      (do
        (enc/cond
          (when     impl/pack-unsigned? (impl/sm-ucount? cnt)) (do (write-id bb sc/id-set-sm*) (write-sm-ucount bb cnt))
          (when-not impl/pack-unsigned? (impl/sm-count?  cnt)) (do (write-id bb sc/id-set-sm_) (write-sm-count  bb cnt))
                                        (impl/md-count?  cnt)  (do (write-id bb sc/id-set-md)  (write-md-count  bb cnt))
          :else                                                (do (write-id bb sc/id-set-lg)  (write-lg-count  bb cnt)))
        (reduce (fn [_ in] (write-typed+meta in bb dout_)) nil s)))))

(defn write-sz
  "Writes given arg using Java `Serializable`.
  Returns true iff allowed."
  [^ByteBuffer bb x]
  (impl/when-debug (println (str "write-sz: " (type x))))
  (when (and (instance? java.io.Serializable x) (not (fn? x)))
    (let [class-name (.getName (class x))] ; Reflect
      (when (impl/freeze-serializable-allowed? class-name)
        (let [class-name-ba (.getBytes class-name StandardCharsets/UTF_8)
              len           (alength   class-name-ba)]

          (enc/cond
            (impl/sm-count? len) (do (write-id bb sc/id-sz-sm) (write-bytes-sm bb class-name-ba))
            (impl/md-count? len) (do (write-id bb sc/id-sz-md) (write-bytes-md bb class-name-ba))
            ;; :else             (do (write-id bb sc/id-sz-lg) (write-bytes-md bb class-name-ba)) ; Unrealistic
            :else                (truss/ex-info! "Serializable class name too long" {:name class-name}))

          ;; Serialize object to isolated ba, then write the length-prefixed ba to stream.
          ;; Can therefore later choose to skip OR deserialize with `readObject`.
          (let [baos  (ByteArrayOutputStream.)
                dos   (DataOutputStream. baos)
                _     (.writeObject (java.io.ObjectOutputStream. dos) x)
                sz-ba (.toByteArray baos)]
            (write-bytes bb sz-ba))

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

(defn write-cached [^ByteBuffer bb dout_ x-val cache_]
  (let [cache @cache_
        k     #_x-val [x-val (meta x-val)] ; Also check meta for equality
        ?idx  (get cache k)
        ^int idx
        (or ?idx
          (let [idx (count cache)]
            (vswap! cache_ assoc k idx)
            idx))

        first-occurance? (nil? ?idx)]

    (enc/cond
      (impl/sm-count? idx)
      (case      (int idx)
        0 (do (write-id bb sc/id-cached-0) (when first-occurance? (write-typed+meta x-val bb dout_)))
        1 (do (write-id bb sc/id-cached-1) (when first-occurance? (write-typed+meta x-val bb dout_)))
        2 (do (write-id bb sc/id-cached-2) (when first-occurance? (write-typed+meta x-val bb dout_)))
        3 (do (write-id bb sc/id-cached-3) (when first-occurance? (write-typed+meta x-val bb dout_)))
        4 (do (write-id bb sc/id-cached-4) (when first-occurance? (write-typed+meta x-val bb dout_)))
        5 (do (write-id bb sc/id-cached-5) (when first-occurance? (write-typed+meta x-val bb dout_)))
        6 (do (write-id bb sc/id-cached-6) (when first-occurance? (write-typed+meta x-val bb dout_)))
        7 (do (write-id bb sc/id-cached-7) (when first-occurance? (write-typed+meta x-val bb dout_)))

        (do
          (write-id       bb sc/id-cached-sm)
          (write-sm-count bb idx)
          (when first-occurance? (write-typed+meta x-val bb dout_))))

      (impl/md-count? idx)
      (do
        (write-id       bb sc/id-cached-md)
        (write-md-count bb idx)
        (when first-occurance? (write-typed+meta x-val bb dout_)))

      :else
      ;; (truss/ex-info! "Max cache size exceeded" {:idx idx})
      (write-typed+meta x-val bb dout_) ; Just freeze uncached
      )))

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
  (let [atype      (or (when (symbol? atype) (when-let [r (resolve atype)] (when (var? r) @r))) atype)
        x          (with-meta 'x    {:tag atype})
        bb         (with-meta 'bb   {:tag 'ByteBuffer})
        id-form    (when id `(write-id ~'bb ~id))
        freezable? (if (= atype 'Object) nil true)]

    `(extend ~atype
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
    (if-let [cache_ (.get impl/tl:cache)]
      (write-cached           bb dout_ x-val cache_)
      (write-typed+meta x-val bb dout_))))

(writer impl/array-class-bytes     nil (write-bytes    bb x))
(writer impl/array-class-objects   nil (write-array-lg bb dout_ x (alength ^objects x) sc/id-object-array-lg))

(when (impl/target-release>= 350)
  (writer impl/array-class-ints    nil (write-array-lg bb dout_ x (alength ^ints                  x) sc/id-int-array-lg))
  (writer impl/array-class-longs   nil (write-array-lg bb dout_ x (alength ^longs                 x) sc/id-long-array-lg))
  (writer impl/array-class-floats  nil (write-array-lg bb dout_ x (alength ^floats                x) sc/id-float-array-lg))
  (writer impl/array-class-doubles nil (write-array-lg bb dout_ x (alength ^doubles               x) sc/id-double-array-lg))
  (writer impl/array-class-strings nil (write-array-lg bb dout_ x (alength ^"[Ljava.lang.String;" x) sc/id-string-array-lg)))

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
    (let [c (class x)]
      (write-id  bb sc/id-type)
      (write-str bb (.getName c))
      (run! (fn [^java.lang.reflect.Field f] (write-typed (.get f x) bb dout_))
        (impl/get-basis-fields c)))))

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
        (truss/catching  (write-sz       bb x))
        (truss/catching  (write-readable bb x))
        (write-typed (impl/wrap-unfreezable x) bb dout_)))

    ;; Without ff
    :let [[r1 e1] (try [(write-sz       bb x)] (catch Throwable t [nil t]))], r1 r1
    :let [[r2 e2] (try [(write-readable bb x)] (catch Throwable t [nil t]))], r2 r2
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
  (toInputStream []))

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
  (skipBytes     [_ ^int n] (let [pos (.position bb)] (.position bb (+ pos n)) n))
  (toDataInput   [_] (bb->din bb))
  (toInputStream [_] (java.io.ByteArrayInputStream. (.array bb) (.position bb) (.remaining bb))))

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
  (toInputStream [_] din))

(defmacro read-sm-ucount [ibr] `(- (int (.readByte  ~ibr)) Byte/MIN_VALUE))
(defmacro read-sm-count  [ibr]    `(int (.readByte  ~ibr)))
(defmacro read-md-count  [ibr]    `(int (.readShort ~ibr)))
(defmacro read-lg-count  [ibr]         `(.readInt   ~ibr))

(declare read-bytes)
(defn    read-bytes-sm* [^IByteReader ibr] (read-bytes ibr (read-sm-ucount ibr)))
(defn    read-bytes-sm  [^IByteReader ibr] (read-bytes ibr (read-sm-count  ibr)))
(defn    read-bytes-md  [^IByteReader ibr] (read-bytes ibr (read-md-count  ibr)))
(defn    read-bytes-lg  [^IByteReader ibr] (read-bytes ibr (read-lg-count  ibr)))
(defn    read-bytes
  ([^IByteReader ibr len]
   (let [len (int         len)
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
  ([^IByteReader ibr len] (String. ^bytes (read-bytes ibr len) StandardCharsets/UTF_8))
  ([^IByteReader ibr]
   (enc/case-eval (int (.readByte ibr))
     sc/id-str-0   ""
     sc/id-str-sm* (String. ^bytes (read-bytes ibr (read-sm-ucount ibr)) StandardCharsets/UTF_8)
     sc/id-str-sm_ (String. ^bytes (read-bytes ibr (read-sm-count  ibr)) StandardCharsets/UTF_8)
     sc/id-str-md  (String. ^bytes (read-bytes ibr (read-md-count  ibr)) StandardCharsets/UTF_8)
     sc/id-str-lg  (String. ^bytes (read-bytes ibr (read-lg-count  ibr)) StandardCharsets/UTF_8))))

(defn read-biginteger [^IByteReader ibr] (BigInteger. ^bytes (read-bytes ibr (.readInt ibr))))

(defmacro read-array [ibr thaw-type array-type array]
  (let [thawed-sym (with-meta 'thawed-sym {:tag thaw-type})
        array-sym  (with-meta 'array-sym  {:tag array-type})]
    `(let [~array-sym ~array]
       (enc/reduce-n
         (fn [_# idx#]
           (let [~thawed-sym (read-typed ~ibr)]
             (aset ~'array-sym idx# ~'thawed-sym)))
         nil (alength ~'array-sym))
       ~'array-sym)))

(enc/declare-remote ^:dynamic taoensso.nippy/*thaw-xform*)

(let [rf! (fn rf! ([x] (persistent! x)) ([acc x] (conj! acc x)))
      rf* (fn rf* ([x]              x)  ([acc x] (conj  acc x)))]

  (defn read-into [to ^IByteReader ibr ^long n]
    (let [transient? (when (impl/editable? to) (> n 10))
          init       (if transient? (transient to) to)
          rf         (if transient? rf! rf*)
          rf         (if-let [xf taoensso.nippy/*thaw-xform*] ((impl/xform* xf) rf) rf)]
      (rf (enc/reduce-n (fn [acc _] (rf acc (read-typed ibr))) init n)))))

(let [rf1! (fn rf1! ([x] (persistent! x)) ([acc kv ] (assoc! acc (key kv) (val kv))))
      rf2! (fn rf2! ([x] (persistent! x)) ([acc k v] (assoc! acc      k         v)))
      rf1* (fn rf1* ([x]              x)  ([acc kv ] (assoc  acc (key kv) (val kv))))
      rf2* (fn rf2* ([x]              x)  ([acc k v] (assoc  acc      k         v)))]

  (defn read-kvs-into [to ^IByteReader ibr ^long n]
    (let [transient? (when (impl/editable? to) (> n 10))
          init       (if transient? (transient to) to)
          rf1        (if transient? rf1! rf1*)
          rf2        (if transient? rf2! rf2*)]

      (if-let [xf taoensso.nippy/*thaw-xform*]
        (let [rf ((impl/xform* xf) rf1)] (rf (enc/reduce-n (fn [acc _] (rf acc (enc/map-entry (read-typed ibr) (read-typed ibr)))) init n)))
        (let [rf                   rf2 ] (rf (enc/reduce-n (fn [acc _] (rf acc                (read-typed ibr) (read-typed ibr)))  init n)))))))

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

(defn read-type [^IByteReader ibr class-name]
  (try
    (let [c          (clojure.lang.RT/classForName class-name)
          num-fields (count (impl/get-basis-fields c))
          field-vals (object-array num-fields)

          ;; Ref. <https://github.com/clojure/clojure/blob/e78519c174fb506afa70e236af509e73160f022a/src/jvm/clojure/lang/Compiler.java#L4799>
          ^java.lang.reflect.Constructor ctr (aget (.getConstructors c) 0)]

      (enc/reduce-n
        (fn [_ i] (aset field-vals i (read-typed ibr)))
        nil num-fields)

      (.newInstance ctr field-vals))

    (catch Exception e
      {:nippy/unthawable
       {:type  :type
        :cause :exception
        :class-name class-name
        :exception  e}})))

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

        sc/id-type (read-type ibr (read-typed ibr))
        sc/id-char (.readChar ibr)

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

        sc/id-long-array-lg   (read-array ibr long   "[J" (long-array   (read-lg-count ibr)))
        sc/id-int-array-lg    (read-array ibr int    "[I" (int-array    (read-lg-count ibr)))

        sc/id-double-array-lg (read-array ibr double "[D" (double-array (read-lg-count ibr)))
        sc/id-float-array-lg  (read-array ibr float  "[F" (float-array  (read-lg-count ibr)))

        sc/id-string-array-lg (read-array ibr String "[Ljava.lang.String;" (make-array String (read-lg-count ibr)))
        sc/id-object-array-lg (read-array ibr Object "[Ljava.lang.Object;" (object-array      (read-lg-count ibr)))

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
        sc/id-vec-2       (read-into [] ibr 2)
        sc/id-vec-3       (read-into [] ibr 3)
        sc/id-vec-sm*     (read-into [] ibr (read-sm-ucount ibr))
        sc/id-vec-sm_     (read-into [] ibr (read-sm-count  ibr))
        sc/id-vec-md      (read-into [] ibr (read-md-count  ibr))
        sc/id-vec-lg      (read-into [] ibr (read-lg-count  ibr))

        sc/id-set-0       #{}
        sc/id-set-sm*     (read-into    #{} ibr (read-sm-ucount ibr))
        sc/id-set-sm_     (read-into    #{} ibr (read-sm-count  ibr))
        sc/id-set-md      (read-into    #{} ibr (read-md-count  ibr))
        sc/id-set-lg      (read-into    #{} ibr (read-lg-count  ibr))

        sc/id-map-0       {}
        sc/id-map-sm*     (read-kvs-into {} ibr (read-sm-ucount ibr))
        sc/id-map-sm_     (read-kvs-into {} ibr (read-sm-count  ibr))
        sc/id-map-md      (read-kvs-into {} ibr (read-md-count  ibr))
        sc/id-map-lg      (read-kvs-into {} ibr (read-lg-count  ibr))

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

    (require-writable! bb (+ 2       ^long utf-len))
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
  (let [bb (assert-big-endian-bb bb)]
    (reify DataOutput
      (^void write [_ ^bytes ba]
       (let [len (alength ba)]
         (require-writable! bb len)
         (.put bb ba 0 len)
         nil))

      (^void write [_ ^bytes ba ^int off ^int len]
       (require-writable! bb len)
       (.put bb ba off len)
       nil)

      (^void write        [_ ^int     b] (require-writable!  bb 1) (.put       bb (unchecked-byte  b))         nil)
      (^void writeBoolean [_ ^boolean b] (require-writable!  bb 1) (.put       bb (unchecked-byte (if b 1 0))) nil)
      (^void writeByte    [_ ^int     b] (require-writable!  bb 1) (.put       bb (unchecked-byte  b))         nil)
      (^void writeShort   [_ ^int     n] (require-writable!  bb 2) (.putShort  bb (unchecked-short n))         nil)
      (^void writeChar    [_ ^int     c] (require-writable!  bb 2) (.putChar   bb (unchecked-char  c))         nil)
      (^void writeInt     [_ ^int     n] (require-writable!  bb 4) (.putInt    bb n) nil)
      (^void writeLong    [_ ^long    n] (require-writable!  bb 8) (.putLong   bb n) nil)
      (^void writeFloat   [_ ^float   n] (require-writable!  bb 4) (.putFloat  bb n) nil)
      (^void writeDouble  [_ ^double  n] (require-writable!  bb 8) (.putDouble bb n) nil)
      (^void writeUTF     [_ ^String  s] (write-modified-utf bb s) nil)

      (^void writeBytes [_ ^String s]
       (let [len (.length s)]
         (require-writable! bb len)
         (loop [idx 0]
           (when (< idx len)
             (.put bb (unchecked-byte (int (.charAt s idx))))
             (recur (inc idx))))
         nil))

      (^void writeChars [_ ^String s]
       (let [len (.length s)]
         (require-writable! bb (* 2 len))
         (loop [idx 0]
           (when (< idx len)
             (.putChar bb (.charAt s idx))
             (recur (inc idx))))
         nil)))))

(defn bb->din
  "Returns a `DataInput` adapter over given `ByteBuffer`.
  Reads from the buffer's current position and advances it."
  ^DataInput [^ByteBuffer bb]
  (let [bb (assert-big-endian-bb bb)]
    (reify DataInput
      (^void readFully [_ ^bytes ba]
       (let [len (alength ba)]
         (require-readable! bb len)
         (.get bb ba 0 len)
         nil))

      (^void readFully [_ ^bytes ba ^int off ^int len]
       (require-readable! bb len)
       (.get bb ba off len)
       nil)

      (^int skipBytes [_ ^int n]
       (let [n       (max n 0)
             skipped (min n (.remaining bb))]
         (.position bb (+ (.position bb) skipped))
         skipped))

      (^boolean   readBoolean [_] (require-readable! bb 1) (not (zero?   (int (.get bb)))))
      (^byte         readByte [_] (require-readable! bb 1)                    (.get bb))
      (^int  readUnsignedByte [_] (require-readable! bb 1) (bit-and 0xFF (int (.get bb))))

      (^short       readShort [_] (require-readable! bb 2)                      (.getShort bb))
      (^int readUnsignedShort [_] (require-readable! bb 2) (bit-and 0xFFFF (int (.getShort bb))))

      (^char     readChar [_] (require-readable! bb 2) (.getChar   bb))
      (^int       readInt [_] (require-readable! bb 4) (.getInt    bb))
      (^long     readLong [_] (require-readable! bb 8) (.getLong   bb))
      (^float   readFloat [_] (require-readable! bb 4) (.getFloat  bb))
      (^double readDouble [_] (require-readable! bb 8) (.getDouble bb))

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
                       (.position bb (inc (.position bb))))
                     (.toString sb))

                   ;; else
                   (do (.append sb (char b)) (recur))))))))))))

(let [^ThreadLocal tl:bb    (enc/threadlocal (java.nio.ByteBuffer/allocate 512))
      ^ThreadLocal tl:depth (enc/threadlocal 0)]

  ;; @LATER: Consider using a simple pool here, with auto GC

  (defn with-bb
    "Executes `(f bb dout_)` and returns ?ba of bb when `f` returns truthy.
      `bb` ---- Auto-expanding `ByteBuffer`. Will reuse ThreadLocal when possible,
                currently only freed via GC when thread dies.
      `dout_` - Call (dout_) to get a `DataOutput` view on `bb`."
    ([          f] (with-bb 512 f))
    ([init-size f]
     (let [init-depth (long (.get      tl:depth))
           cache_           (.get impl/tl:cache)
           init-cache (when cache_ @cache_)]

       (.set tl:depth (inc init-depth))
       (try
         (enc/cond
           (zero? init-depth) ; Unnested call
           (let [^ByteBuffer bb (.get tl:bb)]
             (when-let [[ba final-bb] (with-bb bb cache_ init-cache f)]
               (when-not (identical? bb final-bb)
                 (.set            tl:bb final-bb)) ; May have grown
               ba))

           :else ; Nested call
           (let [private-bb (ByteBuffer/allocate init-size)] ; Isolate from parent bb
             (when-let [[ba _bb] (with-bb private-bb cache_ init-cache f)]
               ba)))

         (finally (.set tl:depth init-depth)))))

    ([bb cache_ cache f]
     (loop [^ByteBuffer bb bb]
       (.clear bb)                          ; Reset buffer before (re)use
       (when cache_ (vreset! cache_ cache)) ; Reset cache  before (re)use
       (let [dout_  (let [v_ (volatile! nil)] (fn [] (or @v_ (vreset! v_ (bb->dout bb)))))
             result
             (try
               (when (f bb dout_)
                 (java.util.Arrays/copyOf (.array bb) (.position bb)))
               (catch java.nio.BufferOverflowException _ ::grow))]

         (if (identical? result ::grow)
           (recur (ByteBuffer/allocate (* 2 (.capacity bb))))
           [result bb]))))))

(comment
  (enc/qb 1e6 ; [115.8 125.5]
    (with-bb 512 (fn [_ _] false))
    (with-bb 512 (fn [_ _] true))))

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
   [java.io
    DataOutput       DataInput
    DataOutputStream DataInputStream
    ByteArrayOutputStream ByteArrayInputStream]))

;;;; Writing

(defprotocol IWriteTypedNoMeta   (write-typed      [_ ^DataOutput dout] "Writes given object as type-prefixed bytes. Excludes IObj meta."))
(defprotocol IWriteTypedWithMeta (write-typed+meta [_ ^DataOutput dout] "Writes given object as type-prefixed bytes. Includes IObj meta when present."))

(defmacro write-id        [dout id] `(.writeByte  ~dout ~id))
(defmacro write-sm-ucount [dout  n] `(.writeByte  ~dout (+ ~n Byte/MIN_VALUE))) ; Unsigned
(defmacro write-sm-count  [dout  n] `(.writeByte  ~dout    ~n))
(defmacro write-md-count  [dout  n] `(.writeShort ~dout    ~n))
(defmacro write-lg-count  [dout  n] `(.writeInt   ~dout    ~n))

(defn write-bytes-sm* [^DataOutput dout ^bytes ba] (let [len (alength ba)] (write-sm-ucount dout len) (.write dout ba 0 len))) ; Unsigned
(defn write-bytes-sm  [^DataOutput dout ^bytes ba] (let [len (alength ba)] (write-sm-count  dout len) (.write dout ba 0 len)))
(defn write-bytes-md  [^DataOutput dout ^bytes ba] (let [len (alength ba)] (write-md-count  dout len) (.write dout ba 0 len)))
(defn write-bytes-lg  [^DataOutput dout ^bytes ba] (let [len (alength ba)] (write-lg-count  dout len) (.write dout ba 0 len)))
(defn write-bytes     [^DataOutput dout ^bytes ba]
  (let [len (alength ba)]
    (if (zero? len)
      (write-id dout sc/id-byte-array-0)
      (do
        (enc/cond
          (impl/sm-count? len) (do (write-id dout sc/id-byte-array-sm) (write-sm-count dout len))
          (impl/md-count? len) (do (write-id dout sc/id-byte-array-md) (write-md-count dout len))
          :else                (do (write-id dout sc/id-byte-array-lg) (write-lg-count dout len)))

        (.write dout ba 0 len)))))

(defn write-array-lg [^DataOutput dout array array-len id]
  (write-id       dout id)
  (write-lg-count dout array-len)
  (enc/reduce-n (fn [_ idx] (write-typed+meta (aget array idx) dout)) nil array-len))

(defn write-biginteger [^DataOutput dout ^BigInteger n] (write-bytes-lg dout (.toByteArray n)))

(defn write-str-sm* [^DataOutput dout ^String s] (write-bytes-sm* dout (.getBytes s StandardCharsets/UTF_8)))
(defn write-str-sm  [^DataOutput dout ^String s] (write-bytes-sm  dout (.getBytes s StandardCharsets/UTF_8)))
(defn write-str-md  [^DataOutput dout ^String s] (write-bytes-md  dout (.getBytes s StandardCharsets/UTF_8)))
(defn write-str-lg  [^DataOutput dout ^String s] (write-bytes-lg  dout (.getBytes s StandardCharsets/UTF_8)))
(defn write-str     [^DataOutput dout ^String s]
  (if (identical? s "")
    (write-id dout sc/id-str-0)
    (let [ba  (.getBytes s StandardCharsets/UTF_8)
          len (alength ba)]
      (enc/cond
        (when     impl/pack-unsigned? (impl/sm-ucount? len)) (do (write-id dout sc/id-str-sm*) (write-sm-ucount dout len))
        (when-not impl/pack-unsigned? (impl/sm-count?  len)) (do (write-id dout sc/id-str-sm_) (write-sm-count  dout len))
                                      (impl/md-count?  len)  (do (write-id dout sc/id-str-md)  (write-md-count  dout len))
        :else                                                (do (write-id dout sc/id-str-lg)  (write-lg-count  dout len)))

      (.write dout ba 0 len))))

(defn write-kw [^DataOutput dout kw]
  (let [s   (if-let [ns (namespace kw)] (str ns "/" (name kw)) (name kw))
        ba  (.getBytes s StandardCharsets/UTF_8)
        len (alength ba)]
    (enc/cond
      (impl/sm-count? len) (do (write-id dout sc/id-kw-sm) (write-sm-count dout len))
      (impl/md-count? len) (do (write-id dout sc/id-kw-md) (write-md-count dout len))
      ;; :else             (do (write-id dout sc/id-kw-lg) (write-lg-count dout len)) ; Unrealistic
      :else                (truss/ex-info! "Keyword too long" {:name s}))

    (.write dout ba 0 len)))

(defn write-sym [^DataOutput dout s]
  (let [s   (if-let [ns (namespace s)] (str ns "/" (name s)) (name s))
        ba  (.getBytes s StandardCharsets/UTF_8)
        len (alength ba)]
    (enc/cond
      (impl/sm-count? len) (do (write-id dout sc/id-sym-sm) (write-sm-count dout len))
      (impl/md-count? len) (do (write-id dout sc/id-sym-md) (write-md-count dout len))
      ;; :else             (do (write-id dout sc/id-sym-lg) (write-lg-count dout len)) ; Unrealistic
      :else                (truss/ex-info! "Symbol too long" {:name s}))

    (.write dout ba 0 len)))

(defn write-long-legacy [^DataOutput dout ^long n]
  (enc/cond
    (zero? n) (write-id dout sc/id-long-0)
    (pos?  n)
    (enc/cond
      (<= n    Byte/MAX_VALUE) (do (write-id dout sc/id-long-sm_) (.writeByte  dout n))
      (<= n   Short/MAX_VALUE) (do (write-id dout sc/id-long-md_) (.writeShort dout n))
      (<= n Integer/MAX_VALUE) (do (write-id dout sc/id-long-lg_) (.writeInt   dout n))
      :else                    (do (write-id dout sc/id-long-xl)  (.writeLong  dout n)))

    :else
    (enc/cond
      (>= n    Byte/MIN_VALUE) (do (write-id dout sc/id-long-sm_) (.writeByte  dout n))
      (>= n   Short/MIN_VALUE) (do (write-id dout sc/id-long-md_) (.writeShort dout n))
      (>= n Integer/MIN_VALUE) (do (write-id dout sc/id-long-lg_) (.writeInt   dout n))
      :else                    (do (write-id dout sc/id-long-xl)  (.writeLong  dout n)))))

(defn write-long [^DataOutput dout ^long n]
  (enc/cond
    (not impl/pack-unsigned?) (write-long-legacy dout n)
    (zero? n)                 (write-id          dout sc/id-long-0)
    (pos?  n)
    (enc/cond
      (<= n impl/range-ubyte)  (do (write-id dout sc/id-long-pos-sm) (.writeByte  dout (+ n    Byte/MIN_VALUE)))
      (<= n impl/range-ushort) (do (write-id dout sc/id-long-pos-md) (.writeShort dout (+ n   Short/MIN_VALUE)))
      (<= n impl/range-uint)   (do (write-id dout sc/id-long-pos-lg) (.writeInt   dout (+ n Integer/MIN_VALUE)))
      :else                    (do (write-id dout sc/id-long-xl)     (.writeLong  dout    n)))

    :else
    (let [y (- n)]
      (enc/cond
        (<= y impl/range-ubyte)  (do (write-id dout sc/id-long-neg-sm) (.writeByte  dout (+ y    Byte/MIN_VALUE)))
        (<= y impl/range-ushort) (do (write-id dout sc/id-long-neg-md) (.writeShort dout (+ y   Short/MIN_VALUE)))
        (<= y impl/range-uint)   (do (write-id dout sc/id-long-neg-lg) (.writeInt   dout (+ y Integer/MIN_VALUE)))
        :else                    (do (write-id dout sc/id-long-xl)     (.writeLong  dout    n))))))

(defn write-vec [^DataOutput dout v]
  (let [cnt (count v)]
    (if (zero? cnt)
      (write-id dout sc/id-vec-0)
      (do
        (enc/cond
          (when     impl/pack-unsigned? (impl/sm-ucount? cnt)) (do (write-id dout sc/id-vec-sm*) (write-sm-ucount dout cnt))
          (when-not impl/pack-unsigned? (impl/sm-count?  cnt)) (do (write-id dout sc/id-vec-sm_) (write-sm-count  dout cnt))
                                        (impl/md-count?  cnt)  (do (write-id dout sc/id-vec-md)  (write-md-count  dout cnt))
          :else                                                (do (write-id dout sc/id-vec-lg)  (write-lg-count  dout cnt)))

        (run! (fn [el] (write-typed+meta el dout)) v)))))

(defn write-kvs
  ([^DataOutput dout id-lg coll]
   (let [cnt (count coll)]
     (write-id       dout id-lg)
     (write-lg-count dout cnt)
     (enc/run-kv!
       (fn [k v]
         (write-typed+meta k dout)
         (write-typed+meta v dout))
       coll)))

  ([^DataOutput dout id-empty id-sm id-md id-lg coll]
   (let [cnt (count coll)]
     (if (zero? cnt)
       (write-id dout id-empty)
       (do
         (enc/cond
           (impl/sm-count? cnt) (do (write-id dout id-sm) (write-sm-count dout cnt))
           (impl/md-count? cnt) (do (write-id dout id-md) (write-md-count dout cnt))
           :else                (do (write-id dout id-lg) (write-lg-count dout cnt)))

         (enc/run-kv!
           (fn [k v]
             (write-typed+meta k dout)
             (write-typed+meta v dout))
           coll))))))

(defn write-counted-coll
  ([^DataOutput dout id-lg coll]
   (let [cnt (count coll)]
     ;; (assert (counted? coll))
     (write-id       dout id-lg)
     (write-lg-count dout cnt)
     (run! (fn [el] (write-typed+meta el dout)) coll)))

  ([^DataOutput dout id-empty id-sm id-md id-lg coll]
   (let [cnt (count coll)]
     ;; (assert (counted? coll))
     (if (zero? cnt)
       (write-id dout id-empty)
       (do
         (enc/cond
           (impl/sm-count? cnt) (do (write-id dout id-sm) (write-sm-count dout cnt))
           (impl/md-count? cnt) (do (write-id dout id-md) (write-md-count dout cnt))
           :else                (do (write-id dout id-lg) (write-lg-count dout cnt)))

         (run! (fn [el] (write-typed+meta el dout)) coll))))))

(defn write-uncounted-coll
  ([^DataOutput dout id-lg coll]
   ;; (assert (not (counted? coll)))
   (let [bas  (ByteArrayOutputStream. 32)
         sout (DataOutputStream. bas)
         ^long cnt (reduce (fn [^long cnt in] (write-typed+meta in sout) (unchecked-inc cnt)) 0 coll)
         ba   (.toByteArray bas)]

     (write-id       dout id-lg)
     (write-lg-count dout cnt)
     (.write         dout ba)))

  ([^DataOutput dout id-empty id-sm id-md id-lg coll]
   (let [bas  (ByteArrayOutputStream. 32)
         sout (DataOutputStream. bas)
         ^long cnt (reduce (fn [^long cnt in] (write-typed+meta in sout) (unchecked-inc cnt)) 0 coll)
         ba   (.toByteArray bas)]

     (if (zero? cnt)
       (write-id dout id-empty)
       (do
         (enc/cond
           (impl/sm-count? cnt) (do (write-id dout id-sm) (write-sm-count dout cnt))
           (impl/md-count? cnt) (do (write-id dout id-md) (write-md-count dout cnt))
           :else                (do (write-id dout id-lg) (write-lg-count dout cnt)))

         (.write dout ba))))))

(defn write-coll
  ([dout id-lg coll]
   (if (counted? coll)
     (write-counted-coll   dout id-lg coll)
     (write-uncounted-coll dout id-lg coll)))

  ([dout id-empty id-sm id-md id-lg coll]
   (if (counted? coll)
     (write-counted-coll   dout id-empty id-sm id-md id-lg coll)
     (write-uncounted-coll dout id-empty id-sm id-md id-lg coll))))

(defn write-map
  "Micro-optimized `write-kvs` w/ id-map-0 id-map-sm id-map-md id-map-lg."
  [^DataOutput dout m is-metadata?]
  (let [cnt (count m)]
    (if (zero? cnt)
      (write-id dout sc/id-map-0)
      (do
        (enc/cond
          (when     impl/pack-unsigned? (impl/sm-ucount? cnt)) (do (write-id dout sc/id-map-sm*) (write-sm-ucount dout cnt))
          (when-not impl/pack-unsigned? (impl/sm-count?  cnt)) (do (write-id dout sc/id-map-sm_) (write-sm-count  dout cnt))
                                        (impl/md-count?  cnt)  (do (write-id dout sc/id-map-md)  (write-md-count  dout cnt))
          :else                                                (do (write-id dout sc/id-map-lg)  (write-lg-count  dout cnt)))

        (enc/run-kv!
          (fn [k v]
            (if (enc/and? is-metadata? (fn? v) (qualified-symbol? k))
              (do
                ;; Strip Clojure v1.10+ metadata protocol extensions
                ;; (used by defprotocol `:extend-via-metadata`)
                (if (impl/target-release>= 340)
                  (write-id dout sc/id-meta-protocol-key)
                  (write-typed    impl/meta-protocol-key dout))
                (write-id dout sc/id-nil))
              (do
                (write-typed+meta k dout)
                (write-typed+meta v dout))))
          m)))))

(comment (meta (thaw (freeze (with-meta [] {:a :A, 'b/c (fn [])})))))

(defn write-set
  "Micro-optimized `write-counted-coll` w/ id-set-0 id-set-sm id-set-md id-set-lg."
  [^DataOutput dout s]
  (let [cnt (count s)]
    (if (zero? cnt)
      (write-id dout sc/id-set-0)
      (do
        (enc/cond
          (when     impl/pack-unsigned? (impl/sm-ucount? cnt)) (do (write-id dout sc/id-set-sm*) (write-sm-ucount dout cnt))
          (when-not impl/pack-unsigned? (impl/sm-count?  cnt)) (do (write-id dout sc/id-set-sm_) (write-sm-count  dout cnt))
                                        (impl/md-count?  cnt)  (do (write-id dout sc/id-set-md)  (write-md-count  dout cnt))
          :else                                                (do (write-id dout sc/id-set-lg)  (write-lg-count  dout cnt)))

        (run! (fn [el] (write-typed+meta el dout)) s)))))

(defn write-sz
  "Writes given arg using Java `Serializable`.
  Returns true iff allowed."
  [^DataOutput dout x]
  (impl/when-debug (println (str "write-sz: " (type x))))
  (when (and (instance? java.io.Serializable x) (not (fn? x)))
    (let [class-name (.getName (class x))] ; Reflect
      (when (impl/freeze-serializable-allowed? class-name)
        (let [class-name-ba (.getBytes class-name StandardCharsets/UTF_8)
              len           (alength   class-name-ba)]

          (enc/cond
            (impl/sm-count? len) (do (write-id dout sc/id-sz-sm) (write-bytes-sm dout class-name-ba))
            (impl/md-count? len) (do (write-id dout sc/id-sz-md) (write-bytes-md dout class-name-ba))
            ;; :else             (do (write-id dout sc/id-sz-lg) (write-bytes-md dout class-name-ba)) ; Unrealistic
            :else                (truss/ex-info! "Serializable class name too long" {:name class-name}))

          ;; Serialize object to isolated ba, then write the length-prefixed ba to stream.
          ;; Can therefore later choose to skip OR deserialize with `readObject`.
          (let [baos  (ByteArrayOutputStream.)
                dos   (DataOutputStream. baos)
                _     (.writeObject (java.io.ObjectOutputStream. dos) x)
                sz-ba (.toByteArray baos)]
            (write-bytes dout sz-ba))

          true)))))

(defn write-readable [^DataOutput dout x]
  (impl/when-debug (println (str "write-readable: " (type x))))
  (when (impl/seems-readable? x)
    (let [edn    (enc/pr-edn  x)
          edn-ba (.getBytes ^String edn StandardCharsets/UTF_8)
          len    (alength edn-ba)]
      (enc/cond
        (impl/sm-count? len) (do (write-id dout sc/id-reader-sm) (write-bytes-sm dout edn-ba))
        (impl/md-count? len) (do (write-id dout sc/id-reader-md) (write-bytes-md dout edn-ba))
        :else                (do (write-id dout sc/id-reader-lg) (write-bytes-lg dout edn-ba)))
      true)))

(defn write-cached [^DataOutput dout x-val cache_]
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
        0 (do (write-id dout sc/id-cached-0) (when first-occurance? (write-typed+meta x-val dout)))
        1 (do (write-id dout sc/id-cached-1) (when first-occurance? (write-typed+meta x-val dout)))
        2 (do (write-id dout sc/id-cached-2) (when first-occurance? (write-typed+meta x-val dout)))
        3 (do (write-id dout sc/id-cached-3) (when first-occurance? (write-typed+meta x-val dout)))
        4 (do (write-id dout sc/id-cached-4) (when first-occurance? (write-typed+meta x-val dout)))
        5 (do (write-id dout sc/id-cached-5) (when first-occurance? (write-typed+meta x-val dout)))
        6 (do (write-id dout sc/id-cached-6) (when first-occurance? (write-typed+meta x-val dout)))
        7 (do (write-id dout sc/id-cached-7) (when first-occurance? (write-typed+meta x-val dout)))

        (do
          (write-id       dout sc/id-cached-sm)
          (write-sm-count dout idx)
          (when first-occurance? (write-typed+meta x-val dout))))

      (impl/md-count? idx)
      (do
        (write-id       dout sc/id-cached-md)
        (write-md-count dout idx)
        (when first-occurance? (write-typed+meta x-val dout)))

      :else
      ;; (truss/ex-info! "Max cache size exceeded" {:idx idx})
      (write-typed+meta x-val dout) ; Just freeze uncached
      )))

;;;;

(enc/declare-remote ^:dynamic taoensso.nippy/*incl-metadata?*)

(extend-protocol IWriteTypedWithMeta
  clojure.lang.IObj ; IMeta => `meta` will work, IObj => `with-meta` will work
  (write-typed+meta [x ^DataOutput dout]
    (when-let [m (when taoensso.nippy/*incl-metadata?* (not-empty (meta x)))]
      (write-id  dout sc/id-meta)
      (write-map dout m :is-metadata))
    (write-typed x dout))

  nil    (write-typed+meta [x dout] (write-typed x dout))
  Object (write-typed+meta [x dout] (write-typed x dout)))

(defmacro ^:private writer
  "Convenience util / short-hand."
  [atype id & impl-body]
  (let [atype      (or (when (symbol? atype) (when-let [r (resolve atype)] (when (var? r) @r))) atype)
        x          (with-meta 'x    {:tag atype})
        dout       (with-meta 'dout {:tag 'DataOutput})
        id-form    (when id `(write-id ~'dout ~id))
        freezable? (if (= atype 'Object) nil true)]

    `(extend ~atype
       impl/INativeFreezable {:native-freezable? (fn [~x      ] ~freezable?) }
       IWriteTypedNoMeta     {:write-typed       (fn [~x ~dout] ~id-form ~@impl-body)})))

(writer nil       sc/id-nil    nil)
(writer (type ()) sc/id-list-0 nil)

(writer Boolean              nil (if (boolean x) (write-id dout sc/id-true) (write-id dout sc/id-false)))
(writer String               nil (write-str dout x))
(writer clojure.lang.Keyword nil (write-kw  dout x))
(writer clojure.lang.Symbol  nil (write-sym dout x))

(writer Character sc/id-char    (.writeChar  dout (int x)))
(writer Byte      sc/id-byte    (.writeByte  dout x))
(writer Short     sc/id-short   (.writeShort dout x))
(writer Integer   sc/id-integer (.writeInt   dout x))
(writer Float     sc/id-float   (.writeFloat dout x))
(writer Long      nil           (write-long  dout x))
(writer Double    nil
  (if (zero? ^double x)
    (do (write-id dout sc/id-double-0))
    (do (write-id dout sc/id-double) (.writeDouble dout x))))

(writer BigInteger sc/id-biginteger (write-biginteger dout x))
(writer BigDecimal sc/id-bigdec
  (write-biginteger dout (.unscaledValue x))
  (.writeInt        dout (.scale         x)))

(writer clojure.lang.BigInt sc/id-bigint (write-biginteger dout (.toBigInteger x)))
(writer clojure.lang.Ratio  sc/id-ratio
  (write-biginteger dout (.numerator   x))
  (write-biginteger dout (.denominator x)))

(writer java.util.Date          sc/id-util-date (.writeLong dout (.getTime  x)))
(writer java.sql.Date           sc/id-sql-date  (.writeLong dout (.getTime  x)))
(writer java.net.URI            sc/id-uri       (write-str  dout (.toString x)))
(writer java.util.regex.Pattern sc/id-regex     (write-str  dout (.toString x)))
(writer java.util.UUID          sc/id-uuid
  (.writeLong dout (.getMostSignificantBits  x))
  (.writeLong dout (.getLeastSignificantBits x)))

(writer Cached nil
  (let [x-val (.-val x)]
    (if-let [cache_ (.get impl/tl:cache)]
      (write-cached     dout x-val cache_)
      (write-typed+meta x-val dout))))

(writer impl/array-class-bytes     nil (write-bytes    dout x))
(writer impl/array-class-objects   nil (write-array-lg dout x (alength ^objects x) sc/id-object-array-lg))

(when (impl/target-release>= 350)
  (writer impl/array-class-ints    nil (write-array-lg dout x (alength ^ints                  x) sc/id-int-array-lg))
  (writer impl/array-class-longs   nil (write-array-lg dout x (alength ^longs                 x) sc/id-long-array-lg))
  (writer impl/array-class-floats  nil (write-array-lg dout x (alength ^floats                x) sc/id-float-array-lg))
  (writer impl/array-class-doubles nil (write-array-lg dout x (alength ^doubles               x) sc/id-double-array-lg))
  (writer impl/array-class-strings nil (write-array-lg dout x (alength ^"[Ljava.lang.String;" x) sc/id-string-array-lg)))

(writer clojure.lang.MapEntry sc/id-map-entry
  (write-typed+meta (key x) dout)
  (write-typed+meta (val x) dout))

(writer clojure.lang.PersistentQueue    nil (write-counted-coll   dout sc/id-queue-lg      x))
(writer clojure.lang.PersistentTreeSet  nil (write-counted-coll   dout sc/id-sorted-set-lg x))
(writer clojure.lang.PersistentTreeMap  nil (write-kvs            dout sc/id-sorted-map-lg x))
(writer clojure.lang.APersistentVector  nil (write-vec            dout                     x))
(writer clojure.lang.APersistentSet     nil (write-set            dout                     x))
(writer clojure.lang.APersistentMap     nil (write-map            dout                     x false))
(writer clojure.lang.PersistentList     nil (write-counted-coll   dout sc/id-list-0 sc/id-list-sm sc/id-list-md sc/id-list-lg x))
(writer clojure.lang.LazySeq            nil (write-uncounted-coll dout  sc/id-seq-0  sc/id-seq-sm  sc/id-seq-md  sc/id-seq-lg x))
(writer clojure.lang.ISeq               nil (write-coll           dout  sc/id-seq-0  sc/id-seq-sm  sc/id-seq-md  sc/id-seq-lg x))
(writer clojure.lang.IRecord            nil
  (let [class-name    (.getName (class x)) ; Reflect
        class-name-ba (.getBytes class-name StandardCharsets/UTF_8)
        len           (alength   class-name-ba)]
    (enc/cond
      (impl/sm-count? len) (do (write-id dout sc/id-record-sm) (write-bytes-sm dout class-name-ba))
      (impl/md-count? len) (do (write-id dout sc/id-record-md) (write-bytes-md dout class-name-ba))
      ;; :else             (do (write-id dout sc/id-record-lg) (write-bytes-md dout class-name-ba)) ; Unrealistic
      :else                (truss/ex-info! "Record class name too long" {:name class-name}))

    (write-typed (into {} x) dout)))

(writer clojure.lang.IType nil
  (let [c (class x)]
    (write-id  dout sc/id-type)
    (write-str dout (.getName c))
    (run! (fn [^java.lang.reflect.Field f] (write-typed (.get f x) dout))
      (impl/get-basis-fields c))))

(enc/compile-if java.time.Instant
  (writer       java.time.Instant sc/id-time-instant
    (.writeLong dout (.getEpochSecond x))
    (.writeInt  dout (.getNano        x))))

(enc/compile-if java.time.Duration
  (writer       java.time.Duration sc/id-time-duration
    (.writeLong dout (.getSeconds x))
    (.writeInt  dout (.getNano    x))))

(enc/compile-if java.time.Period
  (writer       java.time.Period sc/id-time-period
    (.writeInt dout (.getYears  x))
    (.writeInt dout (.getMonths x))
    (.writeInt dout (.getDays   x))))

(enc/declare-remote
  ^:dynamic taoensso.nippy/*freeze-fallback*
  ^:dynamic taoensso.nippy/*final-freeze-fallback*)

(writer Object nil
  (impl/when-debug (println (str "freeze-fallback: " (type x))))
  (if-let [ff taoensso.nippy/*freeze-fallback*]
    (if-not (identical? ff :write-unfreezable)
      (ff dout x) ; Modern approach with ff
      (or         ; Legacy approach with ff
        (truss/catching (write-sz       dout x))
        (truss/catching (write-readable dout x))
        (write-typed  (impl/wrap-unfreezable x) dout)))

    ;; Without ff
    (enc/cond
      :let [[r1 e1] (try [(write-sz       dout x)] (catch Throwable t [nil t]))], r1 r1
      :let [[r2 e2] (try [(write-readable dout x)] (catch Throwable t [nil t]))], r2 r2

      :if-let [fff taoensso.nippy/*final-freeze-fallback*] (fff dout x) ; Deprecated
      :else
      (let [t (type x)]
        (truss/ex-info! (str "Failed to freeze type: " t)
          (enc/assoc-some
            {:type   t
             :as-str (impl/try-pr-edn x)}
            {:serializable-error e1
             :readable-error     e2})
          (or e1 e2))))))

;;;; Reading

(declare read-typed) ; Main read fn, type determined by prefix

(defmacro read-sm-ucount [din] `(- (.readByte  ~din) Byte/MIN_VALUE)) ; Unsigned
(defmacro read-sm-count  [din]    `(.readByte  ~din))
(defmacro read-md-count  [din]    `(.readShort ~din))
(defmacro read-lg-count  [din]    `(.readInt   ~din))

(declare read-bytes)
(defn    read-bytes-sm* [^DataInput din] (read-bytes din (read-sm-ucount din)))
(defn    read-bytes-sm  [^DataInput din] (read-bytes din (read-sm-count  din)))
(defn    read-bytes-md  [^DataInput din] (read-bytes din (read-md-count  din)))
(defn    read-bytes-lg  [^DataInput din] (read-bytes din (read-lg-count  din)))
(defn    read-bytes
  ([^DataInput din len] (let [ba (byte-array len)] (.readFully din ba 0 len) ba))
  ([^DataInput din    ]
   (enc/case-eval (.readByte din)
     sc/id-byte-array-0  (byte-array 0)
     sc/id-byte-array-sm (read-bytes din (read-sm-count din))
     sc/id-byte-array-md (read-bytes din (read-md-count din))
     sc/id-byte-array-lg (read-bytes din (read-lg-count din)))))

(defn read-str-sm* [^DataInput din] (String. ^bytes (read-bytes din (read-sm-ucount din)) StandardCharsets/UTF_8))
(defn read-str-sm  [^DataInput din] (String. ^bytes (read-bytes din (read-sm-count  din)) StandardCharsets/UTF_8))
(defn read-str-md  [^DataInput din] (String. ^bytes (read-bytes din (read-md-count  din)) StandardCharsets/UTF_8))
(defn read-str-lg  [^DataInput din] (String. ^bytes (read-bytes din (read-lg-count  din)) StandardCharsets/UTF_8))
(defn read-str
  ([^DataInput din len] (String. ^bytes (read-bytes din len) StandardCharsets/UTF_8))
  ([^DataInput din    ]
   (enc/case-eval (.readByte din)
     sc/id-str-0  ""
     sc/id-str-sm* (String. ^bytes (read-bytes din (read-sm-ucount din)) StandardCharsets/UTF_8)
     sc/id-str-sm_ (String. ^bytes (read-bytes din (read-sm-count  din)) StandardCharsets/UTF_8)
     sc/id-str-md  (String. ^bytes (read-bytes din (read-md-count  din)) StandardCharsets/UTF_8)
     sc/id-str-lg  (String. ^bytes (read-bytes din (read-lg-count  din)) StandardCharsets/UTF_8))))

(defn read-biginteger [^DataInput din] (BigInteger. ^bytes (read-bytes din (.readInt din))))

(defmacro read-array [din thaw-type array-type array]
  (let [thawed-sym (with-meta 'thawed-sym {:tag thaw-type})
        array-sym  (with-meta 'array-sym  {:tag array-type})]
    `(let [~array-sym ~array]
       (enc/reduce-n
         (fn [_# idx#]
           (let [~thawed-sym (read-typed ~din)]
             (aset ~'array-sym idx# ~'thawed-sym)))
         nil (alength ~'array-sym))
       ~'array-sym)))

(enc/declare-remote ^:dynamic taoensso.nippy/*thaw-xform*)

(let [rf! (fn rf! ([x] (persistent! x)) ([acc x] (conj! acc x)))
      rf* (fn rf* ([x]              x)  ([acc x] (conj  acc x)))]

  (defn read-into [to ^DataInput din ^long n]
    (let [transient? (when (impl/editable? to) (> n 10))
          init       (if transient? (transient to) to)
          rf         (if transient? rf! rf*)
          rf         (if-let [xf taoensso.nippy/*thaw-xform*] ((impl/xform* xf) rf) rf)]

      (rf (enc/reduce-n (fn [acc _] (rf acc (read-typed din))) init n)))))

(let [rf1! (fn rf1! ([x] (persistent! x)) ([acc kv ] (assoc! acc (key kv) (val kv))))
      rf2! (fn rf2! ([x] (persistent! x)) ([acc k v] (assoc! acc      k         v)))
      rf1* (fn rf1* ([x]              x)  ([acc kv ] (assoc  acc (key kv) (val kv))))
      rf2* (fn rf2* ([x]              x)  ([acc k v] (assoc  acc      k         v)))]

  (defn read-kvs-into [to ^DataInput din ^long n]
    (let [transient? (when (impl/editable? to) (> n 10))
          init       (if transient? (transient to) to)
          rf1        (if transient? rf1! rf1*)
          rf2        (if transient? rf2! rf2*)]

      (if-let [xf taoensso.nippy/*thaw-xform*]
        (let [rf ((impl/xform* xf) rf1)] (rf (enc/reduce-n (fn [acc _] (rf acc (enc/map-entry (read-typed din) (read-typed din)))) init n)))
        (let [rf                   rf2 ] (rf (enc/reduce-n (fn [acc _] (rf acc                (read-typed din) (read-typed din)))  init n)))))))

(defn read-kvs-depr [to ^DataInput din] (read-kvs-into to din (quot (.readInt din) 2)))

(enc/declare-remote ^:dynamic taoensso.nippy/*custom-readers*)

(defn read-custom [din prefixed? type-id]
  (if-let [custom-reader (get taoensso.nippy/*custom-readers* type-id)]
    (try
      (custom-reader din)
      (catch Exception e
        (truss/ex-info!
          (str "Reader exception for custom type id: " type-id)
          {:type-id type-id, :prefixed? prefixed?} e)))

    (truss/ex-info!
      (str "No reader provided for custom type id: " type-id)
      {:type-id type-id, :prefixed? prefixed?})))

(defn read-sz!!
  "Reads object using Java `Serializable`. May be unsafe!"
  [^DataInput din class-name]
  (try
    (let [obj (.readObject (java.io.ObjectInputStream. din))] ; May be unsafe!
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

(defn read-sz [^DataInput din class-name legacy?]
  (if legacy?

    ;; Serialized object directly to stream WITHOUT length prefix
    (if (impl/thaw-serializable-allowed? class-name)
      (read-sz!! din class-name)
      (truss/ex-info! ; No way to skip bytes, so best we can do is throw
        "Cannot thaw object: `taoensso.nippy/*thaw-serializable-allowlist*` check failed. This is a security feature. See `*thaw-serializable-allowlist*` docstring or https://github.com/ptaoussanis/nippy/issues/130 for details!"
        {:class-name class-name}))

    (let [sz-ba (read-bytes din)]
      (if (impl/thaw-serializable-allowed? class-name)
        (read-sz!! (DataInputStream. (ByteArrayInputStream. sz-ba)) class-name)
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
          (read-sz!! (DataInputStream. (ByteArrayInputStream. sz-ba)) class-name))))))

(let [class-method-sig (into-array Class [clojure.lang.IPersistentMap])]
  (defn read-record [din class-name]
    (let [content (read-typed din)]
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

(defn read-type [din class-name]
  (try
    (let [c          (clojure.lang.RT/classForName class-name)
          num-fields (count (impl/get-basis-fields c))
          field-vals (object-array num-fields)

          ;; Ref. <https://github.com/clojure/clojure/blob/e78519c174fb506afa70e236af509e73160f022a/src/jvm/clojure/lang/Compiler.java#L4799>
          ^java.lang.reflect.Constructor ctr (aget (.getConstructors c) 0)]

      (enc/reduce-n
        (fn [_ i] (aset field-vals i (read-typed din)))
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
  "Reads one object as type-prefixed bytes from given `DataInput`."
  [^DataInput din]
  (let [type-id (.readByte din)]
    (impl/when-debug (println (str "read-typed: " type-id)))
    (try
      (enc/case-eval type-id

        sc/id-nil               nil
        sc/id-true              true
        sc/id-false             false
        sc/id-meta-protocol-key impl/meta-protocol-key

        sc/id-reader-sm  (impl/read-edn   (read-str din (read-sm-count din)))
        sc/id-reader-md  (impl/read-edn   (read-str din (read-md-count din)))
        sc/id-reader-lg  (impl/read-edn   (read-str din (read-lg-count din)))
        sc/id-reader-lg_ (impl/read-edn   (read-str din (read-lg-count din)))
        sc/id-record-sm  (read-record din (read-str din (read-sm-count din)))
        sc/id-record-md  (read-record din (read-str din (read-md-count din)))
        sc/id-record-lg_ (read-record din (read-str din (read-lg-count din)))

        sc/id-sz-sm  (read-sz din (read-str din (read-sm-count din)) false)
        sc/id-sz-md  (read-sz din (read-str din (read-md-count din)) false)
        sc/id-sz-sm_ (read-sz din (read-str din (read-sm-count din)) :legacy)
        sc/id-sz-md_ (read-sz din (read-str din (read-md-count din)) :legacy)
        sc/id-sz-lg_ (read-sz din (read-str din (read-lg-count din)) :legacy)

        sc/id-type (read-type din (read-typed din))
        sc/id-char (.readChar din)

        sc/id-meta
        (let [m (read-typed din) ; Always consume from stream
              x (read-typed din)]
          (if-let [m (when taoensso.nippy/*incl-metadata?* (not-empty (dissoc m impl/meta-protocol-key)))]
            (with-meta x m)
            (do        x)))

        sc/id-cached-0  (impl/read-cached read-typed 0 din)
        sc/id-cached-1  (impl/read-cached read-typed 1 din)
        sc/id-cached-2  (impl/read-cached read-typed 2 din)
        sc/id-cached-3  (impl/read-cached read-typed 3 din)
        sc/id-cached-4  (impl/read-cached read-typed 4 din)
        sc/id-cached-5  (impl/read-cached read-typed 5 din)
        sc/id-cached-6  (impl/read-cached read-typed 6 din)
        sc/id-cached-7  (impl/read-cached read-typed 7 din)
        sc/id-cached-sm (impl/read-cached read-typed (read-sm-count din) din)
        sc/id-cached-md (impl/read-cached read-typed (read-md-count din) din)

        sc/id-byte-array-0    (byte-array 0)
        sc/id-byte-array-sm   (read-bytes din (read-sm-count din))
        sc/id-byte-array-md   (read-bytes din (read-md-count din))
        sc/id-byte-array-lg   (read-bytes din (read-lg-count din))

        sc/id-long-array-lg   (read-array din long   "[J" (long-array   (read-lg-count din)))
        sc/id-int-array-lg    (read-array din int    "[I" (int-array    (read-lg-count din)))

        sc/id-double-array-lg (read-array din double "[D" (double-array (read-lg-count din)))
        sc/id-float-array-lg  (read-array din float  "[F" (float-array  (read-lg-count din)))

        sc/id-string-array-lg (read-array din String "[Ljava.lang.String;" (make-array String (read-lg-count din)))
        sc/id-object-array-lg (read-array din Object "[Ljava.lang.Object;" (object-array      (read-lg-count din)))

        sc/id-str-0       ""
        sc/id-str-sm*              (read-str din (read-sm-ucount din))
        sc/id-str-sm_              (read-str din (read-sm-count  din))
        sc/id-str-md               (read-str din (read-md-count  din))
        sc/id-str-lg               (read-str din (read-lg-count  din))

        sc/id-kw-sm       (keyword (read-str din (read-sm-count din)))
        sc/id-kw-md       (keyword (read-str din (read-md-count din)))
        sc/id-kw-md_      (keyword (read-str din (read-lg-count din)))
        sc/id-kw-lg_      (keyword (read-str din (read-lg-count din)))

        sc/id-sym-sm      (symbol  (read-str din (read-sm-count din)))
        sc/id-sym-md      (symbol  (read-str din (read-md-count din)))
        sc/id-sym-md_     (symbol  (read-str din (read-lg-count din)))
        sc/id-sym-lg_     (symbol  (read-str din (read-lg-count din)))
        sc/id-regex       (re-pattern               (read-typed din))

        sc/id-vec-0       []
        sc/id-vec-2       (read-into [] din 2)
        sc/id-vec-3       (read-into [] din 3)
        sc/id-vec-sm*     (read-into [] din (read-sm-ucount din))
        sc/id-vec-sm_     (read-into [] din (read-sm-count  din))
        sc/id-vec-md      (read-into [] din (read-md-count  din))
        sc/id-vec-lg      (read-into [] din (read-lg-count  din))

        sc/id-set-0       #{}
        sc/id-set-sm*     (read-into    #{} din (read-sm-ucount din))
        sc/id-set-sm_     (read-into    #{} din (read-sm-count  din))
        sc/id-set-md      (read-into    #{} din (read-md-count  din))
        sc/id-set-lg      (read-into    #{} din (read-lg-count  din))

        sc/id-map-0       {}
        sc/id-map-sm*     (read-kvs-into {} din (read-sm-ucount din))
        sc/id-map-sm_     (read-kvs-into {} din (read-sm-count  din))
        sc/id-map-md      (read-kvs-into {} din (read-md-count  din))
        sc/id-map-lg      (read-kvs-into {} din (read-lg-count  din))

        sc/id-queue-lg      (read-into     clojure.lang.PersistentQueue/EMPTY din (read-lg-count din))
        sc/id-sorted-set-lg (read-into     (sorted-set)                       din (read-lg-count din))
        sc/id-sorted-map-lg (read-kvs-into (sorted-map)                       din (read-lg-count din))

        sc/id-list-0            ()
        sc/id-list-sm     (into () (rseq (read-into [] din (read-sm-count din))))
        sc/id-list-md     (into () (rseq (read-into [] din (read-md-count din))))
        sc/id-list-lg     (into () (rseq (read-into [] din (read-lg-count din))))

        sc/id-seq-0       (lazy-seq nil)
        sc/id-seq-sm      (or (seq (read-into [] din (read-sm-count din))) (lazy-seq nil))
        sc/id-seq-md      (or (seq (read-into [] din (read-md-count din))) (lazy-seq nil))
        sc/id-seq-lg      (or (seq (read-into [] din (read-lg-count din))) (lazy-seq nil))

        sc/id-byte              (.readByte  din)
        sc/id-short             (.readShort din)
        sc/id-integer           (.readInt   din)
        sc/id-long-0      0
        sc/id-long-sm_    (long (.readByte  din))
        sc/id-long-md_    (long (.readShort din))
        sc/id-long-lg_    (long (.readInt   din))
        sc/id-long-xl           (.readLong  din)

        sc/id-long-pos-sm    (- (long (.readByte  din))    Byte/MIN_VALUE)
        sc/id-long-pos-md    (- (long (.readShort din))   Short/MIN_VALUE)
        sc/id-long-pos-lg    (- (long (.readInt   din)) Integer/MIN_VALUE)

        sc/id-long-neg-sm (- (- (long (.readByte  din))    Byte/MIN_VALUE))
        sc/id-long-neg-md (- (- (long (.readShort din))   Short/MIN_VALUE))
        sc/id-long-neg-lg (- (- (long (.readInt   din)) Integer/MIN_VALUE))

        sc/id-bigint      (bigint (read-biginteger din))
        sc/id-biginteger          (read-biginteger din)

        sc/id-float       (.readFloat  din)
        sc/id-double-0    0.0
        sc/id-double      (.readDouble din)

        sc/id-bigdec      (BigDecimal. ^BigInteger (read-biginteger din) (.readInt        din))
        sc/id-ratio       (clojure.lang.Ratio.     (read-biginteger din) (read-biginteger din))

        sc/id-map-entry   (enc/map-entry (read-typed din) (read-typed din))

        sc/id-util-date   (java.util.Date. (.readLong din))
        sc/id-sql-date    (java.sql.Date.  (.readLong din))
        sc/id-uuid        (java.util.UUID. (.readLong din) (.readLong din))
        sc/id-uri         (java.net.URI.  (read-typed din))

        sc/id-prefixed-custom-md (read-custom din :prefixed (.readShort din))

        sc/id-time-instant
        (let [secs  (.readLong din)
              nanos (.readInt  din)]

          (enc/compile-if java.time.Instant
            (java.time.Instant/ofEpochSecond secs nanos)
            {:nippy/unthawable
             {:type  :class
              :cause :class-not-found

              :class-name "java.time.Instant"
              :content    {:epoch-second secs :nano nanos}}}))

        sc/id-time-duration
        (let [secs  (.readLong din)
              nanos (.readInt  din)]

          (enc/compile-if java.time.Duration
            (java.time.Duration/ofSeconds secs nanos)
            {:nippy/unthawable
             {:type  :class
              :cause :class-not-found

              :class-name "java.time.Duration"
              :content    {:seconds secs :nanos nanos}}}))

        sc/id-time-period
        (let [years  (.readInt din)
              months (.readInt din)
              days   (.readInt din)]

          (enc/compile-if java.time.Period
            (java.time.Period/of years months days)
            {:nippy/unthawable
             {:type  :class
              :cause :class-not-found

              :class-name "java.time.Period"
              :content    {:years years :months months :days days}}}))

        ;; Deprecated ------------------------------------------------------
        sc/id-boolean_    (.readBoolean din)
        sc/id-sorted-map_ (read-kvs-depr (sorted-map) din)
        sc/id-map__       (read-kvs-depr {} din)
        sc/id-reader_     (impl/read-edn (.readUTF din))
        sc/id-str_                       (.readUTF din)
        sc/id-kw_               (keyword (.readUTF din))
        sc/id-map_
        (apply hash-map
          (enc/repeatedly-into [] (* 2 (.readInt din))
            (fn [] (read-typed din))))
        ;; -----------------------------------------------------------------

        ;; else
        (if (neg? type-id)
          (read-custom din nil type-id) ; Unprefixed custom type
          (truss/ex-info!
            (str "Unrecognized type id (" type-id "). Data frozen with newer Nippy version?")
            {:type-id type-id})))

      (catch Throwable t
        (truss/ex-info! (str "Thaw failed against type-id: " type-id)
          {:type-id type-id} t)))))

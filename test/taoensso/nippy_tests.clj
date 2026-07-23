(ns taoensso.nippy-tests
  (:require
   [clojure.java.io :as jio]
   [clojure.test :as test :refer [deftest testing is]]
   [clojure.test.check            :as tc]
   [clojure.test.check.generators :as tc-gens]
   [clojure.test.check.properties :as tc-props]
   [taoensso.truss                :as truss :refer [throws?]]
   [taoensso.encore               :as enc   :refer [ba=]]
   [taoensso.nippy-benchmarks     :as benchmarks]
   [taoensso.nippy                :as nippy :refer [freeze thaw]]
   [taoensso.nippy
    [schema      :as sc]
    [impl        :as impl]
    [io          :as io]
    [compression :as compr]
    [encryption  :as encry]
    [tools       :as tools]
    [crypto      :as crypto]]))

(comment
  (remove-ns      'taoensso.nippy-tests)
  (test/run-tests 'taoensso.nippy-tests))

;;;; Config, etc.

(def test-data (nippy/stress-data {:comparable? true}))

(def historical-v362-data
  {:nil         nil
   :booleans    [true false]
   :numbers     [(byte -7) (short 30000) 42 1234567890123456789
                 123456789012345678901234567890N 22/7 (float 1.25) 1.25M]
   :text        ["Nippy \u2603" :nippy/keyword 'nippy/symbol \u03bb]
   :collections {:vector [1 2 3]
                 :list   '(:a :b :c)
                 :set    #{:a :b :c}
                 :map    {:nested {:x 1 :y 2}}}
   :meta        (with-meta [:payload] {:source :v3.6.2})
   :date        (java.util.Date. 1700000000000)
   :uuid        (java.util.UUID. 123 456)
   :uri         (java.net.URI. "https://example.com/nippy")})

(defn- read-resource-bytes ^bytes [resource-name]
  (with-open [in  (jio/input-stream (jio/resource resource-name))
              out (java.io.ByteArrayOutputStream.)]
    (jio/copy in  out)
    (.toByteArray out)))

(comment
  [(nippy/freeze-to-file (java.io.File. "test/data/v3.8.0.npy") test-data)
   (nippy/thaw-from-resource "data/v3.8.0.npy")
   (nippy/thaw-from-resource "data/v3.8.0-RC1.npy")
   (nippy/thaw-from-resource "data/v3.7.0.npy")
   (nippy/thaw-from-resource "data/v3.7.0-RC3.npy")
   (nippy/thaw-from-resource "data/v3.7.0-RC2.npy")
   (nippy/thaw-from-resource "data/v3.7.0-RC1.npy")
   (nippy/thaw-from-resource "data/v3.7.0-beta1.npy")
   (nippy/thaw-from-resource "data/v3.6.2.npy")])

(deftest _historical-fixtures
  [(doseq [version ["v3.7.0-beta1" "v3.7.0-RC1" "v3.7.0-RC2" "v3.7.0-RC3" "v3.7.0" "v3.8.0-RC1" "v3.8.0"]]
     (is (= test-data (nippy/thaw-from-resource (str "data/" version ".npy"))) version))

   (let [legacy (nippy/thaw-from-resource "data/v3.6.2.npy")]
     (is (=
           (dissoc test-data :deftype :defrecord)
           (dissoc legacy    :deftype :defrecord))
       "v3.6.2 stable values"))

   (let [legacy (nippy/thaw-from-resource "data/v3.6.2-compat.npy")]
     [(is (= historical-v362-data legacy) "v3.6.2 focused values")
      (is (=
            (meta (:meta historical-v362-data))
            (meta (:meta legacy)))
        "v3.6.2 focused metadata")])])

(def tc-gen-recursive-any-equatable
  (tc-gens/recursive-gen tc-gens/container-type
    tc-gens/any-equatable))

(defmacro gen-test [num-tests [data-sym] & body]
  `(let [tc-result#
         (tc/quick-check ~num-tests
           (tc-props/for-all [~data-sym tc-gen-recursive-any-equatable]
             ~@body))]
     (true? (:pass? tc-result#))))

(comment
  (tc-gens/sample tc-gen-recursive-any-equatable 10)
  (gen-test 10 [gen-data] true))

(defn array= [x y] (java.util.Arrays/equals ^objects (to-array x) ^objects (to-array y)))

(comment (array= [1 2 3 ##NaN ##Inf] [1 2 3 ##NaN ##Inf])) ; true

(defn freeze-raw-map [entries]
  (io/with-bb 512
    (fn [^java.nio.ByteBuffer bb dout_]
      (io/write-id        bb sc/id-map-sm*)
      (io/write-sm-ucount bb (count entries))
      (run!
        (fn [[k v]]
          (io/write-typed+meta k bb dout_)
          (io/write-typed+meta v bb dout_))
        entries)
      true)))

;;;; Core

(deftest _core
  (println (str "Clojure version: " *clojure-version*))
  (println (str "Target release: " impl/target-release))
  [(is (= test-data test-data) "Test data is comparable")
   (is (=
         (nippy/stress-data {:comparable? true})
         (nippy/stress-data {:comparable? true}))
     "Stress data is deterministic")

   (is (= test-data ((comp thaw freeze) test-data)))
   (is (= test-data ((comp #(thaw   % {:no-header? true
                                       :compressor nippy/lz4-compressor
                                       :encryptor  nil})
                           #(freeze % {:no-header? true}))
                     test-data)))

   (is (= test-data ((comp #(thaw   % {:password [:salted "p"]})
                           #(freeze % {:password [:salted "p"]}))
                     test-data)))

   (is (= test-data ((comp #(thaw   % {:compressor nippy/lzma2-compressor})
                           #(freeze % {:compressor nippy/lzma2-compressor}))
                     test-data)))

   (is (= test-data ((comp #(thaw   % {:compressor nippy/lzma2-compressor
                                       :password [:salted "p"]})
                           #(freeze % {:compressor nippy/lzma2-compressor
                                       :password [:salted "p"]}))
                     test-data)))

   (is (= test-data ((comp #(thaw   % {:compressor nippy/lz4-compressor})
                           #(freeze % {:compressor nippy/lz4-compressor}))
                     test-data)))

   (is (= test-data ((comp #(thaw   % {:compressor nippy/zstd-compressor})
                           #(freeze % {:compressor nippy/zstd-compressor}))
                     test-data)))

   (is (throws? Exception (thaw (freeze test-data {:password "malformed"}))))
   (is (throws? Exception (thaw (freeze test-data {:password [:salted "p"]}))))
   (is (throws? Exception (thaw (freeze test-data {:password [:salted "p"]}))))

   (is
     (= "payload"
       (thaw (freeze "payload" {:password [:salted "pwd"] :encryptor nippy/aes128-cbc-encryptor})
         (do                   {:password [:salted "pwd"]})))
     "CBC auto-encryptor compatibility")

   (testing "Encrypted auto-compression"
     (let [x        (String. (char-array 9000 \u2603))
           password [:salted "p"]
           frozen   (freeze x {:compressor :auto
                               :encryptor nippy/aes128-gcm-encryptor
                               :password password})
           [_ head-meta] (sc/try-parse-header frozen)]
       [(is (= {:compressor-id :lz4
                :encryptor-id  :aes128-gcm-sha512}
               (select-keys head-meta [:compressor-id :encryptor-id])))
        (is (= x (thaw frozen {:password password})))]))

   (testing "Truncated encrypted payloads"
     ;; Ciphertext shorter than its iv+salt prefix must be rejected up front,
     ;; before any (needless) key derivation
     (let [derivations (volatile! 0)
           salt->key   (fn [salt-ba]
                         (vswap! derivations inc)
                         (crypto/take-ba 16 (crypto/sha512-key-ba salt-ba "pwd")))
           iv-size     (long (crypto/get-iv-size crypto/cipher-kit-aes-gcm))
           try-decrypt (fn [salt-size len]
                         (vreset! derivations 0)
                         [(truss/throws :default
                            (crypto/decrypt
                              {:salt-size    salt-size
                               :salt->key-fn salt->key
                               :enc-ba       (byte-array (int len))}))
                          @derivations])]

       (into []
         (for [salt-size [0 16], len [0 1 (dec (+ iv-size salt-size))]]
           (let [[e n-derivations] (try-decrypt salt-size len)]
             [(is (instance? java.io.EOFException e)
                (str "salt-size " salt-size ", length " len))
              (is (zero? n-derivations)
                (str "no key derivation for salt-size " salt-size ", length " len))])))))

   (testing "Unsigned long types"
     (let [range-ushort+ (+ (long impl/range-ushort) 128)
           range-uint+   (+ (long impl/range-uint)   128)]

       [(let [r (range (long -2.5e6) (long 2.5e6))]      (= (thaw (freeze r)) r))
        (let [r (range (- range-ushort+) range-ushort+)] (= (thaw (freeze r)) r))
        (let [n    range-uint+]                          (= (thaw (freeze n)) n))
        (let [n (- range-uint+)]                         (= (thaw (freeze n)) n))]))

   (is (throws? :ex-info "Failed to freeze type" (nippy/freeze (fn []))))

   (testing "Clojure v1.10+ metadata protocol extensions"
     [(is (throws? :ex-info "Failed to freeze type" (nippy/freeze (with-meta [] {:a :A, 'b   (fn [])}))))
      (is (= {:a :A}              (meta (nippy/thaw (nippy/freeze (with-meta [] {:a :A, 'b/c (fn [])}))))))
      (is (= nil                  (meta (nippy/thaw (nippy/freeze (with-meta [] {       'b/c (fn [])})))))
        "Don't attach empty metadata")])

   (let [d (nippy/stress-data {})]
     [(is (= (vec (:bytes   d)) ((comp vec thaw freeze) (:bytes   d))))
      (is (= (vec (:objects d)) ((comp vec thaw freeze) (:objects d))))])

   (testing "Arrays"
     (binding [nippy/*thaw-serializable-allowlist* nippy/default-freeze-serializable-allowlist]
       (mapv (fn [[k aval]] (is (array= aval (-> aval nippy/freeze nippy/thaw)) (name k)))
         (get-in (nippy/stress-data {}) [:non-comparable :arrays]))))

   (is (gen-test 1600 [gen-data] (= gen-data (thaw (freeze gen-data)))) "Generative")])

(deftest _generic-map-thaw
  (let [kvs-n  (fn [n] (mapv (fn [idx] [(keyword (str "rt" idx)) idx]) (range n)))
        thaw-n (fn [n] (nippy/fast-thaw (freeze-raw-map (kvs-n n))))
        rt-map (fn [n] (clojure.lang.RT/map (object-array (mapcat identity (kvs-n n)))))]

    [(testing "Medium maps use bounded RT/map construction"
       (let [thawed (thaw-n 12), expected (rt-map 12)]
         [(is (= expected thawed))
          (is (= (class expected) (class thawed)))]))

     (testing "Construction bounds"
       [(is (= (class (rt-map 11)) (class (thaw-n 11))))
        (is (= (class (rt-map 64)) (class (thaw-n 64))))
        (is (= (into {} (kvs-n 64)) (thaw-n 64)))
        (is (instance? clojure.lang.PersistentHashMap (thaw-n 65)))
        (is (= (into {} (kvs-n 65)) (thaw-n 65)))])

     (testing "Duplicate thawed keys fall back to assoc semantics"
       (let [entries (conj (kvs-n 12) [:rt0 99])]
         (is (= (assoc (into {} (kvs-n 12)) :rt0 99)
               (nippy/fast-thaw (freeze-raw-map entries))))))

     (testing "Thaw transforms use generic construction"
       (is (=
             (dissoc (into {} (kvs-n 12)) :rt1)
             (binding [nippy/*thaw-xform* (remove (fn [x] (and (map-entry? x) (= (key x) :rt1))))]
               (thaw-n 12)))))]))

(deftype ByteBufferUTFType [s])

(nippy/extend-freeze java.sql.Time :nippy-tests/sql-time [x out] (.writeLong out (.getTime x)))
(nippy/extend-thaw                 :nippy-tests/sql-time [in] (java.sql.Time. (.readLong in)))

(deftest _byte-buffer-low-level
  [(testing "DataInput/DataOutput adapters"
     (let [x  (with-meta {:k "v" :nums [1 2 3]} {:meta? true})
           bb (java.nio.ByteBuffer/allocate 2048)]
       (nippy/freeze-to-out! (io/bb->dout bb) x)
       (let [written (.position bb)]
         (.flip bb)
         (is (pos? written))
         (is (= x (nippy/thaw-from-in! (io/bb->din bb))))
         (is (= written (.position bb))))))

   (testing "`freeze-to-out!` writes exactly once when its buffer must grow"
     (let [x    (vec (range 50000)) ; Big enough to force several buffer grows
           baos (java.io.ByteArrayOutputStream.)]
       (nippy/freeze-to-out! (java.io.DataOutputStream. baos) x)
       (let [ba (.toByteArray baos)]
         (is (= (alength ba) (alength (nippy/fast-freeze x)))
           "Discarded writes from grown buffers aren't re-emitted")
         (is (= x (nippy/thaw-from-in!
                    (java.io.DataInputStream.
                      (java.io.ByteArrayInputStream. ba))))))))

   (testing "ByteBuffer entry points"
     (let [bb (java.nio.ByteBuffer/allocate 2048)]
       (nippy/freeze-to-bb! bb :a)
       (nippy/freeze-to-bb! bb {:b [1 2 3]})
       (let [written (.position bb)]
         (.flip bb)
         (is (= :a           (nippy/thaw-from-bb! bb)))
         (is (= {:b [1 2 3]} (nippy/thaw-from-bb! bb)))
         (is (= written (.position bb))))))

   (testing "Sliced heap buffers"
     (let [x      "Nippy-\u00e9-\u2603-\ud83d\ude80"
           ba     (nippy/fast-freeze x)
           prefix 7
           bb     (doto (java.nio.ByteBuffer/allocate (+ prefix (alength ba)))
                    (.position prefix)
                    (.put ba)
                    (.flip)
                    (.position prefix))
           slice  (.slice bb)]
       [(is (pos? (.arrayOffset slice)))
        (is (= x (nippy/thaw-from-bb! slice)))
        (is (= (alength ba) (.position slice)))]))

   (testing "Type fidelity for date variants"
     (let [x  (java.sql.Date. 1700000000000)
           bb (java.nio.ByteBuffer/allocate 256)]
       (nippy/freeze-to-bb! bb x)
       (.flip bb)
       (let [y (nippy/thaw-from-bb! bb)]
         (is (instance? java.sql.Date y))
         (is (= x y)))))

   (testing "Custom extensions override inherited native writers"
     (let [x      (java.sql.Time. 1700000000123)
           raw-ba (nippy/fast-freeze x)]
       [(is (= (int (aget raw-ba 0)) sc/id-prefixed-custom-md))
        (is (instance? java.sql.Time (nippy/fast-thaw raw-ba)))
        (is (= x (nippy/fast-thaw raw-ba)))
        (is (instance? java.sql.Time (thaw (freeze x))))
        (is (= x (thaw (freeze x))))
        (is (= java.util.Date (class (thaw (freeze (java.util.Date. 1700000000123))))))]))

   (testing "UTF methods for custom extensions"
     (nippy/extend-freeze ByteBufferUTFType :nippy-tests/byte-buffer-utf [x out]
       (.writeUTF out (.s x)))

     (nippy/extend-thaw :nippy-tests/byte-buffer-utf [in]
       (ByteBufferUTFType. (.readUTF in)))

     (let [x (ByteBufferUTFType. "abc-\u0000-\u07FF-\u0800")
           bb (java.nio.ByteBuffer/allocate 2048)]
       (nippy/freeze-to-bb! bb x)
       (.flip bb)
       (let [^ByteBufferUTFType y (nippy/thaw-from-bb! bb)]
         (is (= (.s x) (.s y))))))

   (testing "`freeze-to-bb!` leaves nothing behind on failure"
     [(let [bb (java.nio.ByteBuffer/allocate 64)]
        (nippy/freeze-to-bb! bb :ok)
        (let [pos (.position bb)]
          (is (throws? java.io.EOFException
                (nippy/freeze-to-bb! bb (apply str (repeat 1000 "x")))))
          (is (= pos (.position bb)) "Buffer position restored")
          ;; So the buffer is still safe to keep writing to
          (nippy/freeze-to-bb! bb :also-ok)
          (.flip bb)
          (is (= [:ok :also-ok] [(nippy/thaw-from-bb! bb) (nippy/thaw-from-bb! bb)])
            "Buffer still usable")))

      ;; An abandoned write must not register cache idxs, else the NEXT write
      ;; emits a bare ref for a value that was never written
      (nippy/with-cache
        (let [big (apply str (repeat 500 "y"))
              bb  (java.nio.ByteBuffer/allocate 4096)]
          (is (throws? java.io.EOFException
                (nippy/freeze-to-bb! (java.nio.ByteBuffer/allocate 8) (nippy/cache big))))
          (nippy/freeze-to-bb! bb [(nippy/cache big) (nippy/cache big)])
          (.flip bb)
          (is (= [big big] (nippy/thaw-from-bb! bb)) "Session cache not poisoned")))])

   (testing "Bounds and byte-order checks"
     [(is (throws? java.io.EOFException (nippy/freeze-to-bb! (java.nio.ByteBuffer/allocate 1) "too large"))        "BB overflow")
      (is (throws? java.io.EOFException (nippy/freeze-to-bb! (java.nio.ByteBuffer/allocate 1) (object-array 128))) "BB overflow via `write-sz`")
      (is (throws? IllegalArgumentException
            (io/bb->din
              (doto    (java.nio.ByteBuffer/allocate 8)
                (.order java.nio.ByteOrder/LITTLE_ENDIAN)))))

      (is (throws? IllegalArgumentException
            (io/bb->dout
              (doto    (java.nio.ByteBuffer/allocate 8)
                (.order java.nio.ByteOrder/LITTLE_ENDIAN)))))])

   (testing "Malformed length prefixes"
     (let [malformed
           (fn [type-id n]
             (.array
               (doto (java.nio.ByteBuffer/allocate 5)
                 (.put    (unchecked-byte type-id))
                 (.putInt (int n)))))

           cases
           {"byte array"      sc/id-byte-array-lg
            "object array"    sc/id-object-array-lg
            "primitive array" sc/id-int-array-lg
            "string"          sc/id-str-lg}]

       (doseq [n [1024 -7], [label type-id] cases]
         (let [e (truss/throws :default (nippy/fast-thaw (malformed type-id n)))]
           (is (instance? java.io.EOFException (ex-cause e)) (str label ", length " n))))

       ;; Streams have no known remaining length, so only negative lengths
       ;; can be rejected before allocation
       (doseq [[label type-id] cases]
         (is (throws? java.io.EOFException
               (nippy/thaw-from-in!
                 (java.io.DataInputStream.
                   (java.io.ByteArrayInputStream. (malformed type-id -7)))))
           (str label ", stream, negative length")))))

   (testing "Malformed collection counts"
     ;; These build incrementally so there's no allocation to guard, but a
     ;; negative count must still error rather than yield an empty coll
     (let [malformed
           (fn [type-id n]
             (.array
               (doto (java.nio.ByteBuffer/allocate 5)
                 (.put    (unchecked-byte type-id))
                 (.putInt (int n)))))

           cases
           {"vector"     sc/id-vec-lg
            "set"        sc/id-set-lg
            "map"        sc/id-map-lg
            "list"       sc/id-list-lg
            "seq"        sc/id-seq-lg
            "queue"      sc/id-queue-lg
            "sorted set" sc/id-sorted-set-lg
            "sorted map" sc/id-sorted-map-lg}]

       (doseq [n [-1 -7 Integer/MIN_VALUE], [label type-id] cases]
         [(let [e (truss/throws :default (nippy/fast-thaw (malformed type-id n)))]
            (is (instance? java.io.EOFException (ex-cause e))
              (str label ", buffered, count " n)))

          (is (throws? java.io.EOFException
                (nippy/thaw-from-in!
                  (java.io.DataInputStream.
                    (java.io.ByteArrayInputStream. (malformed type-id n)))))
            (str label ", stream, count " n))])))

   (testing "Thread-local buffer retention"
     (let [max-capacity @(ns-resolve 'taoensso.nippy.io 'max-cached-bb-capacity)
           observed    (volatile! nil)]
       (nippy/fast-freeze (byte-array (inc max-capacity)))
       (io/with-bb 512
         (fn [^java.nio.ByteBuffer bb _]
           (vreset! observed (.capacity bb))
           false))
       (is (<= @observed max-capacity))))

   (testing "Streaming thaw"
     ;; We want to confirm that `thaw-from-in!` (which uses legacy `DataInput`)
     ;; thaws the same as `fast-thaw`. Comparing the thawed values can be tricky
     ;; since they incl. uncomparable elements, so we instead compare the
     ;; RE-freezes are identical
     (let [ba (nippy/fast-freeze (nippy/stress-data {}))]
       (is (ba=
             (nippy/freeze (nippy/fast-thaw ba))
             (nippy/freeze (nippy/thaw-from-in! (java.io.DataInputStream. (java.io.ByteArrayInputStream. ba)))))
         "`DataInput` reader agrees with `ByteBuffer` reader on all stress data")))])

(defn- freeze-to-ba
  "Streams x via `freeze-to-out!`, returns the bytes written."
  ^bytes [x]
  (let [baos (java.io.ByteArrayOutputStream.)]
    (nippy/with-cache (nippy/freeze-to-out! (java.io.DataOutputStream. baos) x))
    (.toByteArray baos)))

(defn- stream-write-count
  "Streams x via `freeze-to-out!`, returns the number of writes reaching the
  sink. >1 => x was written incrementally rather than buffered in full.

  NB runs on a FRESH thread: the streaming chunk is retained per thread and
  may have been grown by an earlier write, which would otherwise let a
  buffered value fit in one chunk and mask the very thing we're checking."
  ^long [x]
  (let [n  (java.util.concurrent.atomic.AtomicLong.)
        os (proxy [java.io.OutputStream] []
             (write
               ([_]     (.incrementAndGet n) nil)
               ([_ _ _] (.incrementAndGet n) nil)))
        t  (Thread.
             (fn [] (nippy/with-cache
                      (nippy/freeze-to-out! (java.io.DataOutputStream. os) x))))]
    (.start t)
    (.join  t)
    (.get   n)))

;; Implements `ISeq` but, being a deftype, natively dispatches to the `IType`
;; writer: Clojure protocols prefer class impls over interface impls. Guards
;; against the streaming writer hand-mirroring dispatch and diverging.
(defrecord StreamRec [a b])

(deftype SeqishType [xs]
  clojure.lang.Counted
  clojure.lang.ISeq
  (seq   [_  ] (seq    xs))
  (first [_  ] (first  xs))
  (next  [_  ] (next   xs))
  (more  [_  ] (rest   xs))
  (cons  [_ o] (cons o xs))
  (count [_  ] (count  xs))
  (empty [_  ] nil)
  (equiv [_ _] false))

;; The wiki documents calling `freeze-to-out!` from inside an `extend-freeze`
;; body, so the streaming writer MUST tolerate re-entry on the same thread.
;; It reuses one chunk per thread, so a nested call taking that same chunk
;; would silently clear it out from under the write in progress.
(defrecord NestedFreezeRec [data])
(nippy/extend-freeze NestedFreezeRec :nippy-tests/nested-freeze [x out]
  (nippy/freeze-to-out! out (:data x)))
(nippy/extend-thaw :nippy-tests/nested-freeze [in]
  (->NestedFreezeRec (nippy/thaw-from-in! in)))

(deftest _freeze-to-out-streaming
  ;; `freeze-to-out!` streams counted colls through a small chunk rather than
  ;; buffering the whole value. Its dispatch mirrors the buffered writer's, so
  ;; the two MUST agree byte-for-byte - that's what makes the mirroring safe.
  [(testing "Streamed output is byte-identical to buffered output"
     [(is (ba= (freeze-to-ba test-data) (nippy/fast-freeze test-data)) "Comparable stress data")
      (let [d (nippy/stress-data {})] (is (ba= (freeze-to-ba d)        (nippy/fast-freeze d)) "Full stress data"))
      (is (gen-test 40 [gen-data]         (ba= (freeze-to-ba gen-data) (nippy/fast-freeze gen-data))) "Generated data")])

   (testing "Values spanning many chunks"
     (let [x (vec (range 200000))] ; Serialized size >> chunk size
       [(is (ba= (freeze-to-ba x) (nippy/fast-freeze x)))
        (is (= x (nippy/thaw-from-in!
                   (java.io.DataInputStream.
                     (java.io.ByteArrayInputStream. (freeze-to-ba x))))))]))

   (testing "Types whose dispatch differs from their interfaces"
     ;; `MapEntry` extends `APersistentVector`; `()` is an `EmptyList` that
     ;; does NOT extend `PersistentList`; `SeqishType` is an `ISeq` that
     ;; dispatches to the `IType` writer
     (doseq [[nm x]
             {"MapEntry"   (first {:a 1})
              "empty list" ()
              "list"       '(1 2 3)
              "queue"      (into clojure.lang.PersistentQueue/EMPTY [1 2 3])
              "sorted-map" (sorted-map :a 1 :b 2)
              "sorted-set" (sorted-set 3 1 2)
              "lazy-seq"   (map inc (range 100))
              "range"      (range 100)
              "cons"       (cons 1 (map inc (range 10)))
              "ISeq type"  (->SeqishType [1 2 3])
              "record"     (->StreamRec 1 [2 3])
              "sql-time"   (java.sql.Time. 1700000000123) ; `extend-freeze`d above
              "nested"     {:q (into clojure.lang.PersistentQueue/EMPTY [(first {:a 1}) ()])}}]

       (is (ba= (freeze-to-ba x) (nippy/fast-freeze x)) nm)))

   (testing "Cache refs stay consistent across chunk flushes"
     ;; Cache idxs are assigned as bytes are emitted, so a flush between
     ;; cached values must not disturb them
     (let [x [(nippy/cache "abc") (vec (range 50000)) (nippy/cache "abc") (nippy/cache "abc")]]
       (is (ba= (freeze-to-ba x) (nippy/fast-freeze x))))

     ;; `write-cached` registers its cache idx BEFORE writing the value, so a
     ;; buffered leaf that registers cache idxs and THEN overflows must roll
     ;; them back. Without that, the retry sees the idxs already present and
     ;; emits bare cache REFs, having never written the values at all.
     ;;
     ;; A lazy seq is uncounted, so it's written as one buffered leaf - and the
     ;; filler is essential, leaving too little chunk space for that leaf, which
     ;; is what forces the overflow.
     (let [big    (apply str (repeat 40000 "x"))
           filler (apply str (repeat 50000 "y"))
           x      [filler (map identity [(nippy/cache big) (nippy/cache big)])]
           ba     (freeze-to-ba x)] ; NB freeze outside the thaw's cache session

       [(is (ba= ba (nippy/fast-freeze x)) "Cache rollback on overflow")
        ;; NB reading cache refs needs a `with-cache` session, mirroring the
        ;; write side. Thaw yields the cached values themselves.
        (is (= [filler [big big]]
              (nippy/with-cache
                (nippy/thaw-from-in!
                  (java.io.DataInputStream.
                    (java.io.ByteArrayInputStream. ba))))))]))

   (testing "Cache id encoding boundaries"
     ;; `write-cached-header!` switches encoding at these idxs:
     ;; 0-7 (dedicated ids) | 8-127 (sm) | 128-32767 (md) | >32767 (uncached)
     [(doseq [n [8 9 128 129 32768 32769]]
        (let [x (mapv #(nippy/cache %) (range n))]
          (is (ba= (freeze-to-ba x) (nippy/fast-freeze x)) (str "n=" n))))

      (let [x (nippy/cache [(nippy/cache "a") (nippy/cache "b") (nippy/cache [1 2 3])])
            y [x x]]
        (is (ba= (freeze-to-ba y) (nippy/fast-freeze y)) "Nested cached values"))])

   (testing "Streamable values stream rather than buffer"
     ;; A value spanning many chunks must be written incrementally, not
     ;; accumulated into one buffer. NB this is what makes their total size
     ;; unbounded, so a type that silently stops streaming (e.g. a writer
     ;; registered below `stream-kinds`) quietly restores the ~2 GiB limit.
     (let [n 50000]
       (doseq [[nm x]
               {"vector"     (vec (range n))
                "set"        (into #{} (range n))
                "map"        (into {} (map (fn [i] [i i])) (range n))
                "list"       (apply list (range n))
                "seq"        (seq (vec (range n)))
                "sorted-map" (into (sorted-map) (map (fn [i] [i i])) (range n))
                "sorted-set" (into (sorted-set) (range n))
                "queue"      (into clojure.lang.PersistentQueue/EMPTY (range n))
                "map-entry"  (clojure.lang.MapEntry/create :k (vec (range n)))
                "cached"     (let [c (nippy/cache (vec (range n)))] [c c])}]

         (is (> (stream-write-count x) 1) (str nm " written across multiple chunks")))))

   (testing "Buffered fallback paths"
     [(let [x [(java.util.ArrayList. [1 2 3]) (java.util.ArrayList. (range 100000))]]
        (is (ba= (freeze-to-ba x) (nippy/fast-freeze x)) "Java Serializable"))
      (binding [nippy/*freeze-fallback* :write-unfreezable]
        (let [x [1 (fn []) {:k (fn [])}]]
          (is (ba= (freeze-to-ba x) (nippy/fast-freeze x)) "*freeze-fallback*")))])

   (testing "Metadata"
     (let [x (with-meta [(with-meta {:a 1} {:inner true}) 2]
               {:outer true, :nested (with-meta #{1} {:deep true})})]
       [(is (ba= (freeze-to-ba x) (nippy/fast-freeze x)) "Nested metadata")
        (binding [nippy/*incl-metadata?* false]
          (is (ba= (freeze-to-ba x) (nippy/fast-freeze x)) "Metadata disabled"))]))

   (testing "Single values larger than a whole chunk"
     ;; Exercises the grow path: one leaf that can't fit even an empty chunk
     (let [big (apply str (repeat 200000 "x"))
           x   [1 big {:k big} (byte-array 150000) (repeat 3 big)]]
       (is (ba= (freeze-to-ba x) (nippy/fast-freeze x)))))

   (testing "Re-entrant `freeze-to-out!`"
     ;; The chunk is reused per thread, so a nested call must take its own.
     ;; The inner value spans many chunks, forcing flushes at both depths.
     (let [x [:head
              (->NestedFreezeRec {:rows (vec (range 50000))})
              (->NestedFreezeRec (->NestedFreezeRec [1 2 3]))
              :tail]]
       [(is (ba= (freeze-to-ba x) (nippy/fast-freeze x)) "Streamed == buffered")
        (is (= x (nippy/thaw-from-in!
                   (java.io.DataInputStream.
                     (java.io.ByteArrayInputStream. (freeze-to-ba x)))))
          "Round-trips")]))

   (testing "Concurrent `freeze-to-out!`"
     ;; Guards the per-thread chunk against cross-thread sharing
     (let [x   (vec (range 20000))
           ref (nippy/fast-freeze x)]
       (is (every? true?
             (mapv deref
               (mapv (fn [_] (future (every? true? (repeatedly 50 #(ba= (freeze-to-ba x) ref)))))
                 (range 8))))
         "Each thread gets its own chunk")))])

;;;; Custom types & records

(deftype   MyType [basic_field fancy-field!]) ; Note `fancy-field!` field name will be munged
(defrecord MyRec  [basic_field fancy-field!])
(defrecord LowLevelRec [id])

(deftest _types
  [(testing "Extend to custom type"
     [(is
        (throws? Exception ; No thaw extension yet
          (do
            (alter-var-root #'nippy/*custom-readers* (constantly {}))
            (nippy/extend-freeze MyType 1 [x s]
              (.writeUTF s (.basic_field  x))
              (.writeUTF s (.fancy-field! x)))

            (thaw (freeze (MyType. "basic" "fancy"))))))

      (is
        (do
          (nippy/extend-thaw 1 [s] (MyType. (.readUTF s) (.readUTF s)))
          (let [mt1 (MyType. "basic" "fancy")
                ^MyType mt2 (thaw (freeze mt1))]
            (=
              [(.basic_field mt1) (.fancy-field! mt1)]
              [(.basic_field mt2) (.fancy-field! mt2)]))))])

   (testing "Extend to custom Record"
     (is
       (do
         (nippy/extend-freeze MyRec 2 [x s]
           (.writeUTF s (str "foo-" (:basic_field  x)))
           (.writeUTF s (str "foo-" (:fancy-field! x))))

         (nippy/extend-thaw 2 [s] (MyRec. (.readUTF s) (.readUTF s)))
         (=
           (do           (MyRec. "foo-basic" "foo-fancy"))
           (thaw (freeze (MyRec.     "basic"     "fancy")))))))

   (testing "Keyword (prefixed) extensions"
     (is
       (do
         (nippy/extend-freeze MyRec :nippy-tests/MyRec [x s]
           (.writeUTF s (:basic_field  x))
           (.writeUTF s (:fancy-field! x)))

         (nippy/extend-thaw :nippy-tests/MyRec [s] (MyRec. (.readUTF s) (.readUTF s)))
         (let [mr (MyRec. "basic" "fancy")]
           (=  mr (thaw (freeze mr)))))))

   (testing "Low-level nested Nippy calls in custom extensions"
     (is
       (do
         (nippy/extend-freeze LowLevelRec :nippy-tests/low-level-rec [x out]
           (nippy/freeze-to-out! out (:id x)))

         (nippy/extend-thaw :nippy-tests/low-level-rec [in]
           (->LowLevelRec (nippy/thaw-from-in! in)))

         (let [x  (->LowLevelRec 42)
               bb (java.nio.ByteBuffer/allocate 256)]
           (nippy/freeze-to-bb! bb x)
           (.flip bb)
           (and
             (= x            (thaw (freeze           x)))
             (= {:wrapped x} (thaw (freeze {:wrapped x})))
             (= x            (nippy/thaw-from-bb! bb)))))))])

;;;; Caching

(deftest _caching
  (let [test-data* [test-data test-data test-data test-data] ; Data with duplicates
        cached (mapv nippy/cache test-data*)
        cached (mapv nippy/cache test-data*) ; <=1 wrap auto-enforced
        ]

    [(is (= test-data*  (thaw (freeze test-data* {:compressor nil}))))
     (is (= test-data*  (thaw (freeze cached     {:compressor nil}))))
     (let [size-stress (count (freeze test-data* {:compressor nil}))
           size-cached (count (freeze cached     {:compressor nil}))]
       (is (>= size-stress (* 3 size-cached)))
       (is (<  size-stress (* 4 size-cached))))]))

(deftest _caching-session-recovery
  ;; Cache entries from a failed write mustn't poison later writes in a
  ;; shared `with-cache` session
  (let [baos (java.io.ByteArrayOutputStream.)
        dout (java.io.DataOutputStream. baos)]

    (nippy/with-cache
      ;; `(Object.)` is reliably unfreezable (not Serializable or readable)
      (is (throws? (nippy/freeze-to-out! dout [(nippy/cache "shared") (Object.)])))
      (nippy/freeze-to-out! dout [(nippy/cache "shared")]))

    (let [din (java.io.DataInputStream.
                (java.io.ByteArrayInputStream. (.toByteArray baos)))]
      (is (= ["shared"] (nippy/with-cache (nippy/thaw-from-in! din)))))))

(deftest _caching-session-sink-failure
  ;; Cache must also be restored when the SINK (vs serialization) fails
  (let [baos (java.io.ByteArrayOutputStream.)
        dout (java.io.DataOutputStream. baos)
        bad-dout ; Throws before accepting any bytes
        (proxy [java.io.DataOutputStream] [baos]
          (write [_ba _off _len] (throw (java.io.IOException. "sink failed"))))]

    (nippy/with-cache
      (is (throws? (nippy/freeze-to-out! bad-dout [:kw1 (nippy/cache "v")])))
      (nippy/freeze-to-out! dout [:kw1 (nippy/cache "v")]))

    (let [din (java.io.DataInputStream.
                (java.io.ByteArrayInputStream. (.toByteArray baos)))]
      (is (= [:kw1 "v"] (nippy/with-cache (nippy/thaw-from-in! din)))))))

(defrecord NestedThawWrapper [payload-ba])

(nippy/extend-freeze NestedThawWrapper :test/nested-thaw-wrapper [x dout]
  (let [^bytes ba (:payload-ba x)]
    (.writeInt dout (alength ba))
    (.write    dout ba 0 (alength ba))))

(nippy/extend-thaw :test/nested-thaw-wrapper [din]
  (let [len (.readInt din)
        ba  (byte-array len)]
    (.readFully din ba 0 len)
    (thaw ba))) ; Nested `thaw` => nested `with-cache`

(deftest _caching-nested
  ;; An inner `with-cache` must RESTORE (not remove) the outer cache, else the
  ;; outer thaw throws "Can't thaw without cache available" on its next ref
  (let [frozen
        (freeze
          [(nippy/cache "shared")
           (NestedThawWrapper. (freeze {:inner "payload"}))
           (nippy/cache "shared")])

        thawed (thaw frozen)]

    [(is (= (nth thawed 0) "shared"))
     (is (= (nth thawed 2) "shared")     "Outer cache survives a nested thaw")
     (is (= (nth thawed 1) {:inner "payload"}) "Nested thaw returns its own data")]))

(deftest _caching-metadata
  (let [v1 (with-meta [] {:id :v1})
        v2 (with-meta [] {:id :v2})

        frozen-without-caching (freeze [v1 v2 v1 v2])
        frozen-with-caching
        (freeze [(nippy/cache v1)
                 (nippy/cache v2)
                 (nippy/cache v1)
                 (nippy/cache v2)])]

    [(is (> (count frozen-without-caching)
            (count frozen-with-caching)))

     (is (= (thaw frozen-without-caching)
            (thaw frozen-with-caching)))

     (is (= (mapv meta (thaw frozen-with-caching))
            [{:id :v1} {:id :v2} {:id :v1} {:id :v2}]))]))

;;;; Serialized output

(defn ba-hash [^bytes ba] (hash (seq ba)))

(defn gen-hashes [] (enc/map-vals (fn [v] (ba-hash (freeze v))) test-data))
(defn cmp-hashes [new old] (vec (sort (reduce-kv (fn [s k v] (if (= (get old k) v) s (conj s k))) #{} new))))

(def ref-hashes {:deftype (if (impl/target-release>= 370) -917125089 -671450876), :lazy-seq-empty -574080456, :true -1809580601, :long 598276629, :double -454270428, :lazy-seq -856460618, :short 1152993378, :meta -858252893, :str-long -1970041891, :instant -1401948864, :many-keywords 665654816, :bigint 2033662230, :sym-ns 769802402, :queue 447747779, :float 603100813, :sorted-set 2005004017, :many-strings 1738215727, :nested -1350538572, :queue-empty 1760934486, :duration -775528642, :false 1506926383, :vector 813550992, :util-date 1326218051, :kw 389651898, :sym -1742024487, :str-short -921330463,  :subvec 709331681,   :kw-long 852232872, :integer 624865727, :sym-long -1535730190, :list -1207486853, :ratio 1186850097, :byte -1041979678, :bigdec -1846988137, :nil 2005042235, :defrecord 842721251, :sorted-map -1160380145, :sql-date 80018667, :map-entry 1219306839,  :false-boxed 1506926383, :uri 870148616,   :period -2043530540, :many-longs -1109794519, :uuid -338331115, :set 1649942133,  :kw-ns 1050084331, :map 1989337680, :many-doubles -827569787, :char 858269588})

(comment (cmp-hashes (gen-hashes) ref-hashes)) ; []

(deftest   _stable-serialized-output
  (testing "Stable serialized output"

    (testing "x=y => f(x)=f(y) for SOME inputs, SOMETIMES"
      ;; `x=y => f(x)=f(y)` is unfortunately NOT true in general, and NOT something we
      ;; promise. Still, we do unofficially try our best to maintain this property when
      ;; possible - and to warn when it'll be violated for common/elementary types.
      [(is (not (ba= (freeze {:a 1 :b 1}) (freeze {:b 1 :a 1}))) "Small (array) map (not= (seq {:a 1 :b 1}) (seq {:b 1 :a 1}))")
       (is (not (ba= (freeze [[]])        (freeze ['()])))       "(= [] '()) is true")
       (is      (ba= (freeze (sorted-map :a 1 :b 1))
                     (freeze (sorted-map :b 1 :a 1))) "Sorted structures are generally safe")

       ;; Track serialized output of stress data so that we can detect unintentional changes,
       ;; and warn about intended ones. Hashes will need to be recalculated on changes to stress data.
       (let [reference-hashes ref-hashes
             failures ; #{{:keys [k v]}}
             (reduce-kv
               (fn [failures k v]
                 (or
                   (when (not= v :taoensso.nippy/skip)
                     (let [frozen (freeze v)
                           actual (ba-hash frozen)
                           ref    (get reference-hashes k)]
                       (when (not= actual ref)
                         (conj failures
                           {:k k,
                            :v {:type (type v), :value v}
                            :actual actual
                            :ref    ref
                            :frozen (vec frozen)}))))
                   failures))
               #{}
               test-data)]

         (is (empty? failures)))])

    (testing "x==y => f(x)=f(y)"
      ;; This weaker version of `x=y => f(x)=f(y)` does hold
      [(is (ba= (freeze test-data)
                (freeze test-data)))

       (is (every? true? (repeatedly 1000 (fn [] (ba= (freeze test-data)
                                                      (freeze test-data)))))
         "Try repeatedly to catch possible protocol interface races")

       (is (gen-test 400 [gen-data]
              (ba= (freeze gen-data)
                   (freeze gen-data))) "Generative")])

    (testing "f(x)=f(f-1(f(x)))"
      [(let [test-data (dissoc test-data :lazy-seq)] ; `:lazy-seq` freezes as uncounted-coll, but thaws as counted-coll
         (is (ba=
               (-> test-data freeze)
               (-> test-data freeze thaw freeze))))

       (let [test-data (-> test-data freeze thaw)] ; After `:lazy-seq` is counted-coll
         (is (ba=
               (freeze test-data)
               (reduce (fn [frozen _] (freeze (thaw frozen))) (freeze test-data) (range 1000)))
           "Try repeatedly to catch possible protocol interface races"))

       (is (gen-test 400 [gen-data]
             (ba= (-> gen-data freeze)
                  (-> gen-data freeze thaw freeze))) "Generative")])

    (testing "f(x)=f(y) => x=y"
      (let [vals_ (atom {})]
        (gen-test 400 [gen-data]
          (let [out (freeze     gen-data)
                ref (get @vals_ gen-data ::nx)]
            (swap! vals_ assoc out gen-data)
            (or (= ref ::nx) (= ref out))))))))

;;;; Thread safety

(deftest _thread-safe
  [(is
     (let [futures (mapv (fn [_] (future (= (thaw (freeze test-data)) test-data)))
                     (range 50))]
       (every? deref futures)))

   (is
     (let [futures
           (mapv
             (fn [_]
               (future
                 (= (thaw (freeze test-data {:password [:salted "password"]})
                                            {:password [:salted "password"]})
                   test-data)))
             (range 50))]
       (every? deref futures)))

   (is
     (let [futures
           (mapv
             (fn [_]
               (future
                 (= (thaw (freeze test-data {:password [:cached "password"]})
                                            {:password [:cached "password"]})
                   test-data)))
             (range 50))]
       (every? deref futures)))])

;;;; Redefs

(defrecord MyFoo [] Object (toString [_] "v1"))
(defrecord MyFoo [] Object (toString [_] "v2"))

(deftest _redefs
  (is (= (str (thaw (freeze (MyFoo.)))) "v2")))

;;;; Serializable

(do
  (def ^:private semcn              "java.util.concurrent.Semaphore")
  (def ^:private sem                (java.util.concurrent.Semaphore. 1))
  (defn-         sem? [x] (instance? java.util.concurrent.Semaphore x)))

(deftest _serializable
  [(is (= nippy/*thaw-serializable-allowlist* #{"base.1" "base.2" "add.1" "add.2"})
     "JVM properties override initial allowlist values")

   (is (throws? Exception (nippy/freeze sem {:serializable-allowlist #{}}))
     "Can't freeze Serializable objects unless approved by allowlist")

   (is (sem?
         (nippy/thaw
           (nippy/freeze sem {:serializable-allowlist #{semcn}})
           {:serializable-allowlist #{semcn}}))

     "Can freeze and thaw Serializable objects if approved by allowlist")

   (is (sem?
         (nippy/thaw
           (nippy/freeze sem {:serializable-allowlist #{"java.util.concurrent.*"}})
           {:serializable-allowlist #{"java.util.concurrent.*"}}))

     "Strings in allowlist sets may contain \"*\" wildcards")

   (let [ba     (nippy/freeze sem #_{:serializable-allowlist "*"})
         thawed (nippy/thaw   ba    {:serializable-allowlist #{}})]

     [(is (= :quarantined (get-in thawed [:nippy/unthawable :cause]))
        "Serializable objects will be quarantined when approved for freezing but not thawing.")

      (is (sem? (nippy/read-quarantined-serializable-object-unsafe! thawed))
        "Quarantined Serializable objects can still be manually force-read.")

      (is (sem? (nippy/read-quarantined-serializable-object-unsafe!
                  (nippy/thaw (nippy/freeze thawed))))
        "Quarantined Serializable objects are themselves safely transportable.")])

   (let [obj
         (nippy/thaw
           (nippy/freeze sem)
           {:serializable-allowlist "allow-and-record"})]

     [(is (sem? obj)
        "Special \"allow-and-record\" allowlist permits any class")

      (is
        (contains? (nippy/get-recorded-serializable-classes) semcn)
        "Special \"allow-and-record\" allowlist records classes")])

   (testing "Legacy unlength-prefixed payloads"
     (let [ba (read-resource-bytes "data/legacy-serializable-v2.14.0.npy")
           expected {:class java.util.ArrayList
                     :items [1 2 3]
                     :tail  :legacy-sentinel}
           check-result
           (fn [x]
             (is (= expected
                   {:class (class (first x))
                    :items (vec   (first x))
                    :tail  (second x)})))
           check-bb
           (fn [^java.nio.ByteBuffer bb]
             (let [expected-position (.limit bb)]
               (check-result (nippy/thaw-from-bb! bb))
               (is (= expected-position (.position bb)))))]

       (binding [nippy/*thaw-serializable-allowlist* #{"java.util.ArrayList"}]
         (check-result (nippy/fast-thaw ba))
         (check-result
           (thaw
             (sc/wrap-header ba {:compressor-id nil :encryptor-id nil})
             {:serializable-allowlist #{"java.util.ArrayList"}}))

         (let [bais (java.io.ByteArrayInputStream. ba)]
           (check-result (nippy/thaw-from-in! (java.io.DataInputStream. bais)))
           (is (zero? (.available bais))))

         (check-bb (java.nio.ByteBuffer/wrap ba))
         (check-bb (.asReadOnlyBuffer (java.nio.ByteBuffer/wrap ba)))

         (let [bb
               (doto (java.nio.ByteBuffer/allocate (+ (alength ba) 8))
                 (.position 4)
                 (.put ba)
                 (.flip)
                 (.position 4))]
           (check-bb (.slice bb)))

         (let [bb
               (doto (java.nio.ByteBuffer/allocateDirect (alength ba))
                 (.put ba)
                 (.flip))]
           (check-bb bb)))))

   (testing "Serialization that fails part-way"
     ;; `writeObject` throws only once it reaches the (non-Serializable) referent,
     ;; i.e. after any class name header would already have been written
     (let [x (java.util.concurrent.atomic.AtomicReference. (Object.))]
       (binding [nippy/*freeze-fallback* :write-unfreezable]
         (let [[unfreezable tail] (nippy/fast-thaw (nippy/fast-freeze [x :tail]))]
           [(is (contains? unfreezable :nippy/unfreezable) "Falls through to next fallback")
            (is (= :tail tail)                             "No orphan header bytes left behind")]))))])

;;;; Metadata

(def my-var "Just a string")

(deftest _metadata
  [(is
     (:has-meta?
      (meta
        (nippy/thaw
          (nippy/freeze (with-meta [] {:has-meta? true}) {:incl-metadata? true})
          {:incl-metadata? true}
          )))

     "Metadata successfully included")

   (is
     (nil?
       (meta
         (nippy/thaw
           (nippy/freeze (with-meta [] {:has-meta? true}) {:incl-metadata? true})
           {:incl-metadata? false}
           )))

     "Metadata successfully excluded by thaw")

   (is
     (nil?
       (meta
         (nippy/thaw
           (nippy/freeze (with-meta [] {:has-meta? true}) {:incl-metadata? false})
           {:incl-metadata? true}
           )))

     "Metadata successfully excluded by freeze")

   (is (var? (nippy/read-quarantined-serializable-object-unsafe!
               (nippy/thaw (nippy/freeze #'my-var))))

     "Don't try to preserve metadata on vars")])

;;;; Freezable?

(deftest _freezable?
  [(is (= (nippy/freezable? :foo)                      :native))
   (is (= (nippy/freezable? [:a :b])                   :native))
   (is (= (nippy/freezable? [:a (fn [])])                  nil))
   (is (= (nippy/freezable? [:a (byte-array [1 2 3])]) :native))
   (is (= (nippy/freezable? [:a (java.util.Date.)])    :native))
   (is (= (nippy/freezable? (Exception.))                  nil))
   (is (= (nippy/freezable? (MyType. "a" "b"))         :native))
   (is (= (nippy/freezable? (MyRec.  "a" "b"))         :native))
   (is (= (nippy/freezable? (Exception.) {:allow-java-serializable? true})
         :maybe-java-serializable))])

;;;; thaw-xform

(deftest _thaw-xform
  [(is (= (binding [nippy/*thaw-xform* nil]                                           (thaw (freeze [1 2 :secret 3 4]))) [1 2 :secret   3 4]))
   (is (= (binding [nippy/*thaw-xform* (map (fn [x] (if (= x :secret) :redacted x)))] (thaw (freeze [1 2 :secret 3 4]))) [1 2 :redacted 3 4]))

   (is (= (binding [nippy/*thaw-xform* (remove (fn [x] (and (map-entry? x) (and (= (key x) :x) (val x)))))]
            (thaw (freeze {:a :A, :b :B, :x :X, :c {:x :X}, :d #{:d1 :d2 {:d3 :D3, :x :X}}})))
         {:a :A, :b :B, :c {}, :d #{:d1 :d2 {:d3 :D3}}}))

   (is (= (binding [nippy/*thaw-xform* (remove (fn [x] (and (map? x) (contains? x :x))))]
            (thaw (freeze {:a :A, :b :B, :x :X, :c {:x :X}, :d #{:d1 :d2 {:d3 :D3, :x :X}}})))
         {:a :A, :b :B, :x :X, :c {:x :X}, :d #{:d1 :d2}}))

   (is (= (binding [nippy/*thaw-xform* (map (fn [x] (/ 1 0)))] (thaw (freeze []))) []) "rf not run on empty colls")

   (let [ex (truss/throws :default (binding [nippy/*thaw-xform* (map (fn [x] (/ 1 0)))] (thaw (freeze [:a :b]))))]
     (is (= (-> ex ex-cause ex-cause ex-data :call) '(rf acc in)) "Error thrown via `*thaw-xform*`"))])

;;;; Compressors

(deftest ^:no-auto _compressors
  (println "\nTesting decompression of random data...")
  (doseq [c [compr/zstd-compressor
             compr/lz4-compressor
             compr/lzo-compressor
             compr/snappy-compressor
             compr/lzma2-compressor]]

    (print (str "  With " (name (compr/header-id c)))) (flush)
    (dotimes [_ 5] ; Slow, a few k laps should be sufficient for CI
      (print ".") (flush)
      (dotimes [_ 1000]
        (is
          (nil? (truss/catching :all (compr/decompress c (crypto/rand-bytes 1024))))
          "Decompression never crashes JVM, even against invalid data")))
    (println)))

;;;; Benchmarks

(deftest _benchmarks
  (is (benchmarks/bench-serialization {:all? true})))

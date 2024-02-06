(ns taoensso.nippy-tests
  (:require
   [clojure.test :as test :refer [deftest testing is]]
   [clojure.test.check            :as tc]
   [clojure.test.check.generators :as tc-gens]
   [clojure.test.check.properties :as tc-props]
   [taoensso.encore               :as enc   :refer [ba=]]
   [taoensso.nippy                :as nippy :refer [freeze thaw]]
   [taoensso.nippy.compression    :as compr]
   [taoensso.nippy.crypto         :as crypto]
   [taoensso.nippy-benchmarks     :as benchmarks]))

(comment
  (remove-ns      'taoensso.nippy-tests)
  (test/run-tests 'taoensso.nippy-tests))

;;;; Config, etc.

(def test-data (nippy/stress-data {:comparable? true}))
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

;;;; Core

(deftest _core
  (println (str "Clojure version: " *clojure-version*))
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

   (let [d (nippy/stress-data {})]
     [(is (= (vec (:bytes   d)) ((comp vec thaw freeze) (:bytes   d))))
      (is (= (vec (:objects d)) ((comp vec thaw freeze) (:objects d))))])

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

   (is (enc/throws? Exception (thaw (freeze test-data {:password "malformed"}))))
   (is (enc/throws? Exception (thaw (freeze test-data {:password [:salted "p"]}))))
   (is (enc/throws? Exception (thaw (freeze test-data {:password [:salted "p"]}))))

   (is
     (= "payload"
       (thaw (freeze "payload" {:password [:salted "pwd"] :encryptor nippy/aes128-cbc-encryptor})
         (do                   {:password [:salted "pwd"]})))
     "CBC auto-encryptor compatibility")

   (testing "Unsigned long types"
     (let [range-ushort+ (+ (long @#'nippy/range-ushort) 128)
           range-uint+   (+ (long @#'nippy/range-uint)   128)]

       [(let [r (range (long -2.5e6) (long 2.5e6))]      (= (thaw (freeze r)) r))
        (let [r (range (- range-ushort+) range-ushort+)] (= (thaw (freeze r)) r))
        (let [n    range-uint+]                          (= (thaw (freeze n)) n))
        (let [n (- range-uint+)]                         (= (thaw (freeze n)) n))]))

   (is (gen-test 1600 [gen-data] (= gen-data (thaw (freeze gen-data)))) "Generative")])

;;;; Custom types & records

(deftype   MyType [basic_field fancy-field!]) ; Note `fancy-field!` field name will be munged
(defrecord MyRec  [basic_field fancy-field!])

(deftest _types
  [(testing "Extend to custom type"
     [(is
        (enc/throws? Exception ; No thaw extension yet
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
           (=  mr (thaw (freeze mr)))))))])

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

(deftest _stable-serialized-output
  (testing "Stable serialized output"

    (testing "x=y => f(x)=f(y) for SOME inputs, SOMETIMES"
      ;; `x=y => f(x)=f(y)` is unfortunately NOT true in general, and NOT something we
      ;; promise. Still, we do unofficially try our best to maintain this property when
      ;; possible - and to warn when it'll be violated for common/elementary types.
      [(is (not (ba= (freeze {:a 1 :b 1}) (freeze {:b 1 :a 1}))) "Small (array) map (not= (seq {:a 1 :b 1}) (seq {:b 1 :a 1}))")
       (is (not (ba= (freeze [[]])        (freeze ['()])))       "(= [] '()) is true")
       (is      (ba= (freeze (sorted-map :a 1 :b 1))
                     (freeze (sorted-map :b 1 :a 1))) "Sorted structures are generally safe")

       ;; Track serialized output of stress data so that we can at least be aware of
       ;; (and warn about) unintended changes for common/elementary types, etc. Note that
       ;; reference hashes will need to be recalculated on changes to stress data.
       (let [reference-hashes ; (enc/map-vals (fn [v] (ba-hash (freeze v))) test-data)
             {:deftype 1529147805, :lazy-seq-empty 1277437598, :true -1809580601, :long 219451189, :double -454270428, :lazy-seq -1039619789, :short 1152993378, :meta 352218350, :str-long -1970041891, :instant -1401948864, :many-keywords 665654816, :bigint 2033662230, :sym-ns 769802402, :queue 447747779, :float 603100813, :sorted-set 1443292905, :many-strings 1777678883, :nested -1590473924, :queue-empty 1760934486, :duration -775528642, :false 1506926383, :vector 89425525, :util-date 1326218051, :kw 389651898, :sym -1742024487, :str-short -1097575232, :subvec -2047667173, :kw-long 852232872, :integer 624865727, :sym-long -1535730190, :list -1113199651, :ratio 1186850097, :byte -1041979678, :bigdec -1846988137, :nil 2005042235, :defrecord 287634761, :sorted-map 1464032648, :sql-date 80018667, :map-entry -1353323498, :false-boxed 1506926383, :uri -1374752165, :period -2043530540, :many-longs 759118414, :uuid -338331115, :set -1515144175, :kw-ns 1050084331, :map 358912619, :many-doubles -827569787, :char 858269588}

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
      [(is (ba= (-> test-data freeze)
                (-> test-data freeze thaw freeze)))

       (is (ba= (freeze test-data)
                (reduce (fn [frozen _] (freeze (thaw frozen))) (freeze test-data) (range 1000)))
         "Try repeatedly to catch possible protocol interface races")

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

   (is (enc/throws? Exception (nippy/freeze sem {:serializable-allowlist #{}}))
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
        "Special \"allow-and-record\" allowlist records classes")])])

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

   (let [ex (enc/throws :default (binding [nippy/*thaw-xform* (map (fn [x] (/ 1 0)))] (thaw (freeze [:a :b]))))]
     (is (= (-> ex enc/ex-cause enc/ex-cause ex-data :call) '(rf acc in)) "Error thrown via `*thaw-xform*`"))])

;;;; Compressors

(deftest _compressors
  (doseq [c [compr/zstd-compressor
             compr/lz4-compressor
             compr/lzo-compressor
             compr/snappy-compressor
             compr/lzma2-compressor]]

    (dotimes [_ 2e4]
      (is
        (nil? (enc/catching (compr/decompress c (crypto/rand-bytes 1024))))
        "Decompression never core dumps, even against invalid data"))))

;;;; Benchmarks

(deftest _benchmarks
  (is (benchmarks/bench-serialization {:all? true})))

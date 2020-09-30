(ns taoensso.nippy.tests.main
  (:require
   [clojure.test :as test :refer (is are deftest run-tests)]
   [clojure.test.check            :as tc]
   [clojure.test.check.generators :as tc-gens]
   [clojure.test.check.properties :as tc-props]
   [taoensso.encore :as enc   :refer ()]
   [taoensso.nippy  :as nippy :refer (freeze thaw)]
   ;;java-time support for nippy
   [taoensso.nippy.java-time]
   [taoensso.nippy.benchmarks :as benchmarks]))

(comment (test/run-tests))

(def test-data nippy/stress-data-comparable)
(def tc-num-tests 120)
(def tc-gens
  "Like `tc-gens/any` but removes NaN (which breaks equality tests)"
  (tc-gens/recursive-gen tc-gens/container-type #_simple-type
    (tc-gens/one-of
      [tc-gens/int tc-gens/large-integer #_tc-gens/double
       (tc-gens/double* {:NaN? false})
       tc-gens/char tc-gens/string tc-gens/ratio tc-gens/boolean tc-gens/keyword
       tc-gens/keyword-ns tc-gens/symbol tc-gens/symbol-ns tc-gens/uuid])))

(comment (tc-gens/sample tc-gens 10))

;;;; Core

(deftest _core
  (is (do (println (str "Clojure version: " *clojure-version*)) true))
  (is (= test-data ((comp thaw freeze) test-data)))
  (is (= test-data ((comp #(thaw   % {:no-header? true
                                      :compressor nippy/lz4-compressor
                                      :encryptor  nil})
                          #(freeze % {:no-header? true}))
                    test-data)))

  (is (= test-data ((comp #(thaw   % {:password [:salted "p"]})
                          #(freeze % {:password [:salted "p"]}))
                    test-data)))

  (is (= (vec (:objects nippy/stress-data))
         ((comp vec thaw freeze) (:objects nippy/stress-data))))

  (is (= test-data ((comp #(thaw   % {:compressor nippy/lzma2-compressor})
                          #(freeze % {:compressor nippy/lzma2-compressor}))
                    test-data)))

  (is (= test-data ((comp #(thaw   % {:compressor nippy/lzma2-compressor
                                      :password [:salted "p"]})
                          #(freeze % {:compressor nippy/lzma2-compressor
                                      :password [:salted "p"]}))
                    test-data)))

  (is (= test-data ((comp #(thaw   % {:compressor nippy/lz4-compressor})
                          #(freeze % {:compressor nippy/lz4hc-compressor}))
                    test-data)))

  (is ; Try roundtrip anything that simple-check can dream up
    (:result (tc/quick-check tc-num-tests
               (tc-props/for-all [val tc-gens]
                 (= val (thaw (freeze val)))))))

  (is (thrown? Exception (thaw (freeze test-data {:password "malformed"}))))
  (is (thrown? Exception (thaw (freeze test-data {:password [:salted "p"]})
                           {;; Necessary to prevent against JVM segfault due to
                            ;; https://goo.gl/t0OUIo:
                            :v1-compatibility? false})))
  (is (thrown? Exception (thaw (freeze test-data {:password [:salted "p"]})
                           {:v1-compatibility? false ; Ref. https://goo.gl/t0OUIo
                            :compressor nil})))

  (is ; Snappy lib compatibility (for legacy versions of Nippy)
    (let [^bytes raw-ba    (freeze test-data {:compressor nil})
          ^bytes xerial-ba (org.xerial.snappy.Snappy/compress raw-ba)
          ^bytes iq80-ba   (org.iq80.snappy.Snappy/compress   raw-ba)]
      (= (thaw raw-ba)
        (thaw (org.xerial.snappy.Snappy/uncompress xerial-ba))
        (thaw (org.xerial.snappy.Snappy/uncompress iq80-ba))
        (thaw (org.iq80.snappy.Snappy/uncompress   iq80-ba    0 (alength iq80-ba)))
        (thaw (org.iq80.snappy.Snappy/uncompress   xerial-ba  0 (alength xerial-ba))))))

  (is ; CBC auto-encryptor compatibility
    (= "payload"
      (thaw (freeze "payload" {:password [:salted "pwd"] :encryptor nippy/aes128-cbc-encryptor})
        (do                   {:password [:salted "pwd"]})))))

;;;; Custom types & records

(deftype   MyType [data])
(defrecord MyRec  [data])

(deftest _types
  ;;; Extend to custom Type
  (is (thrown? Exception ; No thaw extension yet
        (do (alter-var-root #'nippy/*custom-readers* (constantly {}))
            (nippy/extend-freeze MyType 1 [x s] (.writeUTF s (.data x)))
            (thaw (freeze (MyType. "val"))))))
  (is (do (nippy/extend-thaw 1 [s] (MyType. (.readUTF s)))
          (let [mt (MyType. "val")] (= (.data ^MyType mt)
                                       (.data ^MyType (thaw (freeze mt)))))))

  ;;; Extend to custom Record
  (is (do (nippy/extend-freeze MyRec 2 [x s] (.writeUTF s (str "foo-" (:data x))))
          (nippy/extend-thaw 2 [s] (MyRec. (.readUTF s)))
          (= (MyRec. "foo-val") (thaw (freeze (MyRec. "val"))))))

  ;;; Keyword (prefixed) extensions
  (is
    (do (nippy/extend-freeze MyRec :nippy-tests/MyRec [x s] (.writeUTF s (:data x)))
        (nippy/extend-thaw :nippy-tests/MyRec [s] (MyRec. (.readUTF s)))
        (let [mr (MyRec. "val")] (= mr (thaw (freeze mr)))))))

;;;; Caching

(deftest _caching
  (let [stress [nippy/stress-data-comparable
                nippy/stress-data-comparable
                nippy/stress-data-comparable
                nippy/stress-data-comparable]
        cached (mapv nippy/cache stress)
        cached (mapv nippy/cache stress) ; <=1 wrap auto-enforced
        ]

    (is (= stress (thaw (freeze stress {:compressor nil}))))
    (is (= stress (thaw (freeze cached {:compressor nil}))))
    (let [size-stress (count (freeze stress {:compressor nil}))
          size-cached (count (freeze cached {:compressor nil}))]
      (is (>= size-stress (* 3 size-cached)))
      (is (<  size-stress (* 4 size-cached))))))

(deftest _caching-metadata
  (let [v1 (with-meta [] {:id :v1})
        v2 (with-meta [] {:id :v2})

        frozen-without-caching (freeze [v1 v2 v1 v2])
        frozen-with-caching
        (freeze [(nippy/cache v1)
                 (nippy/cache v2)
                 (nippy/cache v1)
                 (nippy/cache v2)])]

    (is (> (count frozen-without-caching)
           (count frozen-with-caching)))

    (is (= (thaw frozen-without-caching)
           (thaw frozen-with-caching)))

    (is (= (mapv meta (thaw frozen-with-caching))
           [{:id :v1} {:id :v2} {:id :v1} {:id :v2}]))))

;;;; Stable binary representation of vals

(deftest _stable-bin

  (is (= (seq (freeze test-data))
         (seq (freeze test-data)))) ; f(x)=f(y) | x=y

  ;; As above, but try multiple times to catch possible protocol interface races:
  (is (every? true?
        (repeatedly 1000 (fn [] (= (seq (freeze test-data))
                                  (seq (freeze test-data)))))))

  ;; NB abandoning - no way to do this reliably w/o appropriate contracts from
  ;; (seq <unordered-coll>):
  ;;
  ;; (is (=  (seq (-> test-data freeze))
  ;;         (seq (-> test-data freeze thaw freeze)))) ; f(x)=f(f-1(f(x)))
  ;;
  ;; As above, but with repeated refreeze to catch possible protocol interface races:
  ;; (is (= (seq (freeze test-data))
  ;;        (seq (reduce (fn [frozen _] (freeze (thaw frozen)))
  ;;               (freeze test-data) (range 1000)))))
  )

(defn qc-prop-bijection [& [n]]
  (let [bin->val (atom {})
        val->bin (atom {})]
    (merge
      (tc/quick-check (or n 1)
        (tc-props/for-all [val tc-gens]
          (let [;; Nb need `seq` for Clojure hash equality:
                bin (hash (seq (freeze val)))]
            (and
              (if (contains? val->bin val)
                (= (get val->bin val) bin) ; x=y => f(x)=f(y) by clj=
                (do (swap! val->bin assoc val bin)
                    true))

              (if (contains? bin->val bin)
                (= (get bin->val bin) val) ; f(x)=f(y) => x=y by clj=
                (do (swap! bin->val assoc bin val)
                    true))))))
      #_{:bin->val @bin->val
         :val->bin @val->bin}
      nil)))

(comment
  (tc-gens/sample tc-gens 10)
  (:result (qc-prop-bijection 80))
  (let [{:keys [result bin->val val->bin]} (qc-prop-bijection 10)]
    [result (vals bin->val)]))

(deftest _gc-prop-bijection
  (is (:result (qc-prop-bijection tc-num-tests))))

;;;; Thread safety

(deftest _thread-safe
  (is
    (let [futures
          (mapv
            (fn [_]
              (future
                (= (thaw (freeze test-data)) test-data)))
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
      (every? deref futures))))

;;;; Redefs

(defrecord MyFoo [] Object (toString [_] "v1"))
(str (thaw (freeze (MyFoo.))))
(defrecord MyFoo [] Object (toString [_] "v2"))

(deftest _redefs
  (is (= (str (thaw (freeze (MyFoo.)))) "v2")))

;;;; Serializable

(do
  (def ^:private semcn              "java.util.concurrent.Semaphore")
  (def ^:private sem                (java.util.concurrent.Semaphore. 1))
  (defn-         sem? [x] (instance? java.util.concurrent.Semaphore x)))

(deftest _serializable
  (is (= nippy/*thaw-serializable-allowlist* #{"base.1" "base.2" "add.1" "add.2"})
    "JVM properties override initial allowlist values")

  (is (thrown? Exception (nippy/freeze sem {:serializable-allowlist #{}}))
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

    (is (= :quarantined (get-in thawed [:nippy/unthawable :cause]))
      "Serializable objects will be quarantined when approved for freezing but not thawing.")

    (is (sem? (nippy/read-quarantined-serializable-object-unsafe! thawed))
      "Quarantined Serializable objects can still be manually force-read.")

    (is (sem? (nippy/read-quarantined-serializable-object-unsafe!
                (nippy/thaw (nippy/freeze thawed))))
      "Quarantined Serializable objects are themselves safely transportable."))

  (let [obj
        (nippy/thaw
          (nippy/freeze sem)
          {:serializable-allowlist "allow-and-record"})]

    (is (sem? obj)
      "Special \"allow-and-record\" allowlist permits any class")

    (is
      (contains? (nippy/get-recorded-serializable-classes) semcn)
      "Special \"allow-and-record\" allowlist records classes")))

;;;; Metadata

(deftest _metadata

  (is
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

    "Metadata successfully excluded by freeze"))


(deftest java-time-extension-test
  (let [inst (java.time.Instant/now)]
    (is (= inst (nippy/thaw (nippy/freeze inst))))))

;;;; Benchmarks

(deftest _benchmarks
  (is (benchmarks/bench {})) ; Also tests :cached passwords
  )

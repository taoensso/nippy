(ns taoensso.nippy.utils
  (:require [clojure.string  :as str]
            [taoensso.encore :as enc])
  (:import  [java.io ByteArrayInputStream ByteArrayOutputStream Serializable
             ObjectOutputStream ObjectInputStream]))

;;;; Fallback type tests
;; Unfortunately the only ~reliable way we can tell if something's
;; really serializable/readable is to actually try a full roundtrip.

(let [swap-cache! @#'enc/-swap-cache!] ; TODO Public with newer Encore
  (defn- memoize-type-test [test-fn]
    (let [cache (atom {})] ; {<type> <type-okay?>}
      (fn [x]
        (let [t (type x)
              ;; This is a bit hackish, but no other obvious solutions (?):
              cacheable? (not (re-find #"__\d+" (str t))) ; gensym form
              test (fn [] (try (test-fn x) (catch Exception _ false)))]
          (if cacheable?
            @(swap-cache! cache t #(if % % (delay (test))))
            (test)))))))

(def     readable? (memoize-type-test (fn [x] (-> x enc/pr-edn enc/read-edn) true)))
(def serializable?
  (let [test-fn
        (fn [x]
          (let [class-name (.getName (class x))
                class ^Class (Class/forName class-name) ; Try 1st (fail fast)
                bas (ByteArrayOutputStream.)
                _   (.writeObject (ObjectOutputStream. bas) x)
                ba  (.toByteArray bas)
                object (.readObject (ObjectInputStream.
                                     (ByteArrayInputStream. ba)))]
            (cast class object)
            true))

        mtt (memoize-type-test test-fn)]

    (fn [x]
      (if (instance? Serializable x)
        (if (fn? x)
          false ; Reports as true but is unreliable
          (mtt x))
       false))))

(comment
  (enc/qb 10000
    (readable?     "Hello world") ; Cacheable
    (serializable? "Hello world") ; Cacheable
    (readable?     (fn []))        ; Uncacheable
    (serializable? (fn []))        ; Uncacheable
    ))

;;;;

(defn- is-coll?
  "Checks for _explicit_ IPersistentCollection types with Nippy support.
  Checking for explicit concrete types is tedious but preferable since a
  `freezable?` false positive would be much worse than a false negative."
  [x]
  (let [is? #(when (instance? % x) %)]
    (or
     (is? clojure.lang.APersistentVector)
     (is? clojure.lang.APersistentMap)
     (is? clojure.lang.APersistentSet)
     (is? clojure.lang.PersistentList)
     (is? clojure.lang.PersistentList$EmptyList) ; (type '())
     (is? clojure.lang.PersistentQueue)
     (is? clojure.lang.PersistentTreeSet)
     (is? clojure.lang.PersistentTreeMap)
     (is? clojure.lang.PersistentVector$ChunkedSeq)

     (is? clojure.lang.IRecord) ; TODO Possible to avoid the interface check?
     (is? clojure.lang.LazySeq)

     ;; Too non-specific: could result in false positives (which would be a
     ;; serious problem here):
     ;; (is? clojure.lang.ISeq)

     )))

(comment (is-coll? (clojure.lang.PersistentVector$ChunkedSeq. [1 2 3] 0 0)))

(defmacro ^:private is? [x c] `(when (instance? ~c ~x) ~c))

(defn freezable?
  "Alpha - subject to change.
  Returns truthy iff Nippy *appears* to support freezing the given argument.

  `:allow-clojure-reader?` and `:allow-java-serializable?` options may be
  used to enable the relevant roundtrip fallback test(s). These tests are
  only **moderately reliable** since they're cached by arg type and don't
  test for pre/post serialization value equality (there's no good general
  way of doing so)."

  ;; TODO Not happy with this approach in general, could do with a refactor.
  ;; Maybe return true/false/nil (nil => maybe)?

  ([x] (freezable? x nil))
  ([x {:keys [allow-clojure-reader? allow-java-serializable?]}]
   (if (is-coll? x)
     (when (enc/revery? freezable? x) (type x))
     (or
       (is? x clojure.lang.Keyword)
       (is? x java.lang.String)
       (is? x java.lang.Long)
       (is? x java.lang.Double)
       (nil? x)

       (is? x clojure.lang.BigInt)
       (is? x clojure.lang.Ratio)

       (is? x java.lang.Boolean)
       (is? x java.lang.Integer)
       (is? x java.lang.Short)
       (is? x java.lang.Byte)
       (is? x java.lang.Character)
       (is? x java.math.BigInteger)
       (is? x java.math.BigDecimal)
       (is? x #=(java.lang.Class/forName "[B"))

       (is? x clojure.lang.Symbol)

       (is? x java.util.Date)
       (is? x java.util.UUID)
       (is? x java.util.regex.Pattern)

       (when (and allow-clojure-reader?    (readable?     x)) :clojure-reader)
       (when (and allow-java-serializable? (serializable? x)) :java-serializable)))))

(comment
  (enc/qb 10000 (freezable? "hello")) ; 0.79
  (freezable? [:a :b])
  (freezable? [:a (fn [x] (* x x))])
  (freezable? (.getBytes "foo"))
  (freezable? (java.util.Date.) {:allow-clojure-reader?    true})
  (freezable? (Exception. "_")  {:allow-clojure-reader?    true})
  (freezable? (Exception. "_")  {:allow-java-serializable? true})
  (freezable? (atom {}) {:allow-clojure-reader?    true
                         :allow-java-serializable? true}))

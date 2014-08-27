(ns taoensso.nippy.utils
  {:author "Peter Taoussanis"}
  (:require [clojure.string           :as str]
            [clojure.tools.reader.edn :as edn]
            [taoensso.encore          :as encore])
  (:import  [java.io ByteArrayInputStream ByteArrayOutputStream Serializable
             ObjectOutputStream ObjectInputStream]))

;;;; Fallback type tests
;; Unfortunately the only reliable way we can tell if something's
;; really serializable/readable is to actually try a full roundtrip.

(defn- memoize-type-test [f-test]
  (let [cache (atom {})] ; {<type> <type-okay?>}
    (fn [x]
      (let [t (type x)
            ;; This is a bit hackish, but no other obvious solutions (?):
            cacheable? (not (re-find #"__\d+" (str t))) ; gensym form
            test (fn [] (try (f-test x) (catch Exception _ false)))]
        (if-not cacheable? (test)
          @(encore/swap-val! cache t #(if % % (delay (test)))))))))

(def serializable?
  (memoize-type-test
   (fn [x]
     (when (instance? Serializable x)
       (let [class-name (.getName (class x))
             class ^Class (Class/forName class-name) ; Try 1st (fail fast)
             bas (ByteArrayOutputStream.)
             _   (.writeObject (ObjectOutputStream. bas) x)
             ba  (.toByteArray bas)
             object (.readObject (ObjectInputStream.
                                  (ByteArrayInputStream. ba)))]
         (cast class object)
         true)))))

(def readable? (memoize-type-test (fn [x] (-> x pr-str (edn/read-string)) true)))

(comment
  (serializable? "Hello world")
  (serializable? (fn []))
  (readable? "Hello world")
  (readable? (fn []))

  (time (dotimes [_ 10000] (serializable? "Hello world"))) ; Cacheable
  (time (dotimes [_ 10000] (serializable? (fn []))))        ; Uncacheable
  (time (dotimes [_ 10000] (readable? "Hello world")))     ; Cacheable
  (time (dotimes [_ 10000] (readable? (fn [])))))           ; Uncacheable

;;;;

(defn- is-coll?
  "Checks for _explicit_ IPersistentCollection types with Nippy support."
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
     (is? clojure.lang.IRecord)
     (is? clojure.lang.LazySeq)
     ;; (is? clojure.lang.ISeq)
     )))

(defn freezable?
  "Alpha - subject to change, may be buggy!
  Returns truthy value iff Nippy supports de/serialization of given argument.
  Conservative with default options.

  `:allow-clojure-reader?` and `:allow-java-serializable?` options may be used
  to also enable the relevant roundtrip fallback test(s). These tests are only
  **moderately reliable** since they're cached by arg type and don't test for
  pre/post serialization equality (there's no good general way of doing so)."
  [x & [{:keys [allow-clojure-reader? allow-java-serializable?]}]]
  (let [is? #(when (instance? % x) %)]
    (if (is-coll? x)
      (try
        (when (every? freezable? x) (type x))
        (catch Exception _ false))
      (or
       (is? clojure.lang.Keyword)
       (is? java.lang.String)
       (is? java.lang.Long)
       (is? java.lang.Double)

       (is? clojure.lang.BigInt)
       (is? clojure.lang.Ratio)

       (is? java.lang.Boolean)
       (is? java.lang.Integer)
       (is? java.lang.Short)
       (is? java.lang.Byte)
       (is? java.lang.Character)
       (is? java.math.BigInteger)
       (is? java.math.BigDecimal)
       (is? #=(java.lang.Class/forName "[B"))

       (is? java.util.Date)
       (is? java.util.UUID)

       (when (and allow-clojure-reader? (readable? x)) :clojure-reader)
       (when (and allow-java-serializable?
                  ;; Reports as true but is unreliable:
                  (not (is? clojure.lang.Fn))
                  (serializable? x)) :java-serializable)))))

(comment
  (time (dotimes [_ 10000] (freezable? "hello")))
  (freezable? [:a :b])
  (freezable? [:a (fn [x] (* x x))])
  (freezable? (.getBytes "foo"))
  (freezable? (java.util.Date.) {:allow-clojure-reader?    true})
  (freezable? (Exception. "_")  {:allow-clojure-reader?    true})
  (freezable? (Exception. "_")  {:allow-java-serializable? true})
  (freezable? (atom {}) {:allow-clojure-reader?    true
                         :allow-java-serializable? true}))

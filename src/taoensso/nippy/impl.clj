(ns ^:no-doc taoensso.nippy.impl
  "Private, implementation detail."
  (:require
   [clojure.string  :as str]
   [taoensso.encore :as enc]))

;;;; Fallback type tests

(defn- memoize-type-test
  "Unfortunately the only ~reliable way we can tell if something's
  really serializable/readable is to actually try a full roundtrip."
  [test-fn]
  (let [cache_ (enc/latom {})] ; {<type> <type-okay?>}
    (fn [x]
      (let [t          (type x)
            gensym?    (re-find #"__\d+" (str t))
            cacheable? (not gensym?) ; Hack, but no obviously better solutions
            test       (fn [] (try (test-fn x) (catch Exception _ false)))]

        (if cacheable?
          @(cache_ t #(if % % (delay (test))))
          (do                        (test)))))))

(def seems-readable? (memoize-type-test (fn [x] (-> x enc/pr-edn enc/read-edn) true)))
(def seems-serializable?
  (let [mtt
        (memoize-type-test
          (fn [x]
            (let [class-name (.getName (class x))
                  c   (Class/forName class-name) ; Try 1st (fail fast)
                  bas (java.io.ByteArrayOutputStream.)
                  _   (.writeObject (java.io.ObjectOutputStream. bas) x)
                  ba  (.toByteArray bas)]

              #_
              (cast c
                (.readObject ; Unsafe + usu. unnecessary to check
                  (ObjectInputStream.
                    (ByteArrayInputStream. ba))))

              true)))]

    (fn [x]
      (if (instance? java.io.Serializable x)
        (if (fn? x)
          false ; Reports as true but is unreliable
          (mtt x))
       false))))

(comment
  (enc/qb 1e4 ; [2.52 2.53 521.34 0.63]
    (seems-readable?     "Hello world") ; Cacheable
    (seems-serializable? "Hello world") ; Cacheable
    (seems-readable?     (fn []))       ; Uncacheable
    (seems-serializable? (fn []))       ; Uncacheable
    ))

;;;;

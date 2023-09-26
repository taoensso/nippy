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

;;;; Java Serializable

(defn- allow-and-record?     [s] (= s "allow-and-record"))
(defn- split-class-names>set [s] (when (string? s) (if (= s "") #{} (set (mapv str/trim (str/split s #"[,:]"))))))
(comment
  (split-class-names>set "")
  (split-class-names>set "foo, bar:baz"))

(comment (.getName (.getSuperclass (.getClass (java.util.concurrent.TimeoutException.)))))

(let [ids
      {:freeze {:base :taoensso.nippy.freeze-serializable-allowlist-base
                :add  :taoensso.nippy.freeze-serializable-allowlist-add}
       :thaw   {:base :taoensso.nippy.thaw-serializable-allowlist-base
                :add  :taoensso.nippy.thaw-serializable-allowlist-add}
       :legacy {:base :taoensso.nippy.serializable-whitelist-base
                :add  :taoensso.nippy.serializable-whitelist-add}}]

  (defn init-serializable-allowlist
    [action default incl-legacy?]
    (let [allowlist-base
          (or
            (when-let [s
                       (or
                         (do                (enc/get-sys-val* (get-in ids [action  :base])))
                         (when incl-legacy? (enc/get-sys-val* (get-in ids [:legacy :base]))))]

              (if (allow-and-record? s) s (split-class-names>set s)))
            default)

          allowlist-add
          (when-let [s
                     (or
                       (do                (enc/get-sys-val* (get-in ids [action  :add])))
                       (when incl-legacy? (enc/get-sys-val* (get-in ids [:legacy :add]))))]

            (if (allow-and-record? s) s (split-class-names>set s)))]

      (if (and allowlist-base allowlist-add)
        (into (enc/have set? allowlist-base) allowlist-add)
        (do                  allowlist-base)))))

;;;

(let [nmax    1000
      ngc     16000
      state_  (enc/latom {})  ; {<class-name> <frequency>}
      lock_   (enc/latom nil) ; ?promise
      trim
      (fn [nmax state]
        (persistent!
          (enc/reduce-top nmax val enc/rcompare conj!
            (transient {}) state)))]

  ;; Note: trim strategy isn't perfect: it can be tough for new
  ;; classes to break into the top set since frequencies are being
  ;; reset only for classes outside the top set.
  ;;
  ;; In practice this is probably good enough since the main objective
  ;; is to discard one-off anonymous classes to protect state from
  ;; endlessly growing. Also `gc-rate` allows state to temporarily grow
  ;; significantly beyond `nmax` size, which helps to give new classes
  ;; some chance to accumulate a competitive frequency before next GC.

  (defn ^{:-state_ state_} ; Undocumented
    allow-and-record-any-serializable-class-unsafe
    "A predicate (fn allow-class? [class-name]) fn that can be assigned
    to `*freeze-serializable-allowlist*` and/or
         `*thaw-serializable-allowlist*` that:

      - Will allow ANY class to use Nippy's `Serializable` support (unsafe).
      - And will record {<class-name> <frequency-allowed>} for the <=1000
        classes that ~most frequently made use of this support.

    `get-recorded-serializable-classes` returns the recorded state.

    This predicate is provided as a convenience for users upgrading from
    previous versions of Nippy that allowed the use of `Serializable` for all
    classes by default.

    While transitioning from an unsafe->safe configuration, you can use
    this predicate (unsafe) to record information about which classes have
    been using Nippy's `Serializable` support in your environment.

    Once some time has passed, you can check the recorded state. If you're
    satisfied that all recorded classes are safely `Serializable`, you can
    then merge the recorded classes into Nippy's default allowlist/s, e.g.:

    (alter-var-root #'thaw-serializable-allowlist*
      (fn [_] (into default-thaw-serializable-allowlist
                (keys (get-recorded-serializable-classes)))))"

    [class-name]
    (when-let [p (lock_)] @p)
    (let [n (count (state_ #(assoc % class-name (inc (long (or (get % class-name) 0))))))]
      ;; Garbage collection (GC): may be serializing anonymous classes, etc.
      ;; so input domain could be infinite
      (when (> n ngc) ; Too many classes recorded, uncommon
        (let [p (promise)]
          (when (compare-and-set! lock_ nil p) ; Acquired GC lock
            (try
              (do      (reset! state_ (trim nmax (state_)))) ; GC state
              (finally (reset! lock_  nil) (deliver p nil))))))
      n))

  (defn get-recorded-serializable-classes
    "Returns {<class-name> <frequency>} of the <=1000 classes that ~most
    frequently made use of Nippy's `Serializable` support via
    `allow-and-record-any-serializable-class-unsafe`.

    See that function's docstring for more info."
    [] (trim nmax (state_))))

;;;

(comment
  (count (get-recorded-serializable-classes))
  (enc/reduce-n
    (fn [_ n] (allow-and-record-any-serializable-class-unsafe (str n)))
    nil 0 1e5))

(let [compile
      (enc/fmemoize
        (fn [x]
          (if (allow-and-record? x)
            allow-and-record-any-serializable-class-unsafe
            (enc/name-filter x))))

      fn? fn?
      conform?
      (fn [x cn]
        (if (fn? x)
          (x cn) ; Intentionally uncached, can be handy
          ((compile x) cn)))]

  (defn serializable-allowed? [allow-list class-name]
    (conform? allow-list class-name)))

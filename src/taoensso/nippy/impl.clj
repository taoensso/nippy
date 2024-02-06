(ns ^:no-doc taoensso.nippy.impl
  "Private, implementation detail."
  (:require
   [clojure.string  :as str]
   [taoensso.encore :as enc]))

;;;; Fallback type tests

(defn cache-by-type [f]
  (let [cache_ (enc/latom {})] ; {<type> <result_>}
    (fn [x]
      (let [t (if (fn? x) ::fn (type x))]
        (if-let [result_ (get (cache_) t)]
          @result_
          (if-let [uncacheable-type? (re-find #"\d" (str t))]
            (do                      (f x))
            @(cache_ t #(or % (delay (f x))))))))))

(def seems-readable?
  (cache-by-type
    (fn [x]
      (try
        (enc/read-edn (enc/pr-edn x))
        true
        (catch Throwable _ false)))))

(def seems-serializable?
  (cache-by-type
    (fn [x]
      (enc/cond
        (fn? x) false ; Falsely reports as Serializable

        (instance? java.io.Serializable x)
        (try
          (let [c   (Class/forName (.getName (class x))) ; Try 1st (fail fast)
                bas (java.io.ByteArrayOutputStream.)
                _   (.writeObject (java.io.ObjectOutputStream. bas) x)
                ba  (.toByteArray bas)]
            #_
            (cast c
              (.readObject ; Unsafe + usu. unnecessary to check
                (ObjectInputStream. (ByteArrayInputStream. ba))))
            true)
          (catch Throwable _ false))

        :else false))))

(comment
  (enc/qb 1e6 ; [60.83 61.16 59.86 57.37]
    (seems-readable?     "Hello world")
    (seems-serializable? "Hello world")
    (seems-readable?     (fn []))
    (seems-serializable? (fn []))))

;;;; Java Serializable

(def ^:const ^:private allow-and-record          "allow-and-record")
(defn-                 allow-and-record? [x] (= x allow-and-record))

(defn- classname-set
  "Returns ?#{<classname>}."
  [x]
  (when x
    (if (string? x)
      (if (= x "") #{} (set (mapv str/trim (str/split x #"[,:]"))))
      (enc/have set? x))))

(comment
  (mapv classname-set [nil #{"foo"} "" "foo, bar:baz"])
  (.getName (.getSuperclass (.getClass (java.util.concurrent.TimeoutException.)))))

(defn parse-allowlist
  "Returns #{<classname>}, or `allow-and-record`."
  [default base add]
  (if (or
        (allow-and-record? base)
        (allow-and-record? add))
    allow-and-record
    (into
      (or (classname-set base) default)
      (do (classname-set add)))))

(comment (parse-allowlist #{"default"} "base1,base2" "add1"))

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

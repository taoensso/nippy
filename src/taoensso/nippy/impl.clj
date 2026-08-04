(ns ^:no-doc taoensso.nippy.impl
  "Private misc utils, don't use."
  (:require
   [clojure.string  :as str]
   [taoensso.truss  :as truss]
   [taoensso.encore :as enc]))

;;;;

(defmacro when-debug [& body] (when #_true false `(do ~@body)))

;;;; Compatibility config

(def target-release
  "When freezing values, Nippy will target compatibility with the Nippy version
  specified here: 325 for Nippy v3.2.5, etc.

  Used to help ease data migration for changes to core data types.

  This setting is consumed while Nippy's writer implementations compile. Run a
  clean build whenever changing it so stale compiled writers are not reused.

  When support is added for a new type in Nippy version X, it necessarily means
  that data containing that new type and frozen with Nippy version X is unthawable
  with Nippy versions < X.

  Earlier versions of Nippy will throw an exception on thawing affected data:
    \"Unrecognized type id (<n>). Data frozen with newer Nippy version?\"

  This can present a challenge when updating to new versions of Nippy, e.g.:

    - Rolling updates could lead to old and new versions of Nippy temporarily co-existing.
    - Data written with new types could limit your ability to revert a Nippy update.

  There's no easy solution to this in GENERAL, but we CAN at least help reduce the
  burden related to CHANGES in core data types by introducing changes over 2 phases:

    1. Nippy vX   reads  new (changed) type, writes old type
    2. Nippy vX+1 writes new (changed) type

  When relevant, we can then warn users in the CHANGELOG to not leapfrog
  (e.g. Nippy vX -> Nippy vX+2) when doing rolling updates."

  (enc/get-env {:as :edn, :default 370} :taoensso.nippy.target-release))

(let [target>=
      (fn [min-release]
        (if         target-release
          (>= (long target-release) (long min-release))
          true))]

  (defmacro target-release<  [min-release] (not (target>= min-release)))
  (defmacro target-release>= [min-release]      (target>= min-release)))

(defmacro pack-unsigned?
  "Use tight packing for unsigned integer types?
  Reduces output size for values in some narrow ranges, at the cost of
  writing type ids that were added in Nippy v3.3.0. So enabled only when
  `target-release` >= 330."
  [] `(target-release>= 330))

(comment (macroexpand '(target-release>= 350)))

(comment
  ;; Track new type ids added over time
  (vec (sort (keys taoensso.nippy/public-types-spec)))

  (let [id-history ; {<release> #{type-ids}}
        {370 ; v3.7.0 (2026-07-22), added 6x
         #{0 1 2 3 4 5 6 7 8 9 10 11 12 13 14 15 16 17 18 19 20 21 22 23 24 25 26 27 28
           29 30 31 32 33 34 35 36 37 38 39 40 41 42 43 44 45 46 47 48 49 50 51 52 53 54
           55 56 57 58 59 60 61 62 63 64 65 66 67 68 69 70 71 72 73 74 75 76 77 78 79 80
           81 82 83 84 85 86 87 88 89 90 91 92 93 94 95 96 97 98 99 100 101 102 103 104
           105 106 107 108 109 110 111 112 113 114 115 116 117,
           118 119 120 121 122 123 ; New (123 retired v3.8.0, read-only)
           }

         350 ; v3.5.0 (2025-04-15), added 5x
         ;; #{int-array-lg long-array-lg float-array-lg double-array-lg string-array-lg}
         #{0 1 2 3 4 5 6 7 8 9 10 11 12 13 14 15 16 17 18 19 20 21 22 23 24 25 26 27 28
           29 30 31 32 33 34 35 36 37 38 39 40 41 42 43 44 45 46 47 48 49 50 51 52 53 54
           55 56 57 58 59 60 61 62 63 64 65 66 67 68 69 70 71 72 73 74 75 76 77 78 79 80
           81 82 83 84 85 86 87 88 89 90 91 92 93 94 95 96 97 98 99 100 101 102 103 104
           105 106 107 108 109 110 111 112 113 114 115 116 117}

         340 ; v3.4.0 (2024-04-30), added 2x
         ;; #{map-entry meta-protocol-key}
         #{0 1 2 3 4 5 6 7 8 9 10 11 12 13 14 15 16 17 18 19 20 21 22 23 24 25 26 27 28
           29 30 31 32 33 34 35 36 37 38 39 40 41 42 43 44 45 46 47 48 49 50 51 52 53 54
           55 56 57 58 59 60 61 62 63 64 65 66 67 68 69 70 71 72 73 74 75 76 77 78 79 80
           81 82 83 84 85 86 87 88 89 90 91 92 93 94 95 96 97 98 99 100 101 102 103 104
           105 106 110 111 112 113 114 115}

         330 ; v3.3.0 (2023-10-11), added 11x
         ;; #{long-pos-sm long-pos-md long-pos-lg long-neg-sm long-neg-md long-neg-lg
         ;;   str-sm* vec-sm* set-sm* map-sm* sql-date}
         #{0 1 2 3 4 5 6 7 8 9 10 11 12 13 14 15 16 17 18 19 20 21 22 23 24 25 26 27 28
           29 30 31 32 33 34 35 36 37 38 39 40 41 42 43 44 45 46 47 48 49 50 51 52 53 54
           55 56 57 58 59 60 61 62 63 64 65 66 67 68 69 70 71 72 73 74 75 76 77 78 79 80
           81 82 83 84 85 86 87 88 89 90 91 92 93 94 95 96 97 98 99 100 101 102 105 106
           110 111 112 113 114 115}

         320 ; v3.2.0 (2022-07-18), added none
         #{0 1 2 3 4 5 6 7 8 9 10 11 12 13 14 15 16 17 18 19 20 21 22 23 24 25 26 27 28
           29 30 31 32 33 34 35 36 37 38 39 40 41 42 43 44 45 46 47 48 49 50 51 52 53 54
           55 56 57 58 59 60 61 62 63 64 65 66 67 68 69 70 71 72 73 74 75 76 77 78 79 80
           81 82 83 84 85 86 90 91 100 101 102 105 106 110 111 112 113 114 115}

         313 ; v3.1.3 (2022-06-23), added 5x
         ;; #{time-instant time-duration time-period kw-md sym-md}
         #{0 1 2 3 4 5 6 7 8 9 10 11 12 13 14 15 16 17 18 19 20 21 22 23 24 25 26 27 28
           29 30 31 32 33 34 35 36 37 38 39 40 41 42 43 44 45 46 47 48 49 50 51 52 53 54
           55 56 57 58 59 60 61 62 63 64 65 66 67 68 69 70 71 72 73 74 75 76 77 78 79 80
           81 82 83 84 85 86 90 91 100 101 102 105 106 110 111 112 113 114 115}

         300 ; v3.0.0 (2020-09-20), baseline
         #{0 1 2 3 4 5 6 7 8 9 10 11 12 13 14 15 16 17 18 19 20 21 22 23 24 25 26 27 28
           29 30 31 32 33 34 35 36 37 38 39 40 41 42 43 44 45 46 47 48 49 50 51 52 53 54
           55 56 57 58 59 60 61 62 63 64 65 66 67 68 69 70 71 72 73 74 75 76 77 78 80
           81 82 90 91 100 101 102 105 106 110 111 112 113 114 115}}

        diff
        (fn [new-release old-release]
          (vec (sort (clojure.set/difference (id-history new-release) (id-history old-release)))))]

    (diff 350 340)))

;;;; Java Serializable config

(def ^:const ^:private allow-and-record          "allow-and-record")
(defn-                 allow-and-record? [x] (= x allow-and-record))

(defn- classname-set
  "Returns ?#{<classname>}."
  [x]
  (when x
    (if (string? x)
      (if (= x "") #{} (set (mapv str/trim (str/split x #"[,:]"))))
      (truss/have set? x))))

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
    ^:public allow-and-record-any-serializable-class-unsafe
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

  (defn ^:public get-recorded-serializable-classes
    "Returns {<class-name> <frequency>} of the <=1000 classes that ~most
    frequently made use of Nippy's `Serializable` support via
    `allow-and-record-any-serializable-class-unsafe`.

    See that function's docstring for more info."
    [] (trim nmax (state_))))

(comment
  (count (get-recorded-serializable-classes))
  (enc/reduce-n
    (fn [_ n] (allow-and-record-any-serializable-class-unsafe (str n)))
    nil 0 1e5))

(enc/declare-remote
  ^:dynamic taoensso.nippy/*serializable-whitelist*
  ^:dynamic taoensso.nippy/*freeze-serializable-allowlist*
  ^:dynamic taoensso.nippy/*thaw-serializable-allowlist*)

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

  (defn-       serializable-allowed? [class-name allow-list] (conform? allow-list class-name))
  (defn freeze-serializable-allowed? [x] (serializable-allowed? x taoensso.nippy/*freeze-serializable-allowlist*))
  (defn   thaw-serializable-allowed? [x] (serializable-allowed? x
                                           (or
                                             taoensso.nippy/*serializable-whitelist*
                                             taoensso.nippy/*thaw-serializable-allowlist*))))

(comment
  (enc/qb 1e6 (freeze-serializable-allowed? "foo")) ; 46.03
  (binding [taoensso.nippy/*freeze-serializable-allowlist* #{"foo.*" "bar"}]
    (freeze-serializable-allowed? "foo.bar")))

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

;;;; Object cache

(deftype Cached [val])
(defn ^:public cache
  "Wraps value so that future writes of the same wrapped value with same
  metadata will be efficiently encoded as references to this one.

  (freeze [(cache \"foo\") (cache \"foo\") (cache \"foo\")])
    will incl. a single \"foo\", plus 2x single-byte references to \"foo\".

  Repeated keywords are cached automatically so don't need manual wrapping."
  [x] (if (instance? Cached x) x (Cached. x)))

;;;; Shared dicts

(def ^:private ^:const max-dict-count 32767) ; Same md-count cap as session cache idxs
(def ^:private ^:const fnv64-offset -3750763034362895579) ; FNV-1a 64 offset basis
(def ^:private ^:const fnv64-prime   1099511628211)       ; FNV-1a 64 prime

(defn- fnv64-b  ^long [^long h ^long b] (unchecked-multiply (bit-xor h b) fnv64-prime))
(defn- fnv64-ba ^long [^long h ^bytes ba]
  (let [len (alength ba)]
    (loop [i 0, h h]
      (if (== i len)
        h
        (recur (unchecked-inc i)
          (fnv64-b h (bit-and (aget ba i) 0xff)))))))

(defn- entry-hash
  "Returns a stable 64-bit hash of given `shared-dict` entry (kw or string)
  for dict fingerprints."
  ^long [x]
  ;; Uses an explicit FNV-1a fold over UTF-8 bytes since these hashes go to the wire
  ;; and so must be stable across JVM/Clojure/Nippy versions.
  (fnv64-ba (fnv64-b (long fnv64-offset) (if (keyword? x) 0 1))
    (.getBytes ^String (str x) java.nio.charset.StandardCharsets/UTF_8)))

(deftype SharedDict
  ;; Static shared dict of known kws + strings, Ref. `shared-dict`.
  ;; Immutable after construction => safe to share across threads/sessions
  [^objects entries                    ; idx -> kw | string
   ^java.util.IdentityHashMap kw-idxs  ; {kw  idx} (kws interned => identity is equality)
   ^java.util.HashMap         str-idxs ; {str idx}
   ^long max-str-len                   ; Longest str entry (-1 if none)
   ^longs prefix-hashes                ; [i] = fingerprint of entries[0..i-1], for append-only dict evolution
   ]

  clojure.lang.IDeref (deref [_] (vec entries)))

(defn ^:public shared-dict
  "Experimental, subject to change. Feedback welcome!

  Given an ordered coll of keywords and/or strings, returns a precompiled
  dictionary schema for use with `*shared-dict*` or `:shared-dict` opts:

    {:name \"Alice\", \"country\" \"DE\", \"currency\" \"EUR\"} ; Row data

    (def row-dict (nippy/shared-dict [:name \"country\" \"currency\"])) ; Row schema
    (nippy/freeze x  {:shared-dict row-dict})
    (nippy/thaw   ba {:shared-dict row-dict})

  All participating freeze and thaw calls then encode these known values
  as small 1-3 byte references.

  Usage:

  - Version and store your dict definition with your code!

  - Thawing data that used a dict entry REQUIRES a dict whose leading
    entries are identical (same entries and order) to the dict used to
    freeze (Nippy validates this and throws on mismatch).

  - NEVER remove or reorder entries. APPENDING is safe: data frozen
    with an older (shorter) version of a dict thaws fine with a newer
    (appended-to) version. So during rollouts, upgrade dicts on your
    readers before your writers.

  - Order entries by expected frequency: the first 8 get 1-byte refs.
  - Max dict size: 32767 entries.
  - Deref a dict to get back its entry vector."

  [entries]
  (when-not (and (sequential? entries) (<= 1 (count entries) max-dict-count))
    (truss/ex-info! "`shared-dict` expects a non-empty ordered sequence of <= 32767 keywords/strings"
      {:type (type entries), :count (when (sequential? entries) (count entries))}))

  (let [n    (count entries)
        earr (object-array n)
        kwm  (java.util.IdentityHashMap. n)
        strm (java.util.HashMap. (int (/ n 0.7))) ; Cap ctor arg, avoid rehash
        ph   (long-array (inc n))
        enc8 (.newEncoder java.nio.charset.StandardCharsets/UTF_8)]

    (aset ph 0 (long fnv64-offset)) ; Seed
    (reduce
      (fn [^long idx e]

        (when-not (or (keyword? e) (string? e))
          (truss/ex-info! "`shared-dict` entries must all be keywords or strings"
            {:idx idx, :value e, :type (type e)}))

        (when (if (keyword? e) (.containsKey kwm e) (.containsKey strm e))
          (truss/ex-info! "`shared-dict` entries must be distinct"
            {:idx idx, :duplicate e}))

        (when-not (.canEncode enc8 ^String (str e))
          ;; Unpaired surrogates lossily encode to `?` (in fingerprints AND in
          ;; Nippy's kw/str wire formats themselves), conflating distinct entries
          (truss/ex-info! "`shared-dict` entries must be UTF-8 encodable (no unpaired surrogates)"
            {:idx idx, :value e}))

        (aset earr idx e)
        (if (keyword? e)
          (.put kwm  e (int idx))
          (.put strm e (int idx)))

        (aset ph (inc idx)
          (unchecked-add
            (unchecked-multiply (aget ph idx) fnv64-prime)
            (entry-hash e)))

        (inc idx))
      0 entries)

    (SharedDict. earr kwm strm
      (reduce (fn [^long m ^String s] (max m (.length s))) -1 (.keySet strm))
      ph)))

(enc/declare-remote ^:dynamic taoensso.nippy/*shared-dict*)

(deftype CacheState
  ;; Keywords use `IdentityHashMap`s: keywords are interned and don't override
  ;; `equals`, so identity IS equality for them - and a flat open-addressed
  ;; table is much faster than `HashMap`'s scattered per-entry `Node`s.
  ;; NB unsafe for the arbitrary user values held by `freeze-idxs`.
  [^java.util.HashMap         freeze-idxs ; {[<val> <meta>] <idx>} for freezing `cache`d vals
   ^java.util.IdentityHashMap kw-idxs     ; {<kw> <idx>} for freezing cached kws
   ^java.util.ArrayList       thaw-vals   ; [<val> ...] for thawing, indexed by idx
   ^java.util.IdentityHashMap seen-kws    ; #{<kw>} seen this session, Ref. `write-auto-cached-kw`
   ^java.util.ArrayList       seen-log    ; `seen-kws` in insertion order, for `cache-restore!`
   ^SharedDict                 dict        ; ?SharedDict fixed at session creation, Ref. `shared-dict`
   ^longs                     mut])       ; [<freeze-marker-written?> <thaw-dict-k>]
                                          ;   (mutable long holder: deftype mutable fields are private)

(defn- current-shared-dict
  "Returns ?SharedDict from `taoensso.nippy/*shared-dict*`, validating type."
  ^SharedDict []
  (when-let [d taoensso.nippy/*shared-dict*]
    (if (instance? SharedDict d)
      d
      (truss/ex-info! "Expected `:shared-dict` value to be built with `nippy/shared-dict`"
        {:value d, :type (type d)}))))

(defn new-cache-state ^CacheState []
  (CacheState. (java.util.HashMap.) (java.util.IdentityHashMap.) (java.util.ArrayList.)
    (java.util.IdentityHashMap.) (java.util.ArrayList.)
    (current-shared-dict) (long-array 2)))

(defn dict-count
  "Returns given `CacheState`'s full dict size (0 if no dict)."
  ^long [^CacheState state]
  (if-let [^SharedDict d (.-dict state)]
    (alength ^objects (.-entries d))
    0))

(defn active-dict-count
  "Returns given `CacheState`'s active dict size (0 before its first hit):
  the current freeze-side idx offset for session cache entries."
  ^long [^CacheState state]
  (if (zero? (aget ^longs (.-mut state) 0)) 0 (dict-count state)))

(defn cache-idx-count
  "Returns the number of cache idxs allocated so far by given `CacheState`.

  Idxs come from a SINGLE monotonic sequence shared by both freeze maps,
  since the thaw side assigns them by arrival order. NB both maps store
  SESSION-RELATIVE idxs (`cache-restore!` depends on this); any dict offset
  is added at ref-emission time only."
  ^long [^CacheState state]
  (+ (.size ^java.util.HashMap         (.-freeze-idxs state))
     (.size ^java.util.IdentityHashMap (.-kw-idxs     state))))

(def ^ThreadLocal tl:cache
  "?CacheState for current freeze/thaw session."
  (enc/threadlocal))

(defmacro ^:public with-cache
  "Executes body with support for freezing/thawing cached values.

  This is a low-level util: you won't need to use this yourself unless
  you're using `freeze-to-out!` or `thaw-from-in!` (also low-level utils).

  ALL values frozen within a single `with-cache` body MUST be thawed
  together within a single corresponding `with-cache` body:

    (with-cache (freeze-to-out! dout x1) (freeze-to-out! dout x2))
    (with-cache [(thaw-from-in! din) (thaw-from-in! din)])

  See also `cache`."
  [& body]
  `(let [prev# (.get tl:cache)] ; ?CacheState of enclosing `with-cache`
     (try
       (.set tl:cache (new-cache-state))
       (do ~@body)
       (finally
         ;; Restore (NOT just remove) so that an enclosing `with-cache`
         ;; retains its cache. Removing here would cause the OUTER thaw to
         ;; throw "Can't thaw without cache available" on its next cached ref.
         (if (nil? prev#)
           (.remove tl:cache)
           (.set    tl:cache prev#))))))

(defn cache-mark
  "Returns a checkpoint of given `CacheState`'s write state (`freeze-idxs`
  + `seen-kws` + dict marker flag), for `cache-restore!`. O(1): exploits the
  invariant that entries are only ever added (with idx = current size),
  packing the two sizes (each <= 32768) + flag into a single long."
  ^long [^CacheState state]
  (bit-or
    (bit-shift-left (aget ^longs (.-mut state) 0) 41)
    (bit-shift-left (cache-idx-count state) 20)
    (do (long (.size ^java.util.ArrayList (.-seen-log state))))))

(defn cache-restore!
  "Restores given `CacheState`'s write state to given `cache-mark`
  checkpoint, returns nil. Necessary before reattempting or abandoning a
  write: `freeze-idxs` entries added during a failed/aborted attempt would
  otherwise later yield cache refs to values whose bytes were never
  retained, and stale `seen-kws` entries would make byte output of retried
  writes differ from non-retried writes of equal data."
  [^CacheState state ^long mark]
  (let [idx-mark (bit-and (bit-shift-right mark 20) 0xFFFFF)
        log-mark (bit-and                  mark     0xFFFFF)
        ^java.util.HashMap         fm (.-freeze-idxs state)
        ^java.util.IdentityHashMap km (.-kw-idxs     state)]

    (aset ^longs (.-mut state) 0 (bit-shift-right mark 41)) ; Dict marker flag

    (when (> (cache-idx-count state) idx-mark)
      (if (zero? idx-mark)
        (do (.clear fm) (.clear km))
        ;; NB compares each entry's STORED idx against the count checkpoint,
        ;; correct only because both maps store session-relative idxs
        ;; (idx = `cache-idx-count` at insertion time), Ref. `cache-idx-count`
        (let [pred (reify java.util.function.Predicate
                     (test [_ idx] (>= (long idx) idx-mark)))]
          (.removeIf (.values fm) pred)
          (.removeIf (.values km) pred))))

    (let [^java.util.ArrayList log (.-seen-log state)]
      (when (> (.size log) log-mark)
        (let [^java.util.IdentityHashMap seen (.-seen-kws state)
              tail (.subList log (int log-mark) (.size log))]
          (doseq [kw tail] (.remove seen kw))
          (.clear tail)))) ; Truncates backing list
    nil))

;;

(defn thaw-mark
  "Returns a checkpoint of given `CacheState`'s read state (`thaw-vals` +
  thaw dict count), for `thaw-restore!`. O(1)."
  ^long [^CacheState state]
  (bit-or
    (bit-shift-left (aget ^longs (.-mut state) 1) 32)
    (do (long (.size ^java.util.ArrayList (.-thaw-vals state))))))

(defn thaw-restore!
  "Restores given `CacheState`'s read state to given `thaw-mark`
  checkpoint, returns nil. Necessary when abandoning a failed read in a
  shared session: entries (including reserved slots) from the failed read
  would otherwise poison later reads in the same session, and a dict
  enabled by a failed read's marker would silently misresolve later
  dict-less reads."
  [^CacheState state ^long mark]
  (let [size-mark (bit-and mark 0xFFFFFFFF)
        ^java.util.ArrayList l (.-thaw-vals state)
        n (.size l)]
    (aset ^longs (.-mut state) 1 (bit-shift-right mark 32)) ; Thaw dict count
    (when (> n size-mark)
      (.clear (.subList l (int size-mark) n))) ; Truncates backing list
    nil))

;;;;

(def ^:const range-ubyte  (-    Byte/MAX_VALUE    Byte/MIN_VALUE))
(def ^:const range-ushort (-   Short/MAX_VALUE   Short/MIN_VALUE))
(def ^:const range-uint   (- Integer/MAX_VALUE Integer/MIN_VALUE))

(defmacro sm-ucount? [n] `(<= ~n     range-ubyte)) ; Unsigned
(defmacro sm-count?  [n] `(<= ~n  Byte/MAX_VALUE))
(defmacro md-count?  [n] `(<= ~n Short/MAX_VALUE))

(def ^:const meta-protocol-key :taoensso.nippy/meta-protocol-key)

;; Marker protocols would be clearer but are unfortunately slower due to CLJ-1814
(defprotocol     INativeFreezable        (native-freezable? [_] "Returns truthy iff given arg's type has a native Nippy freeze implementation."))
(defprotocol     ICustomFreezable        (custom-freezable? [_] "Returns truthy iff given arg's type has a custom Nippy freeze implementation."))
(extend-protocol ICustomFreezable Object (custom-freezable? [_] false))

(defmacro editable? [coll] `(instance? clojure.lang.IEditableCollection ~coll))
(defn xform* [xform] (truss/catching-xform {:error/msg "Error thrown via `*thaw-xform*`"} xform))

(def get-basis-fields
  "Returns [`java.lang.reflect.Field` ...] for given class."
  (enc/fmemoize
    (fn [^Class c] ; Auto invalidated on `deftype` redef, etc.
      (let [basis (.invoke (.getMethod c "getBasis" nil) nil nil)]
        (mapv
          (fn [f]
            (let [field (.getDeclaredField c (munge (name f)))]
              (.setAccessible field true)
              (do             field)))
          basis)))))

(comment
  (do (deftype T1 [x])                          (let [t1 (T1. :x)] (get-basis-fields (class t1))))
  (do (deftype T2 [^:unsynchronized-mutable x]) (let [t2 (T2. :x)] (get-basis-fields (class t2)))))

(defn try-pr-edn [x]
  (try
    (enc/pr-edn x)
    (catch Throwable _
      (try
        (str x)
        (catch Throwable _
          :nippy/unprintable)))))

(defn wrap-unfreezable [x]
  {:nippy/unfreezable
   {:type    (type       x)
    :content (try-pr-edn x)}})

(defn assert-custom-type-id [custom-type-id]
  (assert (or      (keyword? custom-type-id)
              (and (integer? custom-type-id) (<= 1 custom-type-id 128)))))

(defn coerce-custom-type-id
  "* +ive byte id ->  -ive byte id (for unprefixed custom types)
   *   Keyword id -> Short hash id (for   prefixed custom types)"
  [custom-type-id]
  (assert-custom-type-id      custom-type-id)
  (if-not           (keyword? custom-type-id)
    (int             (- ^long custom-type-id))
    (let [^int hash-id  (hash custom-type-id)
          short-hash-id
          (if (pos? hash-id)
            (mod hash-id Short/MAX_VALUE)
            (mod hash-id Short/MIN_VALUE))]

      ;; Make sure hash ids can't collide with byte ids (unlikely anyway):
      (assert (not (<= Byte/MIN_VALUE short-hash-id -1))
        "Custom type id hash collision; please choose a different id")

      (int short-hash-id))))

(comment
  (coerce-custom-type-id 77)
  (coerce-custom-type-id :foo/bar))

(defn read-edn [edn]
  (try
    (enc/read-edn {:readers *data-readers*} edn)
    (catch Exception e
      {:nippy/unthawable
       {:type  :reader
        :cause :exception

        :content   edn
        :exception e}})))

(let [pending (Object.)]

  ;; Idxs are dense and assigned in arrival order, so an `ArrayList` with
  ;; positional access serves here - avoiding the `Long` boxing + hashing
  ;; that a `HashMap` would need per cached ref.
  ;;
  ;; NB `idx` comes from untrusted input, so it's bounds-checked against the
  ;; list's CURRENT size before any access, and the list is only ever grown
  ;; one entry at a time (never pre-sized from a wire-supplied value).

  (defn read-cached [read-typed idx input-arg]
    (if-let [^CacheState state (.get tl:cache)]
      (let [^java.util.ArrayList l (.-thaw-vals state)
            idx (long idx) ; Normalize (callers give Long | Integer)
            k   (aget ^longs (.-mut state) 1) ; Thaw dict count, 0 unless a validated dict marker was read
            rel (- idx k) ; Session-relative idx
            n   (.size l)]

        (enc/cond
          (and (< idx k) (>= idx 0)) ; Ref to a shared dict entry
          (aget ^objects (.-entries ^SharedDict (.-dict state)) (int idx))

          (and (< rel n) (>= rel 0)) ; Ref to a value read earlier this session
          (let [v (.get l (int rel))]
            (if (identical? v pending)
              (truss/ex-info! "Bad cache ref: cyclic or corrupt data?" {:idx idx})
              v))

          (== rel n) ; First occurrence
          ;; Reserve idx BEFORE reading: value may itself contain nested
          ;; first occurrences (which the writer idxs AFTER this one)
          (do
            (.add l pending)
            (let [x (read-typed input-arg)]
              (.set l (int rel) x)
              x))

          ;; Legit first occurrences always arrive in idx order, so this
          ;; is corrupt data or (most likely) a cache ref to an earlier
          ;; write outside the current `with-cache` session
          :else
          (truss/ex-info! "Bad cache ref: earlier `with-cache` session or corrupt data?"
            {:idx idx, :cached-count n, :dict-count k})))

      (truss/ex-info! "Can't thaw without cache available. See `with-cache`." {})))

  (defn read-shared-dict-marker!
    "Validates a shared-dict marker (dict count + fingerprint) against the
    current session's dict and enables dict refs for the session, returns
    nil. Refs into the dict idx range are honored ONLY after this validation,
    so a mismatched or missing dict always fails loud. See `shared-dict`."
    [wire-count ^long wire-hash]
    (if-let [^CacheState state (.get tl:cache)]
      (let [^SharedDict dict (.-dict state)
            ^longs     mut  (.-mut  state)
            wire-count (long wire-count)]

        (when (nil? dict)
          (truss/ex-info! "Data was frozen with a `:shared-dict`, so thawing needs the same dict"
            {:frozen {:dict-count wire-count}}))

        ;; A marker activates the dict at its first hit, possibly after
        ;; ordinary session cache entries already exist. A second marker
        ;; indicates concatenated/mixed session output or corrupt data.
        (when-not (zero? (aget mut 1))
          (truss/ex-info! "Unexpected duplicate `:shared-dict` marker: earlier `with-cache` session or corrupt data?"
            {:frozen {:dict-count wire-count}}))

        (let [^longs ph (.-prefix-hashes dict)
              n (alength ^objects (.-entries dict))]
          (when-not (and (<= 1 wire-count n) (== (aget ph (int wire-count)) wire-hash))
            (truss/ex-info! "`:shared-dict` mismatch: data was frozen with a different dict (never remove or reorder dict entries!)"
              {:frozen {:dict-count wire-count}, :given {:dict-count n}}))

          (aset mut 1 wire-count)
          nil))

      (truss/ex-info! "Can't thaw without cache available. See `with-cache`." {}))))

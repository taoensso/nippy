(ns taoensso.min-encore
  (:require [clojure.tools.reader.edn :as tools-edn]
            [clojure.string :as str])
  (:import [java.util.function Function BiFunction])
  (:refer-clojure :exclude
   [defonce if-let if-not if-some when when-not when-some when-let cond]))


(set! *warn-on-reflection* true)

(def -core-merge     clojure.core/merge)
(def -core-update-in clojure.core/update-in)


(defmacro defalias "Defines an alias for a var, preserving its metadata."
  ([    src      ] `(defalias ~(symbol (name src)) ~src nil))
  ([sym src      ] `(defalias ~sym                 ~src nil))
  ([sym src attrs]
   (let [attrs (if (string? attrs) {:doc attrs} attrs)] ; Back compatibility
     `(let [src-var# (var ~src)
            dst-var# (def ~sym (.getRawRoot src-var#))]
        (alter-meta! dst-var#
                     #(-core-merge %
                                   (dissoc (meta src-var#) :column :line :file :ns :test :name)
                                   ~attrs))
        dst-var#))))


(defn name-with-attrs
  "Given a symbol and args, returns [<name-with-attrs-meta> <args>] with
  support for `defn` style `?docstring` and `?attrs-map`."
  ([sym args            ] (name-with-attrs sym args nil))
  ([sym args attrs-merge]
   (let [[?docstring args] (if (and (string? (first args)) (next args)) [(first args) (next args)] [nil args])
         [attrs      args] (if (and (map?    (first args)) (next args)) [(first args) (next args)] [{}  args])
         attrs (if ?docstring (assoc attrs :doc ?docstring) attrs)
         attrs (if (meta sym) (conj (meta sym) attrs) attrs)
         attrs (conj attrs attrs-merge)]
     [(with-meta sym attrs) args])))


(defmacro defonce
  "Like `core/defonce` but supports optional docstring and attrs map."
  {:style/indent 1}
  [sym & args]
  (let [[sym body] (name-with-attrs sym args)]
    `(clojure.core/defonce ~sym ~@body)))


(defmacro if-let
  "Like `core/if-let` but can bind multiple values for `then` iff all tests
  are truthy, supports internal unconditional `:let`s."
  {:style/indent 1}
  ([bindings then     ] `(if-let ~bindings ~then nil))
  ([bindings then else]
   (let [s (seq bindings)]
     (if s ; (if-let [] true false) => true
       (let [[b1 b2 & bnext] s]
         (if (= b1 :let)
           `(let      ~b2  (if-let ~(vec bnext) ~then ~else))
           `(let [b2# ~b2]
              (if b2#
                (let [~b1 b2#]
                  (if-let ~(vec bnext) ~then ~else))
                ~else))))
       then))))

(defmacro if-some
  "Like `core/if-some` but can bind multiple values for `then` iff all tests
  are non-nil, supports internal unconditional `:let`s."
  {:style/indent 1}
  ([bindings then] `(if-some ~bindings ~then nil))
  ([bindings then else]
   (let [s (seq bindings)]
     (if s ; (if-some [] true false) => true
       (let [[b1 b2 & bnext] s]
         (if (= b1 :let)
           `(let      ~b2  (if-some ~(vec bnext) ~then ~else))
           `(let [b2# ~b2]
              (if (nil? b2#)
                ~else
                (let [~b1 b2#]
                  (if-some ~(vec bnext) ~then ~else))))))
       then))))

(defmacro if-not
  "Like `core/if-not` but acts like `if-let` when given a binding vector
  as test expr."
  ;; Also avoids unnecessary `(not test)`
  {:style/indent 1}
  ([test-or-bindings then]
   (if (vector? test-or-bindings)
     `(if-let ~test-or-bindings nil ~then)
     `(if     ~test-or-bindings nil ~then)))

  ([test-or-bindings then else]
   (if (vector? test-or-bindings)
     `(if-let ~test-or-bindings ~else ~then)
     `(if     ~test-or-bindings ~else ~then))))

(defmacro when
  "Like `core/when` but acts like `when-let` when given a binding vector
  as test expr."
  {:style/indent 1}
  [test-or-bindings & body]
  (if (vector? test-or-bindings)
    `(if-let ~test-or-bindings (do ~@body) nil)
    `(if     ~test-or-bindings (do ~@body) nil)))

(defmacro when-not
  "Like `core/when-not` but acts like `when-let` when given a binding vector
  as test expr."
  {:style/indent 1}
  [test-or-bindings & body]
  (if (vector? test-or-bindings)
    `(if-let ~test-or-bindings nil (do ~@body))
    `(if     ~test-or-bindings nil (do ~@body))))

(defmacro when-some
  {:style/indent 1}
  [test-or-bindings & body]
  (if (vector? test-or-bindings)
    `(if-some       ~test-or-bindings  (do ~@body) nil)
    `(if      (nil? ~test-or-bindings) nil (do ~@body))))

(defmacro when-let
  "Like `core/when-let` but can bind multiple values for `body` iff all tests
  are truthy, supports internal unconditional `:let`s."
  {:style/indent 1}
  ;; Now a feature subset of all-case `when`
  [bindings & body] `(if-let ~bindings (do ~@body)))


(defmacro cond
  "Like `core/cond` but supports implicit final `else` clause, and special
  clause keywords for advanced behaviour:

  (cond
    :let [x \"x\"] ; Establish bindings visible to following forms

    :do (println (str \"x value: \" x)) ; Eval expr for side effects

    :if-let [y \"y\"
             z nil]
    \"y and z were both truthy\"

    :if-some [y \"y\"
              z nil]
    \"y and z were both non-nil\")

  :let support inspired by https://github.com/Engelberg/better-cond.
  Simple, flexible way to eliminate deeply-nested control flow code."

  ;; Also avoids unnecessary `(if :else ...)`, etc.
  [& clauses]
  (when-let [[test expr & more] (seq clauses)]
    (if-not (next clauses)
      test ; Implicit else
      (case test
        (true :else :default)       expr                   ; Faster than (if <truthy> ...)
        (false nil)                         `(cond ~@more) ; Faster than (if <falsey> ...)
        :do          `(do          ~expr     (cond ~@more))
        :let         `(let         ~expr     (cond ~@more))
        :when        `(when        ~expr     (cond ~@more)) ; Undocumented
        :when-not    `(when-not    ~expr     (cond ~@more)) ; Undocumented
        :when-some   `(when-some   ~expr     (cond ~@more)) ; Undocumented
        :return-when `(if-let  [x# ~expr] x# (cond ~@more)) ; Undocumented
        :return-some `(if-some [x# ~expr] x# (cond ~@more)) ; Undocumented
        :if-let      `(if-let      ~expr ~(first more) (cond ~@(next more)))
        :if-some     `(if-some     ~expr ~(first more) (cond ~@(next more)))
        :if-not      `(if-not      ~expr ~(first more) (cond ~@(next more))) ; Undocumented

        (if (keyword? test)
          (throw ; Undocumented, but throws at compile-time so easy to catch
            (ex-info "Unrecognized `encore/cond` keyword in `test` clause"
              {:test-form test :expr-form expr}))

          (if (vector? test) ; Undocumented
            `(if-let ~test ~expr (cond ~@more))

            ;; Experimental, assumes `not` = `core/not`:
            (if (and (list? test) (= (first test) 'not))
              `(if ~(second test) (cond ~@more) ~expr)
              `(if ~test ~expr    (cond ~@more)))))))))


(defmacro cond!
  "Like `cond` but throws on non-match like `case` and `condp`."
  [& clauses]
  (if (odd? (count clauses))
    `(cond ~@clauses) ; Has implicit else clause
    `(cond ~@clauses :else (throw (ex-info "No matching `encore/cond!` clause" {})))))


(defmacro case-eval
  "Like `case` but evals test constants for their compile-time value."
  {:style/indent 1}
  [expr & clauses]
  (let [default (when (odd? (count clauses)) (last clauses))
        clauses (if default (butlast clauses) clauses)]
    `(case ~expr
       ~@(map-indexed (fn [i# form#] (if (even? i#) (eval form#) form#)) clauses)
       ~(when default default))))


(defn read-edn
  "Attempts to pave over differences in:
    `clojure.edn/read-string`, `clojure.tools.edn/read-string`,
    `cljs.reader/read-string`, `cljs.tools.reader/read-string`.
   `cljs.reader` in particular can be a pain."

  ([     s] (read-edn nil s))
  ([opts s]
   ;; First normalize behaviour for unexpected inputs:
   (if (or (nil? s) (identical? s ""))
     nil
     (if-not (string? s)
       (throw (ex-info "`read-edn` attempt against non-nil, non-string arg"
                {:given s :type (type s)}))

       (let [readers (get opts :readers ::dynamic)
             default (get opts :default ::dynamic)

             ;; Nb we ignore as implementation[1] detail:
             ;;  *.tools.reader/*data-readers*,
             ;;  *.tools.reader/default-data-reader-fn*
             ;;
             ;; [1] Lib consumer doesn't care that we've standardized to
             ;;     using tools.reader under the covers

             readers
             (if-not (identical? readers ::dynamic)
               readers
               clojure.core/*data-readers*)

             default
             (if-not (identical? default ::dynamic)
               default
               clojure.core/*default-data-reader-fn*)

             opts (assoc opts :readers readers :default default)]
         (tools-edn/read-string opts s))))))


(defn pr-edn
  "Prints arg to an edn string readable with `read-edn`."
  ([      x] (pr-edn nil x))
  ([_opts x]
   (let [sw (java.io.StringWriter.)]
     (binding [*print-level* nil, *print-length* nil,
               ;; *out* sw, *print-dup* false
               ]
       ;; (pr x)
       (print-method x sw) ; Bypass *out*, *print-dup*
       (.toString sw)))))


(defn get-sys-val ([id] (get-sys-val  id id)) ([prop-id env-id] (or (System/getProperty prop-id) (System/getenv env-id))))
(defn read-sys-val ([id] (read-sys-val id id)) ([prop-id env-id] (when-let [s (get-sys-val prop-id env-id)] (read-edn s))))


(defn memoize_
  "Like `core/memoize` but faster, non-racy, and supports invalidation."
  [f]
  (let [nil-sentinel (Object.)
        ;;Concurrent hash maps have per-bucket locking if you use the compute* operators.
        cache_ (java.util.concurrent.ConcurrentHashMap.)]

    (fn
      ([ ] (.computeIfAbsent
            cache_ nil-sentinel
            (reify Function
              (apply [this k]
                (f)))))

      ([& xs]
       (let [x1 (first xs)]

         (cond
           (= x1 :mem/del)
           (let [xn (next  xs)
                 x2 (first xn)]
             (if (= x2 :mem/all)
               (.clear  cache_)
               (.remove cache_ (or xn nil-sentinel)))
             nil)

           (= x1 :mem/fresh)
           (let [xn (next xs)]
             (.compute cache_ (or xn nil-sentinel)
                       (reify BiFunction
                         (apply [this k v]
                           (apply f xn)))))

           :else
           (.computeIfAbsent
            cache_ xs
            (reify Function
              (apply [this k]
                (apply f xs))))))))))


(defn fmemoize
  "Fastest-possible Clj memoize. Non-racy, 0-3 arity only.
  Cljs just passes through to `core/memoize`."
  [f]
  ;; Non-racey just as fast as racey, and protects against nils in maps
  (let [cache0_ (java.util.concurrent.atomic.AtomicReference. nil)
        ;;Concurrent hash maps have per-bucket locking if you use the compute* operators.
        cache1_ (java.util.concurrent.ConcurrentHashMap.)
        cachen_ (java.util.concurrent.ConcurrentHashMap.)]

    (fn
      ([ ]
       @(or
         (.get cache0_)
         (let [dv (delay (f))]
           (if (.compareAndSet cache0_ nil dv)
             dv
             (.get cache0_)))))

      ([x]
       (.computeIfAbsent cache1_ x
                         (reify Function
                           (apply [this v]
                             (f v)))))
      ([x1 x2]
       (let [xs [x1 x2]]
         (.computeIfAbsent cachen_ xs
                           (reify Function
                             (apply [this v]
                               (f x1 x2))))))
      ([x1 x2 x3]
       (let [xs [x1 x2 x3]]
         (.computeIfAbsent cachen_ xs
                           (reify Function
                             (apply [this v]
                               (f x1 x2 x3)))))))))


(def ^:private ^:const atom-tag 'clojure.lang.IAtom)


(defmacro -if-cas! "Micro optimization, mostly for cljs."
  [atom_ old-val new-val then & [?else]]
  `(if (.compareAndSet ~(with-meta atom_ {:tag atom-tag}) ~old-val ~new-val)
     ~then
     ~?else))


(defn -swap-val!
  "Used internally by memoization utils."
  [atom_ k f]
  (loop []
    (let [m0 @atom_
          v1 (f (get m0 k))
          m1 (assoc  m0 k v1)]
      (-if-cas! atom_ m0 m1
        v1
        (recur)))))



(defmacro thread-local-proxy
  [& body] `(proxy [ThreadLocal] [] (initialValue [] (do ~@body))))

(def ^:private srng* (thread-local-proxy
                      (or
                       ;; Very strong and blocking.  See
                       ;; https://stackoverflow.com/questions/137212/how-to-deal-with-a-slow-securerandom-generator
                       (java.security.SecureRandom/getInstanceStrong)
                       ;;Weaker and much faster
                       #_(java.security.SecureRandom/getInstance "SHA1PRNG"))))

(defn secure-rng
   "Returns a thread-local `java.security.SecureRandom`.
     Favours security over performance. Automatically re-seeds occasionally.
     May block while waiting on system entropy!"
   ^java.security.SecureRandom []
   (let [rng ^java.security.SecureRandom (.get ^ThreadLocal srng*)]
     ;; Occasionally supplement current seed for extra security.
     ;; Otherwise an attacker could *theoretically* observe large amounts of
     ;; srng output to determine initial seed, Ref. https://goo.gl/MPM91w
     (when (< (.nextDouble rng) 2.44140625E-4) (.setSeed rng (.generateSeed rng 8)))
     rng))


(defn ba-concat ^bytes [^bytes ba1 ^bytes ba2]
  (let [s1  (alength ba1)
        s2  (alength ba2)
        out (byte-array (+ s1 s2))]
    (System/arraycopy ba1 0 out 0  s1)
    (System/arraycopy ba2 0 out s1 s2)
    out))


(defn ba-split [^bytes ba ^long idx]
  (if (zero? idx)
    [nil ba]
    (let [s (alength ba)]
      (when (> s idx)
        [(java.util.Arrays/copyOf      ba idx)
         (java.util.Arrays/copyOfRange ba idx s)]))))

(defn ba= [^bytes x ^bytes y] (java.util.Arrays/equals x y))


(defn rsome      [pred coll] (reduce    (fn [acc in]  (when-let [p (pred in)]  (reduced p)))     nil coll))
(defn rsome-kv   [pred coll] (reduce-kv (fn [acc k v] (when-let [p (pred k v)] (reduced p)))     nil coll))
(defn rfirst     [pred coll] (reduce    (fn [acc in]  (when        (pred in)   (reduced in)))    nil coll))
(defn rfirst-kv  [pred coll] (reduce-kv (fn [acc k v] (when        (pred k v)  (reduced [k v]))) nil coll))
(defn revery?    [pred coll] (reduce    (fn [acc in]  (if (pred in)  true (reduced false))) true coll))
(defn revery-kv? [pred coll] (reduce-kv (fn [acc k v] (if (pred k v) true (reduced false))) true coll))
(defn revery     [pred coll] (reduce    (fn [acc in]  (if (pred in)  coll (reduced nil))) coll coll))
(defn revery-kv  [pred coll] (reduce-kv (fn [acc k v] (if (pred k v) coll (reduced nil))) coll coll))
(defn reduce-n
  ([rf init       end] (reduce rf init (range       end)))
  ([rf init start end] (reduce rf init (range start end))))

(defmacro new-object [] `(Object.))


(defn rcompare "Reverse comparator."
  {:inline (fn [x y] `(. clojure.lang.Util compare ~y ~x))}
  [x y] (compare y x))


(defn editable?   [x] (instance? clojure.lang.IEditableCollection  x))
(defn queue? [x] (instance? clojure.lang.PersistentQueue x))
(defn queue "Returns a PersistentQueue."
  ([coll] (into (queue) coll))
  ([] clojure.lang.PersistentQueue/EMPTY))


(defn repeatedly-into
  "Like `repeatedly` but faster and `conj`s items into given collection."
  [coll ^long n f]
  (if (and (> n 10) (editable? coll))
    (persistent! (reduce-n (fn [acc _] (conj! acc (f))) (transient coll) n))
    (do          (reduce-n (fn [acc _] (conj  acc (f)))            coll  n))))


(defn str-contains?
  [s substr]
  (.contains ^String s ^String substr))


(defn re-pattern? [x] (instance? java.util.regex.Pattern  x))


(let [sentinel (new-object)
      nil->sentinel (fn [x] (if (nil? x) sentinel x))
      sentinel->nil (fn [x] (if (identical? x sentinel) nil x))]

  (defn reduce-top
    "Reduces the top `n` items from `coll` of N items into in O(N.logn) time.
    For comparsion, (take n (sort-by ...)) is O(N.logN)."
    ([n           rf init coll] (reduce-top n identity compare rf init coll))
    ([n keyfn     rf init coll] (reduce-top n keyfn    compare rf init coll))
    ([n keyfn cmp rf init coll]
     (let [coll-size (count coll)
           n (long (min coll-size (long n)))]

       (if-not (pos? n)
         init
         (let [pq (java.util.PriorityQueue. coll-size
                                            (fn [x y] (cmp (keyfn (sentinel->nil x))
                                                           (keyfn (sentinel->nil y)))))]

           (run! #(.offer pq (nil->sentinel %)) coll)
           (reduce-n (fn [acc _] (rf acc (sentinel->nil (.poll pq))))
                     init n)))))))
(let [always (fn always [?in-str] true)
      never  (fn never  [?in-str] false)

      wild-str->?re-pattern
      (fn [s]
        (when (str-contains? s "*")
          (re-pattern
            (-> (str "^" s "$")
              (str/replace "." "\\.")
              (str/replace "*" "(.*)")))))

      compile
      (fn compile [spec cache?] ; Returns (fn match? [in-str])
        (cond
          (#{:any "*"    } spec) always
          (#{:none #{} []} spec) never
          (re-pattern?     spec) (fn [in-str] (re-find spec in-str))
          (string?         spec)
          (cond
            ;; Ambiguous: "," meant as splitter or literal? Prefer coll.
            ;; (str-contains? spec ",") (recur (mapv str/trim (str/split spec #",")) cache?)
            :if-let [re-pattern (wild-str->?re-pattern spec)]

            (recur re-pattern cache?)
            :else (fn [in-str] (= in-str spec)))

          (or (vector? spec) (set? spec))
          (cond
            ;; (empty? spec)   never
            ((set spec) "*")   always
            (= (count spec) 1) (recur (first spec) cache?)
            :else
            (let [[fixed-strs re-patterns]
                  (reduce
                    (fn [[fixed-strs re-patterns] spec]
                      (if-let [re-pattern (if (re-pattern? spec) spec (wild-str->?re-pattern spec))]
                        [      fixed-strs       (conj re-patterns re-pattern)]
                        [(conj fixed-strs spec)       re-patterns            ]))
                    [#{} []]
                    spec)

                  fx-match (not-empty fixed-strs) ; #{"foo" "bar"}, etc.
                  re-match
                  (when-let [re-patterns (not-empty re-patterns)] ; ["foo.*", "bar.*"], etc.
                    (let [f (fn [in-str] (rsome #(re-find % in-str) re-patterns))]
                      (if cache? (fmemoize f) f)))]

              (cond!
                (and fx-match re-match) (fn [in-str] (or (fx-match in-str) (re-match in-str)))
                fx-match fx-match
                re-match re-match)))

          :else
          (throw
            (ex-info "Unexpected compile spec type"
              {:given spec :type (type spec)}))))]

  (defn compile-str-filter
    "Compiles given spec and returns a fast (fn conform? [?in-str]).

    Spec may be:
      - A regex pattern. Will conform on match.
      - A string, in which any \"*\"s will act as wildcards (#\".*\").
        Will conform on match.

      - A vector or set of regex patterns or strings.
        Will conform on ANY match.
        If you need literal \"*\"s, use an explicit regex pattern instead.

      - {:allow <allow-spec> :deny <deny-spec> :cache? <bool>}.
        Will conform iff allow-spec matches AND deny-spec does NOT.

    Input may be: namespace strings, class names, etc.
    Useful as string allowlist (whitelist) and/or denylist (blacklist).

    Spec examples:
      #{}, \"*\", \"foo.bar\", \"foo.bar.*\", #{\"foo\" \"bar.*\"},
      {:allow #{\"foo\" \"bar.*\"} :deny #{\"foo.*.bar.*\"}}"

    [spec]
    (if-not (map? spec)
      (recur {:allow spec :deny nil})
      (let [cache?         (get spec :cache?)
            allow-spec (or (get spec :allow) (get spec :whitelist))
            deny-spec  (or (get spec :deny)  (get spec :blacklist))

            allow (when-let [as allow-spec] (compile as cache?))
            deny  (when-let [ds deny-spec]  (compile ds cache?))]

        (cond
          (= deny  always) never
          (= allow never)  never

          (and allow deny)
          (fn [?in-str]
            (let [in-str (str ?in-str)]
              (if (allow in-str)
                (if (deny in-str)
                  false
                  true)
                false)))

          allow (if (= allow always) always (fn [?in-str] (if (allow (str ?in-str)) true false)))
          deny  (if (= deny  never)  always (fn [?in-str] (if (deny  (str ?in-str)) true false)))
          :else
          (throw
            (ex-info "compile-str-filter: `allow-spec` and `deny-spec` cannot both be nil"
                     {:allow-spec allow-spec :deny-spec deny-spec})))))))


(defn round0   ^long [n]            (Math/round    (double n)))
(defn round1 ^double [n] (/ (double (Math/round (* (double n)  10.0)))  10.0))
(defn round2 ^double [n] (/ (double (Math/round (* (double n) 100.0))) 100.0))
(defn perc     ^long [n divisor] (Math/round (* (/ (double n) (double divisor)) 100.0)))


(defmacro now-nano* [] `(System/nanoTime))


(defmacro time-ns "Returns number of nanoseconds it took to execute body."
  [& body] `(let [t0# (now-nano*)] ~@body (- (now-nano*) t0#)))


(defn bench*
   "Repeatedly executes fn and returns time taken to complete execution."
   [nlaps {:keys [nlaps-warmup nthreads as-ns?]
           :or   {nlaps-warmup 0
                  nthreads     1}} f]
  (try
    (dotimes [_ nlaps-warmup] (f))
    (let [nanosecs
          (if (= nthreads 1)
            (time-ns (dotimes [_ nlaps] (f)))
            (let [nlaps-per-thread (/ nlaps nthreads)]
              (time-ns
               (let [futures (repeatedly-into [] nthreads
                                              (fn [] (future (dotimes [_ nlaps-per-thread] (f)))))]
                 (mapv deref futures)))))]
      (if as-ns? nanosecs (round0 (/ nanosecs 1e6))))
     (catch Throwable t
       (println (str "Bench failure: " (.getMessage t)))
       -1)))

(defmacro bench [nlaps opts & body] `(bench* ~nlaps ~opts (fn [] ~@body)))

(ns taoensso.nippy.utils
  {:author "Peter Taoussanis"}
  (:require [clojure.string :as str]))

(defmacro case-eval
  "Like `case` but evaluates test constants for their compile-time value."
  [e & clauses]
  (let [;; Don't evaluate default expression!
        default (when (odd? (count clauses)) (last clauses))
        clauses (if default (butlast clauses) clauses)]
    `(case ~e
       ~@(map-indexed (fn [i# form#] (if (even? i#) (eval form#) form#))
                      clauses)
       ~(when default default))))

(defmacro repeatedly-into
  "Like `repeatedly` but faster and `conj`s items into given collection."
  [coll n & body]
  `(let [coll# ~coll
         n# ~n]
     (if (instance? clojure.lang.IEditableCollection coll#)
       (loop [v# (transient coll#) idx# 0]
         (if (>= idx# n#)
           (persistent! v#)
           (recur (conj! v# ~@body)
                  (inc idx#))))
       (loop [v# coll#
              idx# 0]
         (if (>= idx# n#)
           v#
           (recur (conj v# ~@body)
                  (inc idx#)))))))

(defmacro time-ns "Returns number of nanoseconds it takes to execute body."
  [& body] `(let [t0# (System/nanoTime)] ~@body (- (System/nanoTime) t0#)))

(defmacro bench
  "Repeatedly executes form and returns time taken to complete execution."
  [num-laps form & {:keys [warmup-laps num-threads as-ns?]}]
  `(try (when ~warmup-laps (dotimes [_# ~warmup-laps] ~form))
        (let [nanosecs#
              (if-not ~num-threads
                (time-ns (dotimes [_# ~num-laps] ~form))
                (let [laps-per-thread# (int (/ ~num-laps ~num-threads))]
                  (time-ns
                   (->> (fn [] (future (dotimes [_# laps-per-thread#] ~form)))
                        (repeatedly ~num-threads)
                        doall
                        (map deref)
                        dorun))))]
          (if ~as-ns? nanosecs# (Math/round (/ nanosecs# 1000000.0))))
        (catch Exception e# (str "DNF: " (.getMessage e#)))))

(defn version-compare "Comparator for version strings like x.y.z, etc."
  [x y] (let [vals (fn [s] (vec (map #(Integer/parseInt %) (str/split s #"\."))))]
          (compare (vals x) (vals y))))

(defn version-sufficient? [version-str min-version-str]
  (try (>= (version-compare version-str min-version-str) 0)
       (catch Exception _ false)))

(defn memoized
  "Like `memoize` but takes an explicit cache atom (possibly nil) and
  immediately applies memoized f to given arguments."
  [cache f & args]
  (if-not cache
    (apply f args)
    (if-let [dv (@cache args)]
      @dv
      (let [dv (delay (apply f args))]
        (swap! cache assoc args dv)
        @dv))))

(comment (memoized nil +)
         (memoized nil + 5 12))

(defn ba= [^bytes x ^bytes y] (java.util.Arrays/equals x y))

(defn ba-concat ^bytes [^bytes ba1 ^bytes ba2]
  (let [s1  (alength ba1)
        s2  (alength ba2)
        out (byte-array (+ s1 s2))]
    (System/arraycopy ba1 0 out 0  s1)
    (System/arraycopy ba2 0 out s1 s2)
    out))

(defn ba-split [^bytes ba ^Integer idx]
  (let [s (alength ba)]
    (when (> s idx)
      [(java.util.Arrays/copyOf      ba idx)
       (java.util.Arrays/copyOfRange ba idx s)])))

(comment (String. (ba-concat (.getBytes "foo") (.getBytes "bar")))
         (let [[x y] (ba-split (.getBytes "foobar") 5)]
           [(String. x) (String. y)]))

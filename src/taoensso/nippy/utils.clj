(ns taoensso.nippy.utils
  {:author "Peter Taoussanis"}
  (:require [clojure.string :as str])
  (:import  org.iq80.snappy.Snappy))

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

(defn pairs
  "Like (partition 2 coll) but faster and returns lazy seq of vector pairs."
  [coll]
  (lazy-seq
   (when-let [s (seq coll)]
     (let [n (next s)]
       (cons [(first s) (first n)] (pairs (next n)))))))

(defmacro time-ns
  "Returns number of nanoseconds it takes to execute body."
  [& body]
  `(let [t0# (System/nanoTime)]
     ~@body
     (- (System/nanoTime) t0#)))

(defmacro bench
  "Repeatedly executes form and returns time taken to complete execution."
  [num-laps form & {:keys [warmup-laps num-threads as-ms?]
                :or   {as-ms? true}}]
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
          (if ~as-ms? (Math/round (/ nanosecs# 1000000.0)) nanosecs#))
        (catch Exception e# (str "DNF: " (.getMessage e#)))))

(defn version-compare
  "Comparator for version strings like x.y.z, etc."
  [x y]
  (let [vals (fn [s] (vec (map #(Integer/parseInt %) (str/split s #"\."))))]
    (compare (vals x) (vals y))))

(defn version-sufficient?
  [version-str min-version-str]
  (try (>= (version-compare version-str min-version-str) 0)
       (catch Exception _ false)))

;; TODO Unnecessarily complicated. Waiting on http://goo.gl/7mbR3 merge.
(defn compress-bytes [ba]
  (let [ba-size     (alength ^bytes ba)
        ba-out      (byte-array (Snappy/maxCompressedLength ba-size))
        ba-out-size (Snappy/compress ba (int 0) (int ba-size) ba-out (int 0))]
    (java.util.Arrays/copyOf ba-out ba-out-size)))

(defn uncompress-bytes [ba] (Snappy/uncompress ba 0 (alength ^bytes ba)))

(comment (String. (uncompress-bytes (compress-bytes (.getBytes "Test")))))
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
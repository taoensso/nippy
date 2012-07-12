(ns taoensso.nippy.benchmarks
  {:author "Peter Taoussanis"}
  (:use     [taoensso.nippy :as nippy :only (freeze-to-bytes thaw-from-bytes)])
  (:require [taoensso.nippy.utils :as utils]))

;; Remove stuff from stress-data that breaks reader
(def data (dissoc nippy/stress-data :queue :queue-empty :bytes))

(defmacro bench [& body] `(utils/bench 10000 (do ~@body) :warmup-laps 1000))

(defn reader-freeze [x] (binding [*print-dup* false] (pr-str x)))
(defn reader-thaw   [x] (binding [*read-eval* false] (read-string x)))

(def roundtrip        (comp thaw-from-bytes freeze-to-bytes))
(def reader-roundtrip (comp reader-thaw reader-freeze))

(comment

  ;;; Times
  (println
   "---\n"
   {:reader {:freeze (bench (reader-freeze bench-data))
             :thaw   (let [frozen (reader-freeze bench-data)]
                       (bench (reader-thaw frozen)))
             :round  (bench (reader-roundtrip bench-data))}

    :nippy  {:freeze (bench (freeze-to-bytes bench-data))
             :thaw   (let [frozen (freeze-to-bytes bench-data)]
                       (bench (thaw-from-bytes frozen)))
             :round  (bench (roundtrip bench-data))}})

  ;; Clojure 1.3.0, Nippy 0.9.0
  ;;  {:reader {:freeze 23573, :thaw 31923, :round 53253},
  ;;   :nippy  {:freeze 3805,  :thaw 3789,  :round 7522}}
  ;; (float (/ 53253 7522)) = 7.079633

  ;;; Data size
  (let [frozen (reader-freeze bench-data)]   (count (.getBytes frozen "UTF8")))
  (let [frozen (freeze-to-bytes bench-data)] (count frozen))
  ;; 22711, 12168
  )
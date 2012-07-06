(ns taoensso.nippy.benchmarks
  {:author "Peter Taoussanis"}
  (:use [taoensso.nippy :as nippy :only (freeze-to-bytes thaw-from-bytes)]))

;; Remove stuff from stress-data that breaks reader
(def bench-data (dissoc nippy/stress-data :queue :queue-empty :bytes))

(defn reader-freeze [x] (binding [*print-dup* false] (pr-str x)))
(defn reader-thaw   [x] (binding [*read-eval* false] (read-string x)))

(def roundtrip        (comp thaw-from-bytes freeze-to-bytes))
(def reader-roundtrip (comp reader-thaw reader-freeze))

(defmacro time-requests
  "Warms up, then executes given number of requests and returns total execution
  times in msecs."
  [num-requests & body]
  `(do (dotimes [_# (int (/ ~num-requests 4))] ~@body) ; Warm-up
       (let [start-time# (System/nanoTime)]
         (dotimes [_# ~num-requests] ~@body)
         (Math/round (/ (- (System/nanoTime) start-time#) 1000000.0)))))

(comment

  ;;; Times
  (println
   "---\n"
   (let [num 10000]
     {:reader {:freeze (time-requests num (reader-freeze bench-data))
               :thaw   (let [frozen (reader-freeze bench-data)]
                         (time-requests num (reader-thaw frozen)))
               :round  (time-requests num (reader-roundtrip bench-data))}

      :nippy  {:freeze (time-requests num (freeze-to-bytes bench-data))
               :thaw   (let [frozen (freeze-to-bytes bench-data)]
                         (time-requests num (thaw-from-bytes frozen)))
               :round  (time-requests num (roundtrip bench-data))}}))

  ;; Clojure 1.3.0, Nippy 0.9.0
  ;;  {:reader {:freeze 23573, :thaw 31923, :round 53253},
  ;;   :nippy  {:freeze 3805,  :thaw 3789,  :round 7522}}
  ;; (float (/ 53253 7522)) = 7.079633

  ;;; Data size
  (let [frozen (reader-freeze bench-data)]   (count (.getBytes frozen "UTF8")))
  (let [frozen (freeze-to-bytes bench-data)] (count frozen))
  ;; 22711, 12168
  )
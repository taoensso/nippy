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

(defn autobench [] (bench (roundtrip data)))

(comment

  ;;; Times
  (println
   "---\n"
   {:reader {:freeze (bench (reader-freeze data))
             :thaw   (let [frozen (reader-freeze data)]
                       (bench (reader-thaw frozen)))
             :round  (bench (reader-roundtrip data))}

    :nippy  {:freeze (bench (freeze-to-bytes data))
             :thaw   (let [frozen (freeze-to-bytes data)]
                       (bench (thaw-from-bytes frozen)))
             :round  (bench (roundtrip data))}})

  ;; Clojure 1.3.0, Nippy 0.9.2
  ;; {:reader {:freeze 28505, :thaw 36451, :round 59545},
  ;;  :nippy  {:freeze 3751,  :thaw 4184,  :round 7769}}
  ;; (float (/ 59545 7769)) = 7.6644354

  ;; Clojure 1.4.0, Nippy 1.0.0 (+ tagged-uuid, tagged-date)
  ;; {:reader {:freeze 22595, :thaw 31148, :round 54059}
  ;;  :nippy  {:freeze 3324,  :thaw 3725,  :round 6918}}
  ;; (float (/ 54059 6918)) = 7.814253

  ;; Clojure 1.5.1, Nippy 1.2.1 (+ sorted-set, sorted-map)
  ;; (def data (dissoc data :sorted-set :sorted-map))
  ;; {:reader {:freeze 15037, :thaw 27885, :round 43945},
  ;;  :nippy  {:freeze 3194,  :thaw 4734,  :round 8380}}
  ;; (float (/ 43945 8380)) = 5.2440333

;;; Data size
   (let [frozen (reader-freeze data)]   (count (.getBytes frozen "UTF8")))
   (let [frozen (freeze-to-bytes data)] (count frozen))
   ;; 22955, 12402
   ;; (float (/ 22955 12402)) = 1.8509111

;;; Snappy implementations
   (println (bench (roundtrip data)))
   ;;                No Snappy: 6163 6064 6042 6176
   ;;               Snappy JNI: 6489 6446 6542 6412
   ;; Snappy native array copy: 6569 6419 6414 6590
   )
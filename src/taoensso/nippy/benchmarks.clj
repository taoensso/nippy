(ns taoensso.nippy.benchmarks
  {:author "Peter Taoussanis"}
  (:use     [taoensso.nippy :as nippy :only (freeze-to-bytes thaw-from-bytes)])
  (:require [taoensso.nippy.utils :as utils]))

;; Remove stuff from stress-data that breaks reader
(def data (dissoc nippy/stress-data :queue :queue-empty :bytes))

(defmacro bench [& body] `(utils/bench 10000 (do ~@body) :warmup-laps 2000))

(defn reader-freeze [x] (binding [*print-dup* false] (pr-str x)))
(defn reader-thaw   [x] (binding [*read-eval* false] (read-string x)))
(def  reader-roundtrip (comp reader-thaw reader-freeze))

(def roundtrip-defaults  (comp nippy/thaw-from-bytes nippy/freeze-to-bytes))
(def roundtrip-encrypted (comp #(nippy/thaw-from-bytes % :password [:cached "p"])
                               #(nippy/freeze-to-bytes % :password [:cached "p"])))
(def roundtrip-fast      (comp #(nippy/thaw-from-bytes % :compressed? false)
                               #(nippy/freeze-to-bytes % :compress?   false)))

(defn autobench []
  (println "Benchmarking roundtrips")
  (println "-----------------------")
  (let [results {:defaults  (bench (roundtrip-defaults  data))
                 :encrypted (bench (roundtrip-encrypted data))
                 :fast      (bench (roundtrip-fast      data))}]
    (println results)
    results))

(comment

  (do ; Roundtrip times
    (println "Benching (this can take some time)...")
    (println "-------------------------------------")

    (println
     {:reader
      {:freeze (bench (reader-freeze data))
       :thaw   (let [frozen (reader-freeze data)]
                 (bench (reader-thaw frozen)))
       :round  (bench (reader-roundtrip data))
       :data-size (count (.getBytes ^String (reader-freeze data) "UTF-8"))}})

    (println
     {:defaults
      {:freeze (bench (freeze-to-bytes data))
       :thaw   (let [frozen (freeze-to-bytes data)]
                 (bench (thaw-from-bytes frozen)))
       :round  (bench (roundtrip-defaults data))
       :data-size (count (freeze-to-bytes data))}})

    (println
     {:encrypted
      {:freeze (bench (freeze-to-bytes data :password [:cached "p"]))
       :thaw   (let [frozen (freeze-to-bytes data :password [:cached "p"])]
                 (bench (thaw-from-bytes frozen :password [:cached "p"])))
       :round  (bench (roundtrip-encrypted data))
       :data-size (count (freeze-to-bytes data :password [:cached "p"]))}})

    (println
     {:fast
      {:freeze (bench (freeze-to-bytes data :compress? false))
       :thaw   (let [frozen (freeze-to-bytes data :compress? false)]
                 (bench (thaw-from-bytes frozen :compressed? false)))
       :round  (bench (roundtrip-fast data))
       :data-size (count (freeze-to-bytes data :compress? false))}})

    (println "Done! (Time for cake?)"))

  ;;; 11 June 2013: Clojure 1.5.1, Nippy 1.3.0-alpha1
  ;; {:reader    {:freeze 17042, :thaw 31579, :round 48379, :data-size 22954}}
  ;; {:fast      {:freeze 3078,  :thaw 4684,  :round 8117,  :data-size 13274}}
  ;; {:defaults  {:freeze 3810,  :thaw 5295,  :round 9052,  :data-size 12394}}
  ;; {:encrypted {:freeze 5800,  :thaw 6862,  :round 12317, :data-size 12416}}

  ;;; Clojure 1.5.1, Nippy 1.2.1 (+ sorted-set, sorted-map)
  ;; (def data (dissoc data :sorted-set :sorted-map))
  ;; {:reader {:freeze 15037, :thaw 27885, :round 43945},
  ;;  :nippy  {:freeze 3194,  :thaw 4734,  :round 8380}}
  ;; {:reader-size 22975, :defaults-size 12400, :encrypted-size 12400}

  ;;; Clojure 1.4.0, Nippy 1.0.0 (+ tagged-uuid, tagged-date)
  ;; {:reader {:freeze 22595, :thaw 31148, :round 54059}
  ;;  :nippy  {:freeze 3324,  :thaw 3725,  :round 6918}}

  ;;; Clojure 1.3.0, Nippy 0.9.2
  ;; {:reader {:freeze 28505, :thaw 36451, :round 59545},
  ;;  :nippy  {:freeze 3751,  :thaw 4184,  :round 7769}}

  (println (bench (roundtrip data))) ; Snappy implementations
  ;; {:no-snappy [6163 6064 6042 6176] :JNI [6489 6446 6542 6412]
  ;;  :native-array-copy [6569 6419 6414 6590]}
  )
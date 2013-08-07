(ns taoensso.nippy.benchmarks
  {:author "Peter Taoussanis"}
  (:require [clojure.tools.reader.edn :as edn]
            [taoensso.nippy :as nippy :refer (freeze thaw)]
            [taoensso.nippy.utils :as utils]))

;; Remove stuff from stress-data that breaks reader
(def data (dissoc nippy/stress-data :queue :queue-empty :bytes))

(defmacro bench* [& body] `(utils/bench 10000 (do ~@body) :warmup-laps 2000))

(defn freeze-reader [x] (pr-str x))
(defn thaw-reader   [x] (edn/read-string x))
(def  roundtrip-reader (comp thaw-reader freeze-reader))

(def roundtrip-defaults  (comp thaw freeze))
(def roundtrip-encrypted (comp #(thaw   % {:password [:cached "p"]})
                               #(freeze % {:password [:cached "p"]})))
(def roundtrip-fast      (comp thaw #(freeze % {:compressor nil})))

(defn bench [{:keys [reader? laps] :or {reader? true laps 1}}]
  (println)
  (println "Benching (this can take some time)")
  (println "----------------------------------")
  (dotimes [l laps]
    (println)
    (println (str "Lap " (inc l) "/" laps "..."))

    (when reader?
      (println
       {:reader
        {:round     (bench* (roundtrip-reader data))
         :freeze    (bench* (freeze-reader data))
         :thaw      (let [frozen (freeze-reader data)] (bench* (thaw-reader frozen)))
         :data-size (count (.getBytes ^String (freeze-reader data) "UTF-8"))}}))

    (println
     {:defaults
      {:round     (bench* (roundtrip-defaults data))
       :freeze    (bench* (freeze data))
       :thaw      (let [frozen (freeze data)] (bench* (thaw frozen)))
       :data-size (count (freeze data))}})

    (println
     {:encrypted
      {:round     (bench* (roundtrip-encrypted data))
       :freeze    (bench* (freeze data {:password [:cached "p"]}))
       :thaw      (let [frozen (freeze data {:password [:cached "p"]})]
                    (bench* (thaw frozen {:password [:cached "p"]})))
       :data-size (count (freeze data {:password [:cached "p"]}))}})

    (println
     {:fast
      {:round     (bench* (roundtrip-fast data))
       :freeze    (bench* (freeze data {:compressor nil}))
       :thaw      (let [frozen (freeze data {:compressor nil})]
                    (bench* (thaw frozen)))
       :data-size (count (freeze data {:compressor nil}))}}))

  (println)
  (println "Done! (Time for cake?)")
  true)

(comment
  ;; (bench {:reader? true  :laps 2})
  ;; (bench {:reader? false :laps 1})
  ;; (bench {:reader? false :laps 2})

  ;;; 7 Auguest 2013: Nippy v2.2.0-RC1
  ;; {:reader    {:round 71582, :freeze 13656, :thaw 56730, :data-size 22964}}
  ;; {:defaults  {:round 5619,  :freeze 3710,  :thaw 1783,  :data-size 12368}}
  ;; {:encrypted {:round 9113,  :freeze 5324,  :thaw 3500,  :data-size 12388}}
  ;; {:fast      {:round 5130,  :freeze 3286,  :thaw 1667,  :data-size 13325}}

  ;;; 17 June 2013: Clojure 1.5.1, JVM 7 Nippy 2.0.0-alpha6 w/fast io-streams
  ;; {:reader    {:round 49819, :freeze 23601, :thaw 26247, :data-size 22966}}
  ;; {:defaults  {:round 5670,  :freeze 3536,  :thaw 1919,  :data-size 12396}}
  ;; {:encrypted {:round 9038,  :freeze 5111,  :thaw 3582,  :data-size 12420}}
  ;; {:fast      {:round 5182,  :freeze 3177,  :thaw 1820,  :data-size 13342}}

  ;;; 16 June 2013: Clojure 1.5.1, Nippy 2.0.0-alpha6
  ;; {:reader    {:freeze 23601, :thaw 26247, :round 49819, :data-size 22966}}
  ;; {:defaults  {:freeze 3554,  :thaw 2002,  :round 5831,  :data-size 12394}}
  ;; {:encrypted {:freeze 5117,  :thaw 3600,  :round 9006,  :data-size 12420}}
  ;; {:fast      {:freeze 3247,  :thaw 1914,  :round 5329,  :data-size 13342}}

  ;;; 13 June 2013: Clojure 1.5.1, Nippy 2.0.0-alpha1
  ;; {:reader    {:freeze 23124, :thaw 26469, :round 47674, :data-size 22923}}
  ;; {:defaults  {:freeze 4007,  :thaw 2520,  :round 6038,  :data-size 12387}}
  ;; {:encrypted {:freeze 5560,  :thaw 3867,  :round 9157,  :data-size 12405}}
  ;; {:fast      {:freeze 3429,  :thaw 2078,  :round 5577,  :data-size 13237}}

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

  (println (bench* (roundtrip data))) ; Snappy implementations
  ;; {:no-snappy [6163 6064 6042 6176] :JNI [6489 6446 6542 6412]
  ;;  :native-array-copy [6569 6419 6414 6590]}
  )
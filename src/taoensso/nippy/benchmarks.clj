(ns taoensso.nippy.benchmarks
  {:author "Peter Taoussanis"}
  (:require [clojure.tools.reader.edn :as edn]
            [clojure.data.fressian    :as fressian]
            [taoensso.encore          :as encore]
            [taoensso.nippy :as nippy :refer (freeze thaw)]
            [taoensso.nippy.compression :as compression]))

(def data nippy/stress-data-benchable)

(defn fressian-freeze [value]
  (let [^java.nio.ByteBuffer bb (fressian/write value)
        len (.remaining bb)
        ba  (byte-array len)]
    (.get bb ba 0 len)
    ba))

(defn fressian-thaw [value]
  (let [bb (java.nio.ByteBuffer/wrap value)]
    (fressian/read bb)))

(comment (fressian-thaw (fressian-freeze data)))

(defmacro bench* [& body] `(encore/bench 10000 {:warmup-laps 20000} ~@body))
(defn     bench1 [freezer thawer & [sizer]]
  (let [data-frozen (freezer data)
        time-freeze (bench* (freezer data))
        time-thaw   (bench* (thawer  data-frozen))]
    {:round  (+ time-freeze time-thaw)
     :freeze time-freeze
     :thaw   time-thaw
     :size   ((or sizer count) data-frozen)}))

(defn bench [{:keys [reader? lzma2? fressian? laps] :or {laps 1}}]
  (println "\nBenching (this can take some time)")
  (println "----------------------------------")
  (dotimes [l laps]
    (println (str "\nLap " (inc l) "/" laps "..."))

    (when reader? ; Slow
      (println {:reader (bench1 #(pr-str %) #(edn/read-string %)
                                #(count (.getBytes ^String % "UTF-8")))}))

    (println {:default   (bench1 #(freeze % {})
                                 #(thaw   % {}))})
    (println {:fast      (bench1 #(freeze % {:compressor nil
                                             :skip-header? true})
                                 #(thaw   % {:headerless-meta
                                             {:version 2
                                              :compressed? false
                                              :encrypted?  false}}))})
    (println {:encrypted (bench1 #(freeze % {:password [:cached "p"]})
                                 #(thaw   % {:password [:cached "p"]}))})

    (when lzma2? ; Slow as molasses
      (println {:lzma2 (bench1 #(freeze % {:compressor compression/lzma2-compressor})
                               #(thaw   % {:compressor compression/lzma2-compressor}))}))

    (when fressian?
      (println {:fressian (bench1 fressian-freeze fressian-thaw)})))

  (println "\nDone! (Time for cake?)")
  true)

(comment (edn/read-string (pr-str data))
         (bench1 fressian-freeze fressian-thaw))

(comment
  ;; (bench {:reader? true :lzma2? true :fressian? true :laps 1})
  ;; (bench {:laps 2})

  ;;; 2014 Apr 5 w/ headerless :fast, LZ4 replacing Snappy as default compressor
  ;; {:default   {:round 7669,  :freeze 4157, :thaw 3512, :size 16143}}
  ;; {:fast      {:round 6918,  :freeze 3636, :thaw 3282, :size 16992}}
  ;; {:encrypted {:round 11814, :freeze 6180, :thaw 5634, :size 16164}}

  ;;; 2014 Jan 22: with common-type size optimizations, enlarged stress-data
  ;; {:reader    {:round 109544, :freeze 39523, :thaw 70021, :size 27681}}
  ;; {:default   {:round 9234,   :freeze 5128,  :thaw 4106,  :size 15989}}
  ;; {:fast      {:round 7402,   :freeze 4021,  :thaw 3381,  :size 16957}}
  ;; {:encrypted {:round 12594,  :freeze 6884,  :thaw 5710,  :size 16020}}
  ;; {:lzma2     {:round 66759,  :freeze 44246, :thaw 22513, :size 11208}}
  ;; {:fressian  {:round 13052,  :freeze 8694,  :thaw 4358,  :size 16942}}

  ;;; 19 Oct 2013: Nippy v2.3.0, with lzma2 & (nb!) round=freeze+thaw
  ;; {:reader    {:round 67798,  :freeze 23202,  :thaw 44596, :size 22971}}
  ;; {:default   {:round 3632,   :freeze 2349,   :thaw 1283,  :size 12369}}
  ;; {:encrypted {:round 6970,   :freeze 4073,   :thaw 2897,  :size 12388}}
  ;; {:fast      {:round 3294,   :freeze 2109,   :thaw 1185,  :size 13277}}
  ;; {:lzma2     {:round 44590,  :freeze 29567,  :thaw 15023, :size 9076}}

  ;;; 11 Oct 2013: Nippy v2.2.0, with both ztellman mods
  ;; {:defaults  {:round 4319, :freeze 2950, :thaw 1446, :data-size 12369}}
  ;; {:encrypted {:round 7675, :freeze 4479, :thaw 3160, :data-size 12388}}
  ;; {:fast      {:round 3928, :freeze 2530, :thaw 1269, :data-size 13277}}
  ;; {:defaults-delta {:round 0.84 :freeze 0.79 :thaw 1.14}} ; vs 2.2.0

  ;;; 11 Oct 2013: Nippy v2.2.0, with first ztellman mod
  ;; {:defaults  {:round 4059, :freeze 2578, :thaw 1351, :data-size 12342}}
  ;; {:encrypted {:round 7248, :freeze 4058, :thaw 3041, :data-size 12372}}
  ;; {:fast      {:round 3430, :freeze 2085, :thaw 1229, :data-size 13277}}
  ;; {:defaults-delta {:round  0.79 :freeze 0.69 :thaw 1.07}} ; vs 2.2.0

  ;;; 11 Oct 2013: Nippy v2.2.0
  ;; {:defaults  {:round 5135, :freeze 3711, :thaw 1266, :data-size 12393}}
  ;; {:encrypted {:round 8655, :freeze 5323, :thaw 3036, :data-size 12420}}
  ;; {:fast      {:round 4670, :freeze 3282, :thaw 1294, :data-size 13277}}

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

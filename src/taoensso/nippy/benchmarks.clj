(ns taoensso.nippy.benchmarks
  (:require [clojure.data.fressian :as fressian]
            [taoensso.encore       :as enc]
            [taoensso.nippy :as nippy :refer (freeze thaw)]))

(def data #_22 nippy/stress-data-benchable)

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

(defmacro bench* [& body] `(enc/bench 10000 {:warmup-laps 25000} ~@body))
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
      (println {:reader (bench1 enc/pr-edn enc/read-edn
                          #(count (.getBytes ^String % "UTF-8")))}))

    (when lzma2? ; Slow
      (println {:lzma2 (bench1 #(freeze % {:compressor nippy/lzma2-compressor})
                         #(thaw   % {:compressor nippy/lzma2-compressor}))}))

    (when fressian?
      (println {:fressian (bench1 fressian-freeze fressian-thaw)}))

    (println {:encrypted (bench1 #(freeze % {:password [:cached "p"]})
                                 #(thaw   % {:password [:cached "p"]}))})
    (println {:default   (bench1 #(freeze % {})
                                 #(thaw   % {}))})
    (println {:fast1     (bench1 #(freeze % {:compressor nil})
                                 #(thaw   % {:compressor nil}))})
    (println {:fast2     (bench1 #(freeze % {:no-header? true
                                             :compressor nil})
                                 #(thaw   % {:no-header? true
                                             :compressor nil
                                             :encryptor  nil}))}))

  (println "\nDone! (Time for cake?)")
  true)

(comment (enc/read-edn (enc/pr-edn data))
         (bench1 fressian-freeze fressian-thaw))

(comment
  (set! *unchecked-math* false)
  ;; (bench {:reader? true :lzma2? true :fressian? true :laps 2})
  ;; (bench {:laps 2})

  ;;; 2016 Apr 13, v2.12.0-SNAPSHOT, refactor + larger data + new hardware
  {:reader    {:round 52105, :freeze 17678, :thaw 34427, :size 27831}}
  {:lzma2     {:round 43436, :freeze 28518, :thaw 14918, :size 11272}}
  {:fressian  {:round 6875,  :freeze 5035,  :thaw 1840,  :size 17105}}
  {:encrypted {:round 4718,  :freeze 2872,  :thaw 1846,  :size 16420}}
  {:default   {:round 4250,  :freeze 2547,  :thaw 1703,  :size 16400}}
  {:fast1     {:round 3777,  :freeze 2118,  :thaw 1659,  :size 17070}}
  {:fast2     {:round 3753,  :freeze 2119,  :thaw 1634,  :size 17066}}
  ;; 12.26

  ;;; 2015 Oct 6, v2.11.0-alpha4
  {:reader    {:round 73409, :freeze 21823, :thaw 51586, :size 27672}}
  {:lzma2     {:round 56689, :freeze 37222, :thaw 19467, :size 11252}}
  {:fressian  {:round 10666, :freeze 7737,  :thaw 2929,  :size 16985}}
  {:encrypted {:round 6885,  :freeze 4227,  :thaw 2658,  :size 16148}}
  {:default   {:round 6304,  :freeze 3824,  :thaw 2480,  :size 16122}}
  {:fast1     {:round 5352,  :freeze 3272,  :thaw 2080,  :size 16976}}
  {:fast2     {:round 5243,  :freeze 3238,  :thaw 2005,  :size 16972}}
  ;; :reader/:default ratio: 11.64
  ;;
  {:reader    {:round 26,   :freeze 17,   :thaw 9,   :size 2}}
  {:lzma2     {:round 3648, :freeze 3150, :thaw 498, :size 68}}
  {:fressian  {:round 19,   :freeze 7,    :thaw 12,  :size 1}}
  {:encrypted {:round 63,   :freeze 40,   :thaw 23,  :size 36}}
  {:default   {:round 24,   :freeze 17,   :thaw 7,   :size 6}}
  {:fast1     {:round 19,   :freeze 12,   :thaw 7,   :size 6}}
  {:fast2     {:round 4,    :freeze 2,    :thaw 2,   :size 2}}

  ;;; 2015 Sep 29, after read/write API refactor
  {:lzma2     {:round 51640, :freeze 33699, :thaw 17941, :size 11240}}
  {:encrypted {:round 5922,  :freeze 3734,  :thaw 2188,  :size 16132}}
  {:default   {:round 5588,  :freeze 3658,  :thaw 1930,  :size 16113}}
  {:fast      {:round 4533,  :freeze 2688,  :thaw 1845,  :size 16972}}

  ;;; 2015 Sep 28, small collection optimizations
  {:lzma2     {:round 56307, :freeze 36475, :thaw 19832, :size 11244}}
  {:encrypted {:round 6062,  :freeze 3802,  :thaw 2260,  :size 16148}}
  {:default   {:round 5482,  :freeze 3382,  :thaw 2100,  :size 16128}}
  {:fast      {:round 4729,  :freeze 2826,  :thaw 1903,  :size 16972}}

  ;;; 2015 Sep 29, various micro optimizations (incl. &arg elimination)
  {:reader    {:round 63547, :freeze 19374, :thaw 44173, :size 27717}}
  {:lzma2     {:round 51724, :freeze 33502, :thaw 18222, :size 11248}}
  {:fressian  {:round 8813,  :freeze 6460,  :thaw 2353,  :size 16985}}
  {:encrypted {:round 6005,  :freeze 3768,  :thaw 2237,  :size 16164}}
  {:default   {:round 5417,  :freeze 3354,  :thaw 2063,  :size 16145}}
  {:fast      {:round 4659,  :freeze 2712,  :thaw 1947,  :size 17026}}

  ;;; 2015 Sep 15 - v2.10.0-alpha6, Clojure 1.7.0
  {:reader    {:round 94901, :freeze 25781, :thaw 69120, :size 27686}}
  {:lzma2     {:round 65127, :freeze 43150, :thaw 21977, :size 11244}}
  {:encrypted {:round 12590, :freeze 7565,  :thaw 5025,  :size 16148}}
  {:fressian  {:round 12085, :freeze 9168,  :thaw 2917,  :size 16972}}
  {:default   {:round 6974,  :freeze 4582,  :thaw 2392,  :size 16123}}
  {:fast      {:round 6255,  :freeze 3724,  :thaw 2531,  :size 17013}}

  ;;; 2015 Sep 14 - v2.10.0-alpha5, Clojure 1.7.0-RC1
  {:default   {:round 6870,  :freeze 4376, :thaw 2494, :size 16227}}
  {:fast      {:round 6104,  :freeze 3743, :thaw 2361, :size 17013}}
  {:encrypted {:round 12155, :freeze 6908, :thaw 5247, :size 16244}}

  ;;; 2015 June 4 - v2.9.0, Clojure 1.7.0-RC1
  {:reader    {:round 155353, :freeze 44192, :thaw 111161, :size 27693}}
  {:lzma2     {:round 102484, :freeze 68274, :thaw 34210,  :size 11240}}
  {:fressian  {:round 44665,  :freeze 34996, :thaw 9669,   :size 16972}}
  {:encrypted {:round 19791,  :freeze 11354, :thaw 8437,   :size 16148}}
  {:default   {:round 12302,  :freeze 8310,  :thaw 3992,   :size 16126}}
  {:fast      {:round 9802,   :freeze 5944,  :thaw 3858,   :size 17013}}

  ;;; 2015 Apr 17 w/ smart compressor selection, Clojure 1.7.0-beta1
  {:default   {:round 6163,  :freeze 4095, :thaw 2068, :size 16121}}
  {:fast      {:round 5417,  :freeze 3480, :thaw 1937, :size 17013}}
  {:encrypted {:round 10950, :freeze 6400, :thaw 4550, :size 16148}}

  ;;; 2014 Apr 7 w/ some additional implementation tuning
  {:default   {:round 6533,  :freeze 3618, :thaw 2915, :size 16139}}
  {:fast      {:round 6250,  :freeze 3376, :thaw 2874, :size 16992}}
  {:encrypted {:round 10583, :freeze 5581, :thaw 5002, :size 16164}}

  ;;; 2014 Apr 5 w/ headerless :fast, LZ4 replacing Snappy as default compressor
  {:default   {:round 7039,  :freeze 3865, :thaw 3174, :size 16123}}
  {:fast      {:round 6394,  :freeze 3379, :thaw 3015, :size 16992}}
  {:encrypted {:round 11035, :freeze 5860, :thaw 5175, :size 16148}}

  ;;; 2014 Jan 22: with common-type size optimizations, enlarged stress-data
  {:reader    {:round 109544, :freeze 39523, :thaw 70021, :size 27681}}
  {:default   {:round 9234,   :freeze 5128,  :thaw 4106,  :size 15989}}
  {:fast      {:round 7402,   :freeze 4021,  :thaw 3381,  :size 16957}}
  {:encrypted {:round 12594,  :freeze 6884,  :thaw 5710,  :size 16020}}
  {:lzma2     {:round 66759,  :freeze 44246, :thaw 22513, :size 11208}}
  {:fressian  {:round 13052,  :freeze 8694,  :thaw 4358,  :size 16942}}

  ;;; 19 Oct 2013: Nippy v2.3.0, with lzma2 & (nb!) round=freeze+thaw
  {:reader    {:round 67798,  :freeze 23202,  :thaw 44596, :size 22971}}
  {:default   {:round 3632,   :freeze 2349,   :thaw 1283,  :size 12369}}
  {:encrypted {:round 6970,   :freeze 4073,   :thaw 2897,  :size 12388}}
  {:fast      {:round 3294,   :freeze 2109,   :thaw 1185,  :size 13277}}
  {:lzma2     {:round 44590,  :freeze 29567,  :thaw 15023, :size 9076}})

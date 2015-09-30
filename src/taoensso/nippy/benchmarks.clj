(ns taoensso.nippy.benchmarks
  {:author "Peter Taoussanis"}
  (:require [clojure.data.fressian    :as fressian]
            [taoensso.encore          :as encore]
            [taoensso.nippy :as nippy :refer (freeze thaw)]))

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
      (println {:reader (bench1 encore/pr-edn encore/read-edn
                          #(count (.getBytes ^String % "UTF-8")))}))

    (println {:default   (bench1 #(freeze % {})
                                 #(thaw   % {}))})
    (println {:fast      (bench1 #(freeze % {:compressor nil
                                             :skip-header? true})
                                 #(thaw   % {:compressor nil
                                             :encryptor  nil}))})
    (println {:encrypted (bench1 #(freeze % {:password [:cached "p"]})
                                 #(thaw   % {:password [:cached "p"]}))})

    (when lzma2? ; Slow as molasses
      (println {:lzma2 (bench1 #(freeze % {:compressor nippy/lzma2-compressor})
                               #(thaw   % {:compressor nippy/lzma2-compressor}))}))

    (when fressian?
      (println {:fressian (bench1 fressian-freeze fressian-thaw)})))

  (println "\nDone! (Time for cake?)")
  true)

(comment (encore/read-edn (encore/pr-edn data))
         (bench1 fressian-freeze fressian-thaw))

(comment
  (set! *unchecked-math* false)
  ;; (bench {:reader? true :lzma2? true :fressian? true :laps 3})
  ;; (bench {:laps 4})
  ;; (bench {:laps 1 :lzma2? true})

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

(ns taoensso.nippy.benchmarks
  {:author "Peter Taoussanis"}
  (:require [clojure.tools.reader.edn :as edn]
            [clojure.data.fressian    :as fressian]
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
      (println {:reader (bench1 #(pr-str %) #(edn/read-string %)
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

(comment (edn/read-string (pr-str data))
         (bench1 fressian-freeze fressian-thaw))

(comment
  ;; (bench {:reader? true :lzma2? true :fressian? true :laps 1})
  ;; (bench {:laps 2})

  ;;; 2014 Apr 5 w/ headerless :fast, LZ4 replacing Snappy as default compressor
  {:default   {:round 7669,  :freeze 4157, :thaw 3512, :size 16143}}
  {:fast      {:round 6918,  :freeze 3636, :thaw 3282, :size 16992}}
  {:encrypted {:round 11814, :freeze 6180, :thaw 5634, :size 16164}}

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

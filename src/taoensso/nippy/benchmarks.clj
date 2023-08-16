(ns taoensso.nippy.benchmarks
  "Nippy benchmarks."
  (:require
   [clojure.data.fressian :as fress]
   [taoensso.encore       :as enc]
   [taoensso.nippy        :as nippy]))

;;;; Reader

(defn- freeze-reader [x]   (enc/pr-edn   x))
(defn- thaw-reader   [edn] (enc/read-edn edn))

;;;; Fressian

(defn- freeze-fress [x]
  (let [^java.nio.ByteBuffer bb (fress/write x)
        len (.remaining bb)
        ba  (byte-array len)]
    (.get bb ba 0 len)
    (do      ba)))

(defn- thaw-fress [^bytes ba]
  (let [bb (java.nio.ByteBuffer/wrap ba)]
    (fress/read bb)))

(comment (-> data freeze-fress thaw-fress))

;;;; Benchable data

(def data
  "Map of data suitable for benching, a subset of
  `nippy/stress-data-comparable`."
  (reduce-kv
    (fn [m k v]
      (try
        (-> v freeze-reader thaw-reader)
        (-> v freeze-fress  thaw-fress)
        m
        (catch Throwable _ (dissoc m k))))
    nippy/stress-data-comparable
    nippy/stress-data-comparable))

(comment
  (clojure.set/difference
    (set (keys nippy/stress-data-comparable))
    (set (keys data))))

;;;;

(defn- bench1
  [{:keys [laps warmup] :or {laps 1e4, warmup 25e3}} freezer thawer sizer]
  (let [data-frozen (freezer data)
        time-freeze (enc/bench laps {:warmup-laps warmup} (freezer data))
        time-thaw   (enc/bench laps {:warmup-laps warmup} (thawer  data-frozen))
        data-size   (sizer data-frozen)]

    {:round (+ time-freeze time-thaw)
     :freeze   time-freeze
     :thaw                 time-thaw
     :size  data-size}))

(comment (bench1 {} nippy/freeze nippy/thaw count))

(defn bench
  [{:keys [all? reader? fressian? fressian? lzma2? laps warmup]
    :or   {laps      1e4
           warmup    25e3}}]

  (println "\nStarting benchmarks")

  (let [results_ (atom {})]
    (when (or all? reader?)
      (println "Benching Reader...")
      (swap! results_ assoc :reader
        (bench1 {:laps laps, :warmup warmup}
          freeze-reader thaw-reader
          (fn [^String s] (count (.getBytes s "UTF-8"))))))

    (when (or all? fressian?)
      (println "- Benching Fressian...")
      (swap! results_ assoc :fressian (bench1 {:laps laps, :warmup warmup} freeze-fress thaw-fress count)))

    (when (or all? lzma2?)
      (println "- Benching Nippy/LZMA2...")
      (swap! results_ assoc :nippy/lzma2
        (bench1 {:laps laps, :warmup warmup}
          #(nippy/freeze % {:compressor nippy/lzma2-compressor})
          #(nippy/thaw   % {:compressor nippy/lzma2-compressor})
          count)))

    (println "- Benching Nippy/encrypted...")
    (swap! results_ assoc :nippy/encrypted
      (bench1 {:laps laps, :warmup warmup}
        #(nippy/freeze % {:password [:cached "p"]})
        #(nippy/thaw   % {:password [:cached "p"]})
        count))

    (println "- Benching Nippy/default...")
    (swap! results_ assoc :nippy/default (bench1 {:laps laps, :warmup warmup} nippy/freeze      nippy/thaw      count))

    (println "- Benching Nippy/fast...")
    (swap! results_ assoc :nippy/fast (bench1 {:laps laps, :warmup warmup} nippy/fast-freeze nippy/fast-thaw count))

    (println "- Benchmarks complete! (Time for cake?)")

    (let [results @results_]
      (println "\nBenchmark results:")
      (doseq [[k v] results] (println k " " v))
      (do           results))))

(comment
  (do
    (set! *unchecked-math* false)
    (bench {:all? true}))

  ;; 2023 Aug 1, 2020 Apple MBP M1
  ;; [com.taoensso/nippy        "3.2.0"]
  ;; [org.clojure/tools.reader  "1.3.6"]
  ;; [org.clojure/data.fressian "1.0.0"]

  {:reader          {:round 35041, :freeze 5942,  :thaw 29099, :size 39389}
   :fressian        {:round 6241,  :freeze 3429,  :thaw 2812,  :size 22850}
   :nippy/lzma2     {:round 33561, :freeze 20530, :thaw 13031, :size 11444}
   :nippy/encrypted {:round 3390,  :freeze 1807,  :thaw 1583,  :size 16468}
   :nippy/default   {:round 2845,  :freeze 1513,  :thaw 1332,  :size 16440}
   :nippy/fast      {:round 2634,  :freeze 1338,  :thaw 1296,  :size 28454}})

(ns taoensso.nippy-benchmarks
  (:require
   [clojure.data.fressian      :as fress]
   [taoensso.encore            :as enc]
   [taoensso.nippy             :as nippy]
   [taoensso.nippy.compression :as compr]))

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
    (swap! results_ assoc :nippy/fast    (bench1 {:laps laps, :warmup warmup} nippy/fast-freeze nippy/fast-thaw count))

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

;;;; Compressors

(let [_    (require '[taoensso.nippy :as nippy])
      data (nippy/freeze nippy/stress-data-comparable {:compressor nil})]

  (defn bench1-compressor
    [{:keys [laps warmup] :or {laps 1e4, warmup 2e4}} compressor]
    (let [data-compressed                                       (compr/compress   compressor data)
          time-compress   (enc/bench laps {:warmup-laps warmup} (compr/compress   compressor data))
          time-decompress (enc/bench laps {:warmup-laps warmup} (compr/decompress compressor data-compressed))]

      {:round (+ time-compress time-decompress)
       :compress   time-compress
       :decompress time-decompress
       :ratio      (enc/round2 (/ (count data-compressed) (count data)))}))

  (defn bench-compressors [bench1-opts lzma-opts]
    (merge
      (let [bench1 #(bench1-compressor bench1-opts %)]
        {:zstd/prepended     (bench1 (compr/->ZstdCompressor true))
         :zstd/unprepended   (bench1 (compr/->ZstdCompressor false))
         :lz4                (bench1 (compr/->LZ4Compressor))
         :lzo                (bench1 (compr/->LZOCompressor))
         :snappy/prepended   (bench1 (compr/->SnappyCompressor true))
         :snappy/unprepended (bench1 (compr/->SnappyCompressor false))})

      (let [bench1 #(bench1-compressor (merge bench1-opts lzma-opts) %)]
        {:lzma2/level0 (bench1 (compr/->LZMA2Compressor 0))
         :lzma2/level3 (bench1 (compr/->LZMA2Compressor 3))
         :lzma2/level6 (bench1 (compr/->LZMA2Compressor 6))
         :lzma2/level9 (bench1 (compr/->LZMA2Compressor 9))}))))

(comment
  (bench-compressors
    {:laps 1e4 :warmup 2e4}
    {:laps 1e2 :warmup 2e2})

  ;; 2023 Aug 1, 2020 Apple MBP M1
  ;; [org.tukaani/xz           "1.9"]
  ;; [io.airlift/aircompressor "0.25"]

  {:zstd/prepended     {:round 1672,   :compress 1279,   :decompress 393,   :ratio 0.53}
   :zstd/unprepended   {:round 1668,   :compress 1271,   :decompress 397,   :ratio 0.53}
   :lz4                {:round 269,    :compress 238,    :decompress 31,    :ratio 0.58}
   :lzo                {:round 259,    :compress 216,    :decompress 43,    :ratio 0.58}
   :snappy/prepended   {:round 339,    :compress 205,    :decompress 134,   :ratio 0.58}
   :snappy/unprepended {:round 340,    :compress 206,    :decompress 134,   :ratio 0.58}
   :lzma2/level0       {:round 30300,  :compress 18500,  :decompress 11800, :ratio 0.4}
   :lzma2/level3       {:round 49200,  :compress 35700,  :decompress 13500, :ratio 0.4}
   :lzma2/level6       {:round 102600, :compress 86700,  :decompress 15900, :ratio 0.41}
   :lzma2/level9       {:round 434800, :compress 394700, :decompress 40100, :ratio 0.41}})

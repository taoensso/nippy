(ns taoensso.nippy-benchmarks
  (:require
   [clojure.data.fressian :as fress]
   [taoensso.encore       :as enc]
   [taoensso.nippy        :as nippy]
   [taoensso.nippy
    [impl        :as impl]
    [compression :as compr]]))

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

;;;; Bench data

(def default-bench-data
  "Subset of stress data suitable for benching."
  (let [sd (nippy/stress-data {:comparable? true})]
    (reduce-kv
      (fn [m k v]
        (try
          (-> v freeze-reader thaw-reader)
          (-> v freeze-fress  thaw-fress)
          m
          (catch Throwable _ (dissoc m k))))
      sd sd)))

(comment
  (clojure.set/difference
    (set (keys (nippy/stress-data {:comparable? true})))
    (set (keys default-bench-data))))

;;;;  Serialization

(defn- bench1-serialization
  [freezer thawer sizer
   {:keys [laps warmup bench-data]
    :or
    {laps       1e4
     warmup     25e3
     bench-data default-bench-data}}]

  (let [data-frozen                                       (freezer bench-data)
        time-freeze (enc/bench laps {:nlaps-warmup warmup} (freezer bench-data))
        time-thaw   (enc/bench laps {:nlaps-warmup warmup} (thawer  data-frozen))
        data-size   (sizer data-frozen)]

    {:round (+ time-freeze time-thaw)
     :freeze   time-freeze
     :thaw                 time-thaw
     :size  data-size}))

(comment (bench1-serialization nippy/freeze nippy/thaw count {}))

(defn- printed-results [results]
  (println "\nBenchmark results:")
  (doseq [[k v] results] (println k " " v))
  (do           results))

(defn bench-serialization
  [{:keys [all? reader? fressian? fressian? lzma2? laps warmup bench-data]
    :as   opts
    :or
    {laps   1e4
     warmup 25e3}}]

  (println (str "\nRunning benchmarks (target release: " impl/target-release ")..."))

  (let [results_ (atom {})]
    (when (or all? reader?)
      (println "  With Reader...")
      (swap! results_ assoc :reader
        (bench1-serialization freeze-reader thaw-reader
          (fn [^String s] (count (.getBytes s "UTF-8")))
          (assoc opts :laps laps, :warmup warmup))))

    (when (or all? fressian?)
      (println "  With Fressian...")
      (swap! results_ assoc :fressian
        (bench1-serialization freeze-fress thaw-fress count
          (assoc opts :laps laps, :warmup warmup))))

    (when (or all? lzma2?)
      (println "  With Nippy/LZMA2...")
      (swap! results_ assoc :nippy/lzma2
        (bench1-serialization
          #(nippy/freeze % {:compressor nippy/lzma2-compressor})
          #(nippy/thaw   % {:compressor nippy/lzma2-compressor})
          count
          (assoc opts :laps laps, :warmup warmup))))

    (println "  With Nippy/encrypted...")
    (swap! results_ assoc :nippy/encrypted
      (bench1-serialization
        #(nippy/freeze % {:password [:cached "p"]})
        #(nippy/thaw   % {:password [:cached "p"]})
        count
        (assoc opts :laps laps, :warmup warmup)))

    (println "  With Nippy/default...")
    (swap! results_ assoc :nippy/default
      (bench1-serialization nippy/freeze nippy/thaw count
        (assoc opts :laps laps, :warmup warmup)))

    (println "  With Nippy/fast...")
    (swap! results_ assoc :nippy/fast
      (bench1-serialization nippy/fast-freeze nippy/fast-thaw count
        (assoc opts :laps laps, :warmup warmup)))

    (println "\nBenchmarks done:")
    (printed-results @results_)))

;;;; Compression

(defn- bench1-compressor
  [compressor
   {:keys [laps warmup bench-data]
    :or
    {laps       1e4
     warmup     2e4
     bench-data default-bench-data}}]

  (let [data-frozen     (nippy/freeze bench-data {:compressor nil})
        data-compressed                                       (compr/compress   compressor data-frozen)
        time-compress   (enc/bench laps {:nlaps-warmup warmup} (compr/compress   compressor data-frozen))
        time-decompress (enc/bench laps {:nlaps-warmup warmup} (compr/decompress compressor data-compressed))]

    {:round   (+ time-compress time-decompress)
     :compress   time-compress
     :decompress time-decompress
     :ratio      (enc/round2 (/ (count data-compressed) (count data-frozen)))}))

(defn bench-compressors [opts lzma-opts]
  (printed-results
    (merge
      (let [bench1 #(bench1-compressor % opts)]
        {:zstd/prepended     (bench1 (compr/->ZstdCompressor true))
         :zstd/unprepended   (bench1 (compr/->ZstdCompressor false))
         :lz4                (bench1 (compr/->LZ4Compressor))
         :lzo                (bench1 (compr/->LZOCompressor))
         :snappy/prepended   (bench1 (compr/->SnappyCompressor true))
         :snappy/unprepended (bench1 (compr/->SnappyCompressor false))})

      (let [bench1 #(bench1-compressor % (merge opts lzma-opts))]
        {:lzma2/level0 (bench1 (compr/->LZMA2Compressor 0))
         :lzma2/level3 (bench1 (compr/->LZMA2Compressor 3))
         :lzma2/level6 (bench1 (compr/->LZMA2Compressor 6))
         :lzma2/level9 (bench1 (compr/->LZMA2Compressor 9))}))))

;;;; Results

(comment
  {:last-updated    "2026-09-06"
   :system          "2020 Macbook Pro M1, 16 GB memory"
   :clojure-version "1.12.5"
   :java-version    "OpenJDK 26.0.2.1"
   :deps
   '[[com.taoensso/nippy        "3.9.0"]
     [org.clojure/tools.reader  "1.6.0"]
     [org.clojure/data.fressian "1.1.1"]
     [org.tukaani/xz            "1.12"]
     [io.airlift/aircompressor  "2.0.3"]]}

  (bench-serialization {:all? true})

  ;; Per-category medians of 3 AC-powered runs
  {:reader          {:round 11042, :freeze 3533, :thaw 7052, :size 15880}
   :fressian        {:round 2957,  :freeze 1975, :thaw 932,  :size 12222}
   :nippy/lzma2     {:round 9451,  :freeze 6082, :thaw 3375, :size 3700}
   :nippy/encrypted {:round 1156,  :freeze 558,  :thaw 600,  :size 7863}
   :nippy/default   {:round 1093,  :freeze 523,  :thaw 578,  :size 7835}
   :nippy/fast      {:round 885,   :freeze 359,  :thaw 529,  :size 14017}}

  (bench-compressors
    {:laps 1e4 :warmup 2e4}
    {:laps 1e2 :warmup 2e2})

  ;; Times in msecs / 10k laps, except LZMA2: 100 laps
  ;; Note that ratio depends on compressibility of stress data
  ;; Per-category medians of 3 AC-powered runs
  {:lz4                {:round 203,  :compress 161,  :decompress 42,  :ratio 0.56}
   :lzo                {:round 248,  :compress 187,  :decompress 60,  :ratio 0.52}
   :snappy/prepended   {:round 334,  :compress 208,  :decompress 126, :ratio 0.48}
   :snappy/unprepended {:round 332,  :compress 207,  :decompress 125, :ratio 0.48}
   :zstd/prepended     {:round 976,  :compress 741,  :decompress 233, :ratio 0.35}
   :zstd/unprepended   {:round 974,  :compress 740,  :decompress 234, :ratio 0.35}
   :lzma2/level0       {:round 94,   :compress 65,   :decompress 30,  :ratio 0.26}
   :lzma2/level3       {:round 165,  :compress 130,  :decompress 35,  :ratio 0.25}
   :lzma2/level6       {:round 666,  :compress 622,  :decompress 44,  :ratio 0.25}
   :lzma2/level9       {:round 1911, :compress 1765, :decompress 147, :ratio 0.25}})

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
  {:last-updated    "2026-07-22"
   :system          "2020 Macbook Pro M1, 16 GB memory"
   :clojure-version "1.12.5"
   :java-version    "OpenJDK 25"
   :deps
   '[[com.taoensso/nippy        "3.8.0-RC1"]
     [org.clojure/tools.reader  "1.6.0"]
     [org.clojure/data.fressian "1.1.1"]
     [org.tukaani/xz            "1.12"]
     [io.airlift/aircompressor  "2.0.3"]]}

  (bench-serialization {:all? true})

  ;; Per-category medians of 3 AC-powered runs
  {:reader          {:round 12970, :freeze 3852, :thaw 9118, :size 15880}
   :fressian        {:round 4131,  :freeze 2948, :thaw 1183, :size 12222}
   :nippy/lzma2     {:round 12613, :freeze 8114, :thaw 4499, :size 3888}
   :nippy/encrypted {:round 2606,  :freeze 1326, :thaw 1280, :size 8546}
   :nippy/default   {:round 2216,  :freeze 1029, :thaw 1187, :size 8518}
   :nippy/fast      {:round 1882,  :freeze 822,  :thaw 1060, :size 17105}}

  (bench-compressors
    {:laps 1e4 :warmup 2e4}
    {:laps 1e2 :warmup 2e2})

  ;; Note that ratio depends on compressibility of stress data
  ;; Per-category medians of 3 AC-powered runs
  {:lz4                {:round 354,  :compress 260,  :decompress 94,  :ratio 0.5}
   :lzo                {:round 487,  :compress 331,  :decompress 156, :ratio 0.45}
   :snappy/prepended   {:round 494,  :compress 309,  :decompress 185, :ratio 0.42}
   :snappy/unprepended {:round 441,  :compress 272,  :decompress 169, :ratio 0.42}
   :zstd/prepended     {:round 1908, :compress 1124, :decompress 784, :ratio 0.29}
   :zstd/unprepended   {:round 1193, :compress 949,  :decompress 244, :ratio 0.29}
   :lzma2/level0       {:round 173,  :compress 116,  :decompress 57,  :ratio 0.23}
   :lzma2/level3       {:round 258,  :compress 219,  :decompress 39,  :ratio 0.21}
   :lzma2/level6       {:round 916,  :compress 868,  :decompress 48,  :ratio 0.21}
   :lzma2/level9       {:round 2089, :compress 1930, :decompress 159, :ratio 0.21}})

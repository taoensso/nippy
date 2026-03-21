(ns taoensso.nippy-benchmarks
  (:require
   [clojure.data.fressian      :as fress]
   [taoensso.encore            :as enc]
   [taoensso.nippy             :as nippy]
   [taoensso.nippy.compression :as compr])
  (:import
   [java.io ByteArrayInputStream ByteArrayOutputStream DataInputStream DataOutputStream]
   [java.nio ByteBuffer]))

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
        time-freeze (enc/bench laps {:warmup-laps warmup} (freezer bench-data))
        time-thaw   (enc/bench laps {:warmup-laps warmup} (thawer  data-frozen))
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

(defn bench-byte-buffer-low-level
  "Benchmarks low-level stream-backed freeze/thaw against ByteBuffer-backed
  freeze/thaw. Intended as a focused micro-benchmark for adapter overhead."
  [{:keys [laps warmup bench-data]
    :or
    {laps       1e4
     warmup     25e3
     bench-data default-bench-data}}]
  (println "\nRunning low-level stream vs byte-buffer benchmark...")
  (let [ref-frozen (nippy/fast-freeze bench-data)
        cap        (+ (count ref-frozen) 64)

        freeze-stream
        (fn []
          (let [baos (ByteArrayOutputStream. cap)
                dos  (DataOutputStream. baos)]
            (nippy/with-cache (nippy/freeze-to-out! dos bench-data))
            (.toByteArray baos)))

        ;; Use runtime resolution: freeze-to-byte-buffer! is only available
        ;; in the current branch, not in the released nippy v3.6.0.
        freeze-to-bb! (requiring-resolve 'taoensso.nippy/freeze-to-byte-buffer!)
        thaw-from-bb! (requiring-resolve 'taoensso.nippy/thaw-from-byte-buffer!)

        freeze-bb
        (fn []
          (let [bb (ByteBuffer/allocate cap)]
            (nippy/with-cache (freeze-to-bb! bb bench-data))
            (let [len (.position bb)
                  ba  (byte-array len)]
              (.flip bb)
              (.get bb ba 0 len)
              ba)))

        thaw-stream
        (fn [^bytes ba]
          (let [dis (DataInputStream. (ByteArrayInputStream. ba))]
            (nippy/with-cache (nippy/thaw-from-in! dis))))

        thaw-bb
        (fn [^bytes ba]
          (let [bb (ByteBuffer/wrap ba)]
            (nippy/with-cache (thaw-from-bb! bb))))

        data-stream (freeze-stream)
        data-bb     (freeze-bb)]

    (assert (= bench-data (thaw-stream data-stream)))
    (assert (= bench-data (thaw-bb     data-bb)))

    (let [time-freeze-stream (enc/bench laps {:warmup-laps warmup} (freeze-stream))
          time-thaw-stream   (enc/bench laps {:warmup-laps warmup} (thaw-stream data-stream))
          time-freeze-bb     (enc/bench laps {:warmup-laps warmup} (freeze-bb))
          time-thaw-bb       (enc/bench laps {:warmup-laps warmup} (thaw-bb data-bb))

          stream-round (+ time-freeze-stream time-thaw-stream)
          bb-round     (+ time-freeze-bb     time-thaw-bb)

          results
          {:nippy/low-level-stream
           {:round  stream-round
            :freeze time-freeze-stream
            :thaw   time-thaw-stream
            :size   (count data-stream)}

           :nippy/low-level-byte-buffer
           {:round  bb-round
            :freeze time-freeze-bb
            :thaw   time-thaw-bb
            :size   (count data-bb)}

           :speedup
           {:round  (enc/round2 (/ stream-round      bb-round))
            :freeze (enc/round2 (/ time-freeze-stream time-freeze-bb))
            :thaw   (enc/round2 (/ time-thaw-stream   time-thaw-bb))}}]

      (println "\nLow-level stream vs byte-buffer benchmark done:")
      (printed-results results))))

(defn bench-serialization-data
  "Like `bench-serialization` but returns results without printing."
  [{:keys [all? reader? fressian? lzma2? laps warmup bench-data]
    :as   opts
    :or
    {laps   1e4
     warmup 25e3}}]

  (let [results_ (atom {})]
    (when (or all? reader?)
      (swap! results_ assoc :reader
        (bench1-serialization freeze-reader thaw-reader
          (fn [^String s] (count (.getBytes s "UTF-8")))
          (assoc opts :laps laps, :warmup warmup))))

    (when (or all? fressian?)
      (swap! results_ assoc :fressian
        (bench1-serialization freeze-fress thaw-fress count
          (assoc opts :laps laps, :warmup warmup))))

    (when (or all? lzma2?)
      (swap! results_ assoc :nippy/lzma2
        (bench1-serialization
          #(nippy/freeze % {:compressor nippy/lzma2-compressor})
          #(nippy/thaw   % {:compressor nippy/lzma2-compressor})
          count
          (assoc opts :laps laps, :warmup warmup))))

    (swap! results_ assoc :nippy/encrypted
      (bench1-serialization
        #(nippy/freeze % {:password [:cached "p"]})
        #(nippy/thaw   % {:password [:cached "p"]})
        count
        (assoc opts :laps laps, :warmup warmup)))

    (swap! results_ assoc :nippy/default
      (bench1-serialization nippy/freeze nippy/thaw count
        (assoc opts :laps laps, :warmup warmup)))

    (swap! results_ assoc :nippy/fast
      (bench1-serialization nippy/fast-freeze nippy/fast-thaw count
        (assoc opts :laps laps, :warmup warmup)))

    @results_))

(defn bench-serialization
  [{:keys [all? reader? fressian? lzma2? laps warmup bench-data]
    :as   opts
    :or
    {laps   1e4
     warmup 25e3}}]

  (println "\nRunning benchmarks...")

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

(defn bench-compare
  "Compares current branch vs released nippy v3.6.0.

  Spawns a subprocess using the :bench-v360 Lein profile (which swaps the
  current nippy source for the released JAR) to obtain reference numbers.

  Options are the same as `bench-serialization`."
  [opts]
  (println "\nBenchmarking current branch...")
  (let [current (bench-serialization-data opts)

        _ (println "\nBenchmarking released nippy v3.6.0 (subprocess)...")
        proc    (-> (ProcessBuilder.
                      ["lein" "with-profile" "+bench-v360"
                       "run" "-m" "taoensso.nippy-bench-runner"])
                    (.directory (java.io.File. (System/getProperty "user.dir")))
                    .start)
        ;; Let the subprocess print its progress/warnings to our stderr
        _       (future (with-open [r (java.io.BufferedReader.
                                        (java.io.InputStreamReader.
                                          (.getErrorStream proc)))]
                          (doseq [line (line-seq r)]
                            (binding [*out* *err*] (println line)))))
        edn-out (slurp (.getInputStream proc))
        _       (.waitFor proc)
        released (clojure.edn/read-string edn-out)

        ks (sort (keys current))
        pad (fn [s n] (let [s (str s)] (str s (apply str (repeat (- n (count s)) " ")))))]

    (println "\n=== Serialization benchmark: current branch vs v3.6.0 ===")
    (println "(times in µs, lower is better; speedup = v3.6.0/current, higher means current is faster)")
    (println)
    (printf "%-22s  %8s  %8s  %8s  %8s  %8s  %8s  %8s%n"
      "" "cur-frz" "rls-frz" "cur-thw" "rls-thw" "cur-rnd" "rls-rnd" "speedup")
    (println (apply str (repeat 90 "-")))
    (doseq [k ks]
      (let [c (get current  k)
            r (get released k)]
        (when (and c r)
          (printf "%-22s  %8d  %8d  %8d  %8d  %8d  %8d  %8.2fx%n"
            k
            (:freeze c) (:freeze r)
            (:thaw   c) (:thaw   r)
            (:round  c) (:round  r)
            (double (/ (:round r) (:round c)))))))
    (println)
    {:current  current
     :released released}))

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
        time-compress   (enc/bench laps {:warmup-laps warmup} (compr/compress   compressor data-frozen))
        time-decompress (enc/bench laps {:warmup-laps warmup} (compr/decompress compressor data-compressed))]

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

  ;; Compare current branch vs released v3.6.0 (runs a subprocess):
  (bench-compare {})

  ;; Single-version serialization benchmarks:
  (bench-serialization {:all? true})

  {:last-updated    "2024-01-16"
   :system          "2020 Macbook Pro M1, 16 GB memory"
   :clojure-version "1.11.1"
   :java-version    "OpenJDK 21"
   :deps
   '[[com.taoensso/nippy        "3.4.0-beta1"]
     [org.clojure/tools.reader  "1.3.7"]
     [org.clojure/data.fressian "1.0.0"]
     [org.tukaani/xz            "1.9"]
     [io.airlift/aircompressor  "0.25"]]}

  (bench-serialization {:all? true})

  {:reader          {:round 13496, :freeze 5088, :thaw 8408, :size 15880}
   :fressian        {:round 3898,  :freeze 2350, :thaw 1548, :size 12222}
   :nippy/lzma2     {:round 12341, :freeze 7809, :thaw 4532, :size 3916}
   :nippy/encrypted {:round 2939,  :freeze 1505, :thaw 1434, :size 8547}
   :nippy/default   {:round 2704,  :freeze 1330, :thaw 1374, :size 8519}
   :nippy/fast      {:round 2425,  :freeze 1117, :thaw 1308, :size 17088}}

  (enc/round2 (/ 2704 13496)) ; 0.20 of reader   roundtrip time
  (enc/round2 (/ 2704  3898)) ; 0.69 of fressian roundtrip time

  (enc/round2 (/ 8519 15880)) ; 0.54 of reader   output size
  (enc/round2 (/ 8519 12222)) ; 0.70 of fressian output size

  (bench-byte-buffer-low-level {:laps 1e4 :warmup 2e4})
  {:nippy/low-level-stream      {:round 628, :freeze 277, :thaw 351, :size 17098}
   :nippy/low-level-byte-buffer {:round 496, :freeze 221, :thaw 275, :size 17098}
   :speedup                     {:round 1.27, :freeze 1.25, :thaw 1.28}}

  (bench-compressors
    {:laps 1e4 :warmup 2e4}
    {:laps 1e2 :warmup 2e2})

  ;; Note that ratio depends on compressibility of stress data
  {:lz4                {:round 293,  :compress 234,  :decompress 59,  :ratio 0.5}
   :lzo                {:round 483,  :compress 349,  :decompress 134, :ratio 0.46}
   :snappy/prepended   {:round 472,  :compress 296,  :decompress 176, :ratio 0.43}
   :snappy/unprepended {:round 420,  :compress 260,  :decompress 160, :ratio 0.43}
   :zstd/prepended     {:round 2105, :compress 1419, :decompress 686, :ratio 0.3}
   :zstd/unprepended   {:round 1261, :compress 921,  :decompress 340, :ratio 0.3}
   :lzma2/level0       {:round 158,  :compress 121,  :decompress 37,  :ratio 0.24}
   :lzma2/level3       {:round 536,  :compress 436,  :decompress 100, :ratio 0.22}
   :lzma2/level6       {:round 1136, :compress 1075, :decompress 61,  :ratio 0.21}
   :lzma2/level9       {:round 2391, :compress 2096, :decompress 295, :ratio 0.21}})

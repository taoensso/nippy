(ns taoensso.nippy-bench-runner
  "Entry point for the :bench-v360 subprocess used by bench-compare.
  Runs the serialization benchmark and prints results as EDN to stdout.
  Progress messages go to stderr so the parent can parse stdout cleanly."
  (:require [taoensso.nippy-benchmarks :as b]))

(defn -main [& _]
  (binding [*out* *err*]
    (println "Running released nippy v3.6.0 benchmark..."))
  (prn (b/bench-serialization-data {})))

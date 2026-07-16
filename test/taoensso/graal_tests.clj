(ns taoensso.graal-tests
  (:require [taoensso.nippy :as nippy])
  (:gen-class))

(defprotocol IGraalType (graal-values [x]))
(deftype      GraalType [x ^:unsynchronized-mutable y]
  IGraalType
  (graal-values [_] [x y]))

(defn- check! [pred message] (when-not pred (throw (ex-info message {}))))

(defn -main [& args]
  (let [x
        {:type    (GraalType. :public :mutable)
         :ints    (int-array    [1 2 3])
         :longs   (long-array   [4 5 6])
         :floats  (float-array  [1.25 2.5])
         :doubles (double-array [3.75 4.5])}

        y (nippy/fast-thaw (nippy/fast-freeze x))]

    (check! (= [:public :mutable] (graal-values (:type    y))) "Roundtrip failed: deftype")
    (check! (= [1 2 3]            (vec          (:ints    y))) "Roundtrip failed: int-array")
    (check! (= [4 5 6]            (vec          (:longs   y))) "Roundtrip failed: long-array")
    (check! (= [1.25 2.5]         (vec          (:floats  y))) "Roundtrip failed: float-array")
    (check! (= [3.75 4.5]         (vec          (:doubles y))) "Roundtrip failed: double-array")
    (println "Nippy native-image freeze/thaw passed")))

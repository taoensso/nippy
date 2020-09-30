(ns taoensso.nippy.java-time
  (:require [taoensso.nippy :as nippy])
  (:import [java.time Instant]))


(nippy/extend-freeze
 Instant :java.time/instant
 [^Instant instant out]
 (nippy/-freeze-without-meta! [(.getEpochSecond instant)
                               (long (.getNano instant))] out))


(nippy/extend-thaw
 :java.time/instant
 [in]
 (let [[seconds nanos] (nippy/thaw-from-in! in)]
   (Instant/ofEpochSecond seconds nanos)))


(comment
  (let [inst (Instant/now)]
    (= inst (nippy/thaw (nippy/freeze inst))))
  )

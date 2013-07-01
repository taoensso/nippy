(ns taoensso.nippy.tools
  "Alpha - subject to change.
  Utilities for third-party tools that want to add fully-user-configurable Nippy
  support. Used by Carmine and Faraday."
  {:author "Peter Taoussanis"}
  (:require [taoensso.nippy       :as nippy]
            [taoensso.nippy.utils :as utils]))

(defrecord WrappedForFreezing [value opts])
(defn wrapped-for-freezing? [x] (instance? WrappedForFreezing x))
(defn wrap-for-freezing
  "Wraps arg (any freezable data type) so that (tools/freeze <wrapped-arg>)
  will serialize the arg using given options."
  [value & [opts]] (->WrappedForFreezing value opts))

(defn freeze
  "Like `nippy/freeze` but takes options from special argument wrapper when
  present."
  [x & [{:keys [default-opts]}]]
  (if (wrapped-for-freezing? x)
    (nippy/freeze (:value x) (or (:opts x) default-opts))
    (nippy/freeze x default-opts)))

(comment (freeze (wrap-for-freezing "wrapped"))
         (freeze "unwrapped"))

(defrecord EncryptedFrozen [ba])
(defn encrypted-frozen? [x] (instance? EncryptedFrozen x))

(def ^:dynamic *thaw-opts* nil)
(defmacro with-thaw-opts
  "Evaluates body using given options for any automatic deserialization in
  context."
  [opts & body] `(binding [*thaw-opts* ~opts] ~@body))

(defn thaw
  "Like `nippy/thaw` but takes options from *thaw-opts* binding, and wraps
  encrypted bytes for easy identification when no password has been provided
  for decryption."
  [ba & {:keys [default-opts]}]
  (let [result (nippy/thaw ba (merge (or *thaw-opts* default-opts)
                                     {:taoensso.nippy/tools-thaw? true}))]
    (if (= result :taoensso.nippy/need-password)
      (EncryptedFrozen. ba)
      result)))

(comment (thaw (nippy/freeze "c" {:password [:cached "p"]}))
         (with-thaw-opts {:password [:cached "p"]}
           (thaw (nippy/freeze "c" {:password [:cached "p"]}))))
(ns taoensso.nippy.tools
  "Utilities for third-party tools that want to add fully-user-configurable
  Nippy support. Used by Carmine and Faraday."
  {:author "Peter Taoussanis"}
  (:require [taoensso.nippy :as nippy]))

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

(def ^:dynamic *thaw-opts* nil)
(defmacro with-thaw-opts
  "Evaluates body using given options for any automatic deserialization in
  context."
  [opts & body] `(binding [*thaw-opts* ~opts] ~@body))

(defn thaw "Like `nippy/thaw` but takes options from *thaw-opts* binding."
  [ba & [{:keys [default-opts]}]]
  (nippy/thaw ba (merge default-opts *thaw-opts*)))

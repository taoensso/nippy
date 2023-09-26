(ns taoensso.nippy.tools
  "Utils for community tools that want to add user-configurable Nippy support.
  Used by Carmine, Faraday, etc."
  (:require
   [taoensso.encore :as enc]
   [taoensso.nippy  :as nippy]))

(def ^:dynamic *freeze-opts* nil)
(def ^:dynamic *thaw-opts*   nil)

(do
  (defmacro with-freeze-opts  [opts & body] `(binding [*freeze-opts*                               ~opts ] ~@body))
  (defmacro with-freeze-opts+ [opts & body] `(binding [*freeze-opts* (enc/fast-merge *freeze-opts* ~opts)] ~@body))
  (defmacro with-thaw-opts    [opts & body] `(binding [*thaw-opts*                                 ~opts ] ~@body))
  (defmacro with-thaw-opts+   [opts & body] `(binding [*thaw-opts*   (enc/fast-merge *thaw-opts*   ~opts)] ~@body)))

(deftype WrappedForFreezing [val opts])
(defn wrapped-for-freezing? [x] (instance? WrappedForFreezing x))

(defn wrap-for-freezing
  "Captures (merge `tools/*thaw-opts*` `wrap-opts`), and returns
    the given argument in a wrapped form so that `tools/freeze` will
    use the captured options when freezing the wrapper argument.

    See also `tools/freeze`."
  ([x          ] (wrap-for-freezing x nil))
  ([x wrap-opts]
   (let [captured-opts (enc/fast-merge *freeze-opts* wrap-opts)] ; wrap > dynamic
     (if (instance? WrappedForFreezing x)
       (let [^WrappedForFreezing x x]
         (if (= (.-opts x) captured-opts)
           x
           (WrappedForFreezing. (.-val x) captured-opts)))
       (WrappedForFreezing.            x  captured-opts)))))

(defn freeze
  "Like `nippy/freeze` but uses as options the following, merged in
    order of ascending preference:

      1. `default-opts` given to this fn            (default nil).
      2. `tools/*freeze-opts*` dynamic value        (default nil).
      3. Opts captured by `tools/wrap-for-freezing` (default nil).

    See also `tools/wrap-for-freezing`."
  ([x             ] (freeze x nil))
  ([x default-opts]
   (let [default-opts (get default-opts :default-opts default-opts) ; Back compatibility
         active-opts  (enc/fast-merge default-opts *freeze-opts*)] ; dynamic > default

     (if (instance? WrappedForFreezing x)
       (let [^WrappedForFreezing x x]
         (nippy/freeze (.-val x) (enc/fast-merge active-opts (.-opts x)))) ; captured > active!
       (nippy/freeze          x                  active-opts)))))

(defn thaw
  "Like `nippy/thaw` but uses as options the following, merged in
    order of ascending preference:

      1. `default-opts` given to this fn   (default nil).
      2. `tools/*thaw-opts*` dynamic value (default nil)."
  ([ba             ] (thaw ba nil))
  ([ba default-opts]
   (let [default-opts (get default-opts :default-opts default-opts) ; Back compatibility
         active-opts  (enc/fast-merge default-opts *thaw-opts*)] ; dynamic > default

     (nippy/thaw ba active-opts))))

(comment (thaw (freeze (wrap-for-freezing "wrapped"))))

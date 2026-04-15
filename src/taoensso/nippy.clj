(ns taoensso.nippy
  "High-performance serialization library for Clojure."
  {:author "Peter Taoussanis (@ptaoussanis)"}
  (:require
   [clojure.java.io :as jio]
   [taoensso.truss  :as truss]
   [taoensso.encore :as enc]
   [taoensso.nippy
    [impl        :as impl]
    [schema      :as sc]
    [io          :as io]
    [compression :as compression]
    [encryption  :as encryption]])

  (:import
   [taoensso.nippy.io ByteBufferReader]
   [java.nio.charset StandardCharsets]
   [java.nio ByteBuffer]
   [java.io
    DataOutput       DataInput
    DataOutputStream DataInputStream
    ByteArrayOutputStream ByteArrayInputStream]))

(enc/assert-min-encore-version [3 160 1])

(comment
  (set! *warn-on-reflection* true)
  (set! *unchecked-math* :warn-on-boxed)
  (set! *unchecked-math* false)
  (thaw (freeze (stress-data {:comparable? false}))))

;;;;

(enc/defaliases ; ns imports for user convenience
  compression/compress
  compression/decompress
  compression/zstd-compressor
  compression/lz4-compressor
  compression/lz4hc-compressor
  compression/snappy-compressor
  compression/lzma2-compressor

  encryption/encrypt
  encryption/decrypt

  encryption/aes128-gcm-encryptor
  encryption/aes128-cbc-encryptor
  encryption/aes128-gcm-encryptor
  {:src encryption/aes128-gcm-encryptor, :alias aes128-encryptor})

(def public-types-spec
  "Public representation of Nippy's internal type schema.
  For use by tooling and advanced users.

  **HIGHLY EXPERIMENTAL!**
  Subject to breaking change without notice.
  Currently completely untested, may contain bugs!
  Intended for use only by early adopters to give design feedback.

  Format:
    {<type-id> {:keys [type-id type-kw payload-spec deprecated?]}},

    - `type-id`: A +ive single-byte identifier like `110`.
                 -ive type ids are reserved for custom user-defined types.

    - `type-kw`: A keyword like `:kw-sm`,
      suffixes used to differentiate subtypes of different sizes:
        -0  ; Empty       => 0 byte         payload / element-count
        -sm ; Small       => 1 byte (byte)  payload / element-count
        -md ; Medium      => 2 byte (short) payload / element-count
        -lg ; Large       => 4 byte (int)   payload / element-count
        -xl ; Extra large => 8 byte (long)  payload / element-count

    - `payload-spec` examples:
      - nil                        ; No spec available (e.g. unpredictable payload)
      - []                         ; Type has no payload
      - [[:bytes 4]]               ; Type has    payload of exactly 4 bytes
      - [[:bytes 2] [:elements 2]] ; Type has    payload of exactly 2 bytes,
                                   ; followed by 2 elements

      - [[:bytes    {:read 2}]
         [:elements {:read 4 :multiplier 2 :unsigned? true}]]

        ; Type has payload of <short-count> bytes, followed by
        ; <unsigned-int-count>*2 (multiplier) elements

      Note that `payload-spec` can be handy for skipping over items in
      data stream without fully reading every item."

  ;; TODO Add unit tests for size data once API is finalized

  (reduce-kv
    (fn [m type-id [type-kw ?payload-spec]]
      (assoc m type-id
        (enc/assoc-when
          {:type-id type-id
           :type-kw type-kw}

          :payload-spec ?payload-spec
          :deprecated? (enc/str-ends-with? (name type-kw) "_"))))

    sc/types-spec
    sc/types-spec))

;;;; Dynamic config (see also `nippy.tools` ns!)

;; For back compatibility (incl. Timbre's Carmine appender)
(enc/defonce ^:dynamic ^:no-doc ^:deprecated *final-freeze-fallback* "Prefer `*freeze-fallback`." nil)
(enc/defonce ^:dynamic                             *freeze-fallback*
  "Controls Nippy's behaviour when trying to freeze an object with a type for
  which Nippy doesn't currently have a native (protocol) implementation.

  Possible values:

    1. `nil` (no fallback, default)
       Tries the following in order:
         - Freeze with Java's `Serializable` interface if this seems possible
         - Freeze with Clojure's reader                if this seems possible
         - Throw

    2. `:write-unfreezable` keyword
       Tries the following in order:
         - Freeze with Java's `Serializable` interface if this seems possible
         - Freeze with Clojure's reader                if this seems possible
         - Freeze a {:nippy/unfreezable {:type _}} placeholder value

    3. [Advanced] Custom (fn [^java.io.DataOutput dout obj]) that must
        write an appropriate object type id and payload to the given
       `DataOutput` stream."

  nil)

(enc/defonce ^:dynamic *custom-readers* "{<hash-or-byte-id> (fn [^DataInput din])->read}" nil)
(enc/defonce ^:dynamic *auto-freeze-compressor*
  "(fn [byte-array])->compressor used by `(freeze <x> {:compressor :auto}),
  nil => default"
  nil)

(enc/defonce ^:dynamic *incl-metadata?* "Include metadata when freezing/thawing?" true)

(enc/defonce ^:dynamic *thaw-xform*
  "Experimental, subject to change. Feedback welcome!

  Transducer to use when thawing standard Clojure collection types
  (vectors, maps, sets, lists, etc.).

  Allows fast+flexible inspection and manipulation of data being thawed.

  Key-val style data structures like maps will provide `MapEntry` args
  to reducing function. Use `map-entry?`, `key`, `val` utils for these.

  Example transducers:

    (map (fn [x] (println x) x)) ; Print each coll item thawed

    (comp
      (map    (fn [x] (if (= x :secret) :redacted x))) ; Replace secrets
      (remove (fn [x] ; Remove maps with a truthy :remove?
                (or
                  (and (map?       x) (:remove? x))
                  (and (map-entry? x) (= (key x) :remove?) (val y)))))))

  Note that while this is a very powerful feature, correctly writing
  and debugging transducers and reducing fns can be tricky.

  To help, if Nippy encounters an error while applying your xform, it
  will throw a detailed `ExceptionInfo` with message
  \"Error thrown via `*thaw-xform*`\" to help you debug."

  {:added "v3.3.0-RC1 (2023-08-02)"}
  nil)

(comment
  (binding [*thaw-xform*
            (comp
              (map (fn [x] (println x) x))
              (map (fn [x] (if (= x 1) 0 x)))
              (map (fn [x] (/ 1 0))))]

    (thaw (freeze [1 1 0 1 1]))))

;;;; Java Serializable config
;; Unfortunately quite complex to do this safely

(def default-freeze-serializable-allowlist
  "Allows *any* class name to be frozen using Java's `Serializable` interface.
  This is generally safe since RCE risk is present only when thawing.
  See also `*freeze-serializable-allowlist*`."
  #{"*"})

(def default-thaw-serializable-allowlist
  "A set of common safe class names to allow to be frozen using Java's
  `Serializable` interface. PRs welcome for additions.
  See also `*thaw-serializable-allowlist*`."
  #{"[Z" "[B" "[S" "[I" "[J" "[F" "[D" "[C" "[Ljava.lang.String;"

    "java.lang.Throwable"
    "java.lang.Exception"
    "java.lang.RuntimeException"
    "java.lang.ArithmeticException"
    "java.lang.IllegalArgumentException"
    "java.lang.NullPointerException"
    "java.lang.IndexOutOfBoundsException"
    "java.lang.ClassCastException"

    "java.net.URI"
    ;; "java.util.UUID" ; Unnecessary (have native Nippy implementation)
    ;; "java.util.Date" ; ''
    ;; "java.sql.Date"  ; ''

    #_"java.time.*" ; Safe?
    "java.time.Clock"
    "java.time.LocalDate"
    "java.time.LocalDateTime"
    "java.time.LocalTime"
    "java.time.MonthDay"
    "java.time.OffsetDateTime"
    "java.time.OffsetTime"
    "java.time.Year"
    "java.time.YearMonth"
    "java.time.ZonedDateTime"
    "java.time.ZoneId"
    "java.time.ZoneOffset"
    "java.time.DateTimeException"
    
    "org.joda.time.DateTime"

    "clojure.lang.ExceptionInfo"
    "clojure.lang.ArityException"})

(let [doc
      "Used when attempting to <freeze/thaw> an object that:
    - Does NOT implement Nippy's native freeze  protocol.
    - DOES     implement Java's  `Serializable` interface.

  In this case, an allowlist will be checked to see if Java's
  `Serializable` interface may be used.

  This is a security measure to prevent possible Remote Code Execution
  (RCE) when thawing malicious payloads. See [1] for details.

  If `freeze` encounters a disallowed `Serializable` class, it will throw.
  If `thaw`   encounters a disallowed `Serializable` class, it will:

    - Throw if it's not possible to safely quarantine the object
      (object was frozen with Nippy < v2.15.0-final).

    - Otherwise it will return a safely quarantined object of form
      `{:nippy/unthawable {:class-name <> :content <quarantined-ba>}}`.
      - Quarantined objects may be manually unquarantined with
        `read-quarantined-serializable-object-unsafe!`.

  There are 2x allowlists:
    - `*freeze-serializable-allowlist*` ; Checked when freezing
    -   `*thaw-serializable-allowlist*` ; Checked when thawing

  Example allowlist values:
    - `(fn allow-class? [class-name] true)`            ; Arbitrary predicate fn
    - `#{\"java.lang.Throwable\", \"clojure.lang.*\"}` ; Set of class names
    - `\"allow-and-record\"`                           ; Special value, see [2]

    Note that class names in sets may contain \"*\" wildcards.

  Default allowlist values are:
    - default-freeze-serializable-allowlist ; `{\"*\"}` => allow any class
    -   default-thaw-serializable-allowlist ; A set of common safe classes

  Allowlist values may be overridden with `binding`, `alter-var-root`, or:

    - `taoensso.nippy.<freeze/thaw>-serializable-allowlist-base` JVM property value
    - `taoensso.nippy.<freeze/thaw>-serializable-allowlist-add`  JVM property value

    - `TAOENSSO_NIPPY_<FREEZE/THAW>_SERIALIZABLE_ALLOWLIST_BASE` Environment variable value
    - `TAOENSSO_NIPPY_<FREEZE/THAW>_SERIALIZABLE_ALLOWLIST_ADD`  Environment variable value

  If present, these will be read as comma-separated lists of class names
  and formed into sets. Each initial allowlist value will then be:
  (into (or <?base> <default>) <?additions>).

    I.e. you can use:
      - The \"base\" property/var to REPLACE Nippy's default allowlists.
      - The \"add\"  property/var to ADD TO  Nippy's default allowlists.

  The special `\"allow-and-record\"` value is also possible, see [2].

  Upgrading from an older version of Nippy and unsure whether you've been
  using Nippy's `Serializable` support, or which classes to allow? See [2].

  See also `taoensso.encore/name-filter` for a util to help easily build
  more advanced predicate functions.

  Thanks to Timo Mihaljov (@solita-timo-mihaljov) for an excellent report
  identifying this vulnerability.

  [1] https://github.com/ptaoussanis/nippy/issues/130
  [2] See `allow-and-record-any-serializable-class-unsafe`."]

  (enc/defonce ^{:dynamic true :doc doc} *freeze-serializable-allowlist*
    (impl/parse-allowlist default-freeze-serializable-allowlist
      (enc/get-env :taoensso.nippy.freeze-serializable-allowlist-base)
      (enc/get-env :taoensso.nippy.freeze-serializable-allowlist-add)))

  (enc/defonce ^{:dynamic true :doc doc} *thaw-serializable-allowlist*
    (impl/parse-allowlist default-thaw-serializable-allowlist
      (enc/get-env
        [:taoensso.nippy.thaw-serializable-allowlist-base
         :taoensso.nippy.serializable-whitelist-base ; Back compatibility
         ])
      (enc/get-env
        [:taoensso.nippy.thaw-serializable-allowlist-add
         :taoensso.nippy.serializable-whitelist-add ; Back compatibility
         ]))))

(enc/defonce ^:dynamic ^:no-doc ^:deprecated *serializable-whitelist*
  ;; Back compatibility for Crux, Ref. <https://github.com/juxt/crux/releases/tag/20.09-1.11.0>
  "Prefer `*thaw-serializable-allowlist*`." nil)

(enc/defaliases
  impl/allow-and-record-any-serializable-class-unsafe
  impl/get-recorded-serializable-classes)

(defn- call-with-bindings
  "Allow opts to override config bindings."
  [action opts f]
  (if (empty? opts)
    (f)
    (let [opt->bindings
          (fn [bindings id var]
            (let [v (get opts id :default)]
              (if (identical? v :default)
                (do    bindings)
                (assoc bindings var v))))

          bindings
          (-> nil
            (opt->bindings :freeze-fallback        #'*freeze-fallback*)
            (opt->bindings :final-freeze-fallback  #'*final-freeze-fallback*)
            (opt->bindings :auto-freeze-compressor #'*auto-freeze-compressor*)
            (opt->bindings :custom-readers         #'*custom-readers*)
            (opt->bindings :incl-metadata?         #'*incl-metadata?*)
            (opt->bindings :thaw-xform             #'*thaw-xform*)
            (opt->bindings :serializable-allowlist
              (case action
                :freeze #'*freeze-serializable-allowlist*
                :thaw     #'*thaw-serializable-allowlist*)))]

      (if-not bindings
        (f) ; Common case
        (try
          (push-thread-bindings bindings)
          (f)
          (finally
            (pop-thread-bindings)))))))

(comment
  (enc/qb 1e5 ; [7.28 16.74]
    (call-with-bindings :freeze {}                       (fn [] *freeze-fallback*))
    (call-with-bindings :freeze {:freeze-fallback "foo"} (fn [] *freeze-fallback*))))

;;;; Freeze API

(defn- freeze-raw ^bytes [x] (io/with-bb 512 (fn [bb dout_] (io/write-typed+meta x bb dout_) true)))
(defn  freeze
  "Main freezing util.

  Serializes given arg (any Clojure data type) to a byte array.
  To freeze custom types, extend the Clojure reader or see `extend-freeze`."

  (^bytes [x] (freeze x nil))
  (^bytes
   [x
    {:as   opts
     :keys [compressor encryptor password serializable-allowlist incl-metadata?]
     :or   {compressor :auto
            encryptor  aes128-gcm-encryptor}}]

   (call-with-bindings :freeze opts
     (fn []
       (let [no-header? (or (get opts :no-header?) (get opts :skip-header?)) ; Undocumented
             encryptor  (when password encryptor)
             ^bytes ba  (impl/with-cache (freeze-raw x))]

         (if (and (nil? compressor) (nil? encryptor))
           (if no-header?
             (do             ba)
             (sc/wrap-header ba {:compressor-id nil :encryptor-id nil}))

           (let [compressor
                 (if (identical? compressor :auto)
                   (if no-header?
                     lz4-compressor
                     (if-let [fc *auto-freeze-compressor*]
                       (fc ba)
                       ;; Intelligently enable compression only if benefit
                       ;; is likely to outweigh cost:
                       (when (> (alength ba) 8192) lz4-compressor)))

                   (if (fn? compressor)
                     (compressor ba) ; Assume compressor selector fn
                     compressor      ; Assume compressor
                     ))

                 ba (if compressor (compress compressor         ba) ba)
                 ba (if encryptor  (encrypt  encryptor password ba) ba)]

             (if no-header?
               (do             ba)
               (sc/wrap-header ba
                 {:compressor-id (when-let [c compressor] (or (compression/standard-header-ids (compression/header-id c)) :else))
                  :encryptor-id  (when-let [e encryptor]  (or (encryption/standard-header-ids  (encryption/header-id  e)) :else))})))))))))

(defn fast-freeze
  "Like `freeze` but:
    - Writes data WITHOUT a Nippy header.
    - No support for compression or encryption.
    - Must be thawed with `fast-thaw`.

  Equivalent to (but a little faster than) `freeze` with opts:
    {:no-header? true, :compressor nil, :encryptor nil}.

  Intended for use only by advanced users that clearly understand the tradeoffs.
  I STRONGLY recommend that most users prefer the standard `freeze` since:
    - The Nippy header is useful for data portability and preservation
    - Compression is often benefitial at little/no cost
    - The performance difference between `freeze` and `fast-freeze` is
      often negligible in practice."

  ^bytes [x] (impl/with-cache (freeze-raw x)))

(defn freeze-to-out!
  "Low-level util. Serializes given arg (any Clojure data type) to given `DataOutput`.
  In most cases you want `freeze` instead."
  [^DataOutput dout x] (let [ba (freeze-raw x)] (.write dout ba 0 (alength ba))))

(defn freeze-to-bb!
  "Low-level util. Serializes given arg (any Clojure data type) to given `ByteBuffer`.
  In most cases you want `freeze` instead."
  [^ByteBuffer bb x]
  (let [bb    (io/assert-big-endian-bb bb)
        dout_ (let [v_ (volatile! nil)] (fn [] (or @v_ (vreset! v_ (io/bb->dout bb)))))]
    (try
      (io/write-typed+meta x bb dout_)
      (catch java.nio.BufferOverflowException _
        (throw
          (java.io.EOFException.
            (str "ByteBuffer overflow while freezing: remaining " (.remaining bb) " bytes.")))))))

(defn freeze-to-string
  "Like `freeze`, but returns a Base64-encoded string.
  See also `thaw-from-string`."
  ([x            ] (freeze-to-string x nil))
  ([x freeze-opts]
   (let [ba (freeze x freeze-opts)]
     (.encodeToString (java.util.Base64/getEncoder)
       ba))))

(defn freeze-to-file
  "Like `freeze`, but writes to `(clojure.java.io/file <file>)`."
  ([file x            ] (freeze-to-file file x nil))
  ([file x freeze-opts]
   (let [^bytes ba (freeze x freeze-opts)]
     (with-open [os (jio/output-stream (jio/file file))]
       (.write os ba))
     ba)))

;;;; Thaw API

(def ^:private err-msg-unknown-thaw-failure "Possible decryption/decompression error, unfrozen/damaged data, etc.")
(def ^:private err-msg-unrecognized-header  "Unrecognized (but apparently well-formed) header. Data frozen with newer Nippy version?")

(declare thaw-from-bb*)

(defn thaw
  "Main thawing util.

  Deserializes given frozen Nippy byte array to its original Clojure data type.
  To thaw custom types, extend the Clojure reader or see `extend-thaw`.

  Opts include:
    `:v1-compatibility?` - support data frozen by VERY old Nippy versions? (default false)
    `:compressor` - :auto (checks header, default)  an ICompressor, or nil
    `:encryptor`  - :auto (checks header, default), an IEncryptor,  or nil"

  ([ba] (thaw ba nil))
  ([^bytes ba
    {:as   opts
     :keys [v1-compatibility? compressor encryptor password
            serializable-allowlist incl-metadata? thaw-xform]
     :or   {compressor :auto
            encryptor  :auto}}]

   (assert (not (get opts :headerless-meta))
     ":headerless-meta `thaw` opt removed in Nippy v2.7+")

   (call-with-bindings :thaw opts
     (fn []
       (let [v2+?       (not v1-compatibility?)
             no-header? (get opts :no-header?) ; Intentionally undocumented
             ex
             (fn ex
               ([  msg] (ex nil msg))
               ([e msg]
                (truss/ex-info! (str "Thaw failed. " msg)
                  {:opts
                   (assoc opts
                     :compressor compressor
                     :encryptor  encryptor)

                   :bindings
                   (enc/assoc-some {}
                     {'*freeze-fallback*             *freeze-fallback*
                      '*final-freeze-fallback*       *final-freeze-fallback*
                      '*auto-freeze-compressor*      *auto-freeze-compressor*
                      '*custom-readers*              *custom-readers*
                      '*incl-metadata?*              *incl-metadata?*
                      '*thaw-serializable-allowlist* *thaw-serializable-allowlist*
                      '*thaw-xform*                  *thaw-xform*})}
                  e)))

             thaw-data
             (fn [data-ba compressor-id encryptor-id ex-fn]
               (let [compressor (if (identical? compressor :auto) (compression/get-auto-compressor compressor-id) compressor)
                     encryptor  (if (identical? encryptor  :auto) (encryption/get-auto-encryptor   encryptor-id)  encryptor)]

                 (when (and encryptor (not password))
                   (ex "Password required for decryption."))

                 (try
                   (let [ba data-ba
                         ba (if encryptor  (decrypt    encryptor password ba) ba)
                         ba (if compressor (decompress compressor         ba) ba)]
                     (impl/with-cache (thaw-from-bb* (ByteBuffer/wrap ba))))
                   (catch Exception e (ex-fn e)))))

             ;; Hacky + can actually segfault JVM due to Snappy bug,
             ;; Ref. <http://goo.gl/mh7Rpy> - no better alternatives, unfortunately
             thaw-v1-data
             (fn [data-ba ex-fn]
               (thaw-data data-ba :snappy nil
                 (fn [_] (thaw-data data-ba nil nil (fn [_] (ex-fn nil))))))]

         (if no-header?
           (if v2+?
             (thaw-data ba :no-header :no-header (fn [e]                          (ex e err-msg-unknown-thaw-failure)))
             (thaw-data ba :no-header :no-header (fn [e] (thaw-v1-data ba (fn [_] (ex e err-msg-unknown-thaw-failure))))))

           ;; At this point we assume that we have a header iff we have v2+ data
           (if-let [[data-ba {:keys [compressor-id encryptor-id unrecognized-meta?]
                              :as   head-meta}] (sc/try-parse-header ba)]

             ;; A well-formed header _appears_ to be present (it's possible though
             ;; unlikely that this is a fluke and data is actually headerless):
             (if v2+?
               (if unrecognized-meta?
                 (ex err-msg-unrecognized-header)
                 (thaw-data data-ba compressor-id encryptor-id
                   (fn [e] (ex e err-msg-unknown-thaw-failure))))

               (if unrecognized-meta?
                 (thaw-v1-data ba                              (fn [_]                          (ex   err-msg-unrecognized-header)))
                 (thaw-data data-ba compressor-id encryptor-id (fn [e] (thaw-v1-data ba (fn [_] (ex e err-msg-unknown-thaw-failure)))))))

             ;; Well-formed header definitely not present
             (if v2+?
               (ex err-msg-unknown-thaw-failure)
               (thaw-v1-data ba (fn [_] (ex err-msg-unknown-thaw-failure)))))))))))

(comment
  (thaw (freeze "hello"))
  (thaw (freeze "hello"  {:compressor nil}))
  (thaw (freeze "hello"  {:password [:salted "p"]})) ; ex: no pwd
  (thaw (freeze "hello") {:password [:salted "p"]}))

(defn fast-thaw
  "Like `thaw` but:
    - Supports only data frozen with `fast-freeze`.
    - No support for compression or encryption.

  Equivalent to (but a little faster than) `thaw` with opts:
    {:no-header? true, :compressor nil, :encryptor nil}."
  [^bytes ba] (impl/with-cache (thaw-from-bb* (ByteBuffer/wrap ba))))

(defn- thaw-from-bb* [^ByteBuffer bb] (io/read-typed (ByteBufferReader. bb)))
(defn  thaw-from-bb!
  "Low-level util. Deserializes a frozen object from given `ByteBuffer` to
  its original Clojure data type. In most cases you want `thaw` instead."
  [^ByteBuffer bb]
  (io/assert-big-endian-bb bb)
  (thaw-from-bb*           bb))

(defn thaw-from-in!
  "Low-level util. Deserializes a frozen object from given `DataInput` to
  its original Clojure data type. In most cases you want `thaw` instead."
  [^DataInput din] (io/read-typed din))

(defn thaw-from-string
  "Like `thaw`, but takes a Base64-encoded string.
  See also `freeze-to-string`."
  ([s                  ] (thaw-from-string s nil))
  ([^String s thaw-opts]
   (let [ba (.decode (java.util.Base64/getDecoder) s)]
     (thaw ba thaw-opts))))

(comment (thaw-from-string (freeze-to-string {:a :A :b [:B1 :B2]})))

(defn thaw-from-file
  "Like `thaw`, but reads from `(clojure.java.io/file <file>)`."
  ([file          ] (thaw-from-file file nil))
  ([file thaw-opts]
   (let [file (jio/file file)
         frozen-ba
         (let [ba (byte-array (.length file))]
           (with-open [dis (DataInputStream. (jio/input-stream file))]
             (.readFully dis ba)
             (do             ba)))]

     (thaw frozen-ba thaw-opts))))

(defn thaw-from-resource
  "Like `thaw`, but reads from `(clojure.java.io/resource <res>)`."
  ([res          ] (thaw-from-resource res nil))
  ([res thaw-opts]
   (let [res (jio/resource res)
         frozen-ba
         (with-open [is   (jio/input-stream res)
                     baos (ByteArrayOutputStream.)]
           (jio/copy is  baos)
           (.toByteArray baos))]

     (thaw frozen-ba thaw-opts))))

(comment
  (freeze-to-file "resources/foo.npy" "hello, world!")
  (thaw-from-file "resources/foo.npy")
  (thaw-from-resource       "foo.npy"))

;;;; Custom types

(defmacro extend-freeze
  "Extends Nippy to support freezing of a custom type (ideally concrete) with
  given id of form:

    * ℕ∈[1, 128]           - 0 byte overhead. You are responsible for managing ids.
    * (Namespaced) keyword - 2 byte overhead. Keyword will be hashed to 16 bit int,
                             collisions will throw at compile-time.

  NB: be careful about extending to interfaces, Ref. <http://goo.gl/6gGRlU>.

  (defrecord MyRec [data])
  (extend-freeze MyRec :foo/my-type [x ^DataOutput dout] ; Keyword id
    (.writeUTF [dout] (:data x)))
  ;; or
  (extend-freeze MyRec 1 [x ^DataOutput dout] ; Byte id
    (.writeUTF [dout] (:data x)))"

  [type custom-type-id [x dout] & body]
  (impl/assert-custom-type-id custom-type-id)
  (let [write-id-form
        (if (keyword? custom-type-id)
          ;; Prefixed [const byte id][cust hash id][payload]:
          `(do (.writeByte  ~dout (unchecked-byte ~sc/id-prefixed-custom-md))
               (.writeShort ~dout ~(impl/coerce-custom-type-id custom-type-id)))
          ;; Unprefixed [cust byte id][payload]:
          `(.writeByte ~dout (unchecked-byte ~(impl/coerce-custom-type-id custom-type-id))))]

    `(extend-type ~type
       impl/INativeFreezable   (~'native-freezable? [~'_] true)
       impl/ICustomFreezable   (~'custom-freezable? [~'_] true)
       io/IWriteTypedNoMetaDin (~'write-typed-din   [~x ~(with-meta dout {:tag 'java.io.DataOutput})] ~write-id-form ~@body))))

(defmacro extend-thaw
  "Extends Nippy to support thawing of a custom type with given id:
  (extend-thaw :foo/my-type [^DataInput din] ; Keyword id
    (MyRec. (.readUTF din)))
  ;; or
  (extend-thaw 1 [^DataInput din] ; Byte id
    (MyRec. (.readUTF din)))"
  [custom-type-id [din] & body]
  (impl/assert-custom-type-id custom-type-id)
  `(do
     (when (contains? *custom-readers* ~(impl/coerce-custom-type-id custom-type-id))
       (println (str "Warning: resetting Nippy thaw for custom type with id: " ~custom-type-id)))

     (alter-var-root #'*custom-readers*
       (fn     [m#]
         (assoc m# ~(impl/coerce-custom-type-id custom-type-id)
           (fn [~(with-meta din {:tag 'java.io.DataInput})] ~@body))))))

(comment
  *custom-readers*
  (defrecord MyRec [data])
  (extend-freeze MyRec 1 [x dout] (.writeUTF dout (:data x)))
  (extend-thaw         1 [din]    (MyRec. (.readUTF din)))
  (thaw (freeze (MyRec. "Joe"))))

;;;; Misc utils

(defn freezable?
  "Alpha, subject to change.
  Returns ∈ #{:native :maybe-clojure-reader :maybe-java-serializable nil},
  truthy iff Nippy seems to support freezing for the given argument.

  Important: result can be inaccurate in some cases. To be completely sure you
  unfortunately need to try freeze then thaw the argument, and check the thawed
  value.

  Options include:
    `recursive?`               - Check recursively into given arg?
    `allow-clojure-reader?`    - Allow freezing with Clojure's reader?
    `allow-java-serializable?` - Allow freezing with Java's `Serializable`?"

  ([x] (freezable? x nil))
  ([x
    {:as opts
     :keys [recursive? allow-clojure-reader? allow-java-serializable?]
     :or   {recursive? true}}]

   (or
     (and
       (impl/native-freezable? x) ; Incl. `custom-freezable?`
       (and
         (or
           (not recursive?) (not (coll? x))
           (enc/revery? #(freezable? % opts) x)))
       :native)

     (and allow-clojure-reader?    (impl/seems-readable?     x) :maybe-clojure-reader)
     (and allow-java-serializable? (impl/seems-serializable? x) :maybe-java-serializable)
     nil)))

(comment (enc/qb 1e6 (freezable? "hello"))) ; 49.76

(defn inspect-ba
  "Experimental, subject to change. Feedback welcome!"
  ([ba          ] (inspect-ba ba nil))
  ([ba thaw-opts]
   (when (enc/bytes? ba)
     (let [[first2bytes nextbytes] (enc/ba-split ba 2)
           ?known-wrapper
           (enc/cond
             (enc/ba= first2bytes (.getBytes "\u0000<" StandardCharsets/UTF_8)) :carmine/bin
             (enc/ba= first2bytes (.getBytes "\u0000>" StandardCharsets/UTF_8)) :carmine/clj)

           unwrapped-ba (if ?known-wrapper nextbytes ba)
           [data-ba ?nippy-header] (or (sc/try-parse-header unwrapped-ba)
                                       [unwrapped-ba :no-header])]

       {:?known-wrapper  ?known-wrapper
        :?header         ?nippy-header
        :thawable?       (try (thaw unwrapped-ba thaw-opts) true
                              (catch Exception _ false))
        :unwrapped-ba    unwrapped-ba
        :data-ba         data-ba
        :unwrapped-len   (alength ^bytes unwrapped-ba)
        :ba-len          (alength ^bytes ba)
        :data-len        (alength ^bytes data-ba)}))))

(comment
  (do            (inspect-ba (freeze "hello")))
  (seq (:data-ba (inspect-ba (freeze "hello")))))

(enc/defalias io/read-quarantined-serializable-object-unsafe!)

(comment
  (read-quarantined-serializable-object-unsafe!
    (thaw (freeze (java.util.concurrent.Semaphore. 1)))))

(enc/defaliases impl/cache impl/with-cache)

(comment
  (thaw (freeze [(cache "foo") (cache "foo") (cache "foo")]))
  (let [v1 (with-meta [] {:id :v1})
        v2 (with-meta [] {:id :v2})]
    (mapv meta
      (thaw (freeze [(cache v1) (cache v2) (cache v1) (cache v2)])))))

(defn ^:no-doc ^:deprecated try-write-serializable [^DataOutput dout x] (truss/catching :all (when-let [^bytes ba (io/with-bb 512 (fn [bb dout_] (io/write-sz       bb x)))] (.write dout ba 0 (alength ba)) true)))
(defn ^:no-doc ^:deprecated try-write-readable     [^DataOutput dout x] (truss/catching :all (when-let [^bytes ba (io/with-bb 512 (fn [bb dout_] (io/write-readable bb x)))] (.write dout ba 0 (alength ba)) true)))
(defn ^:no-doc ^:deprecated     write-unfreezable  [^DataOutput dout x]
  (let [x*        (impl/wrap-unfreezable x)
        ^bytes ba (io/with-bb 512 (fn [bb dout_] (io/write-typed x* bb dout_) true))]
    (.write dout ba 0 (alength ba))))

;;;; Stress data (for tests, benching, etc.)

(defrecord StressRecord [x])
(deftype   StressType [x ^:unsynchronized-mutable y]
  clojure.lang.IDeref (deref  [_]       [x y])
  Object              (equals [_ other] (and (instance? StressType other) (= [x y] @other))))

(defn stress-data
  "Returns map of reference stress data for use by tests, benchmarks, etc."
  [{:keys [comparable?] :as opts}]
  (let [rng      (java.util.Random. 123456) ; Seeded for determinism
        rand-nth (fn [coll] (nth coll (.nextInt rng (count coll))))
        base
        {:nil                   nil
         :true                  true
         :false                 false
         :false-boxed (Boolean. false)

         :char      \ಬ
         :str-short "ಬಾ ಇಲ್ಲಿ ಸಂಭವಿಸ"
         :str-long  (reduce str (range 1024))
         :kw        :keyword
         :kw-ns     ::keyword
         :sym       'foo
         :sym-ns    'foo/bar
         :kw-long   (keyword (reduce str "_" (range 128)) (reduce str "_" (range 128)))
         :sym-long  (symbol  (reduce str "_" (range 128)) (reduce str "_" (range 128)))

         :byte      (byte   16)
         :short     (short  42)
         :integer   (int    3)
         :long      (long   3)
         :float     (float  3.1415926535897932384626433832795)
         :double    (double 3.1415926535897932384626433832795)
         :bigdec    (bigdec 3.1415926535897932384626433832795)
         :bigint    (bigint  31415926535897932384626433832795)
         :ratio     22/7

         :list      (list 1 2 3 4 5 (list 6 7 8 (list 9 10 (list) ())))
         :vector    [1 2 3 4 5 [6 7 8 [9 10 [[]]]]]
         :subvec    (subvec [1 2 3 4 5 6 7 8] 2 8)
         :map       {:a 1 :b 2 :c 3 :d {:e 4 :f {:g 5 :h 6 :i 7 :j {{} {}}}}}
         :map-entry (clojure.lang.MapEntry/create "key" "val")
         :set       #{1 2 3 4 5 #{6 7 8 #{9 10 #{#{}}}}}
         :meta      (with-meta {:a :A} {:metakey :metaval})
         :nested    [#{{1 [:a :b] 2 [:c :d] 3 [:e :f]} [#{{[] ()}}] #{:a :b}}
                     #{{1 [:a :b] 2 [:c :d] 3 [:e :f]} [#{{[] ()}}] #{:a :b}}
                     [1 [1 2 [1 2 3 [1 2 3 4 [1 2 3 4 5 "ಬಾ ಇಲ್ಲಿ ಸಂಭವಿಸ"] {} #{} [] ()]]]]]

         :sorted-set     (sorted-set 1 2 3 4 5)
         :sorted-map     (sorted-map :b 2 :a 1 :d 4 :c 3)
         :lazy-seq-empty (map identity ())
         :lazy-seq       (repeatedly 64 #(do nil))
         :queue          (into clojure.lang.PersistentQueue/EMPTY [:a :b :c :d :e :f :g])
         :queue-empty          clojure.lang.PersistentQueue/EMPTY

         :uuid      (java.util.UUID. 7232453380187312026 -7067939076204274491)
         :uri       (java.net.URI. "https://clojure.org")
         :defrecord (StressRecord. "data")
         :deftype   (StressType.   "normal field" "private field")

         :util-date (java.util.Date. 1577884455500)
         :sql-date  (java.sql.Date.  1577884455500)
         :instant   (enc/compile-if java.time.Instant  (java.time.Instant/parse "2020-01-01T13:14:15.50Z") ::skip)
         :duration  (enc/compile-if java.time.Duration (java.time.Duration/ofSeconds 100 100)              ::skip)
         :period    (enc/compile-if java.time.Period   (java.time.Period/of 1 1 1)                         ::skip)

         :many-longs    (vec (repeatedly 512         #(rand-nth (range 10))))
         :many-doubles  (vec (repeatedly 512 #(double (rand-nth (range 10)))))
         :many-strings  (vec (repeatedly 512         #(rand-nth ["foo" "bar" "baz" "qux"])))
         :many-keywords (vec (repeatedly 512
                               #(keyword
                                  (rand-nth ["foo" "bar" "baz" "qux" nil])
                                  (rand-nth ["foo" "bar" "baz" "qux"    ]))))}]

    (if comparable?
      base
      (assoc base
        :non-comparable
        {:regex     #"^(https?:)?//(www\?|\?)?"
         :throwable (Throwable. "Msg")
         :exception (Exception. "Msg")
         :ex-info   (ex-info    "Msg" {:data "data"})
         :arrays
         {:boolean (boolean-array     (mapv even?  (range 32)))
          :byte    (byte-array        (mapv byte   (range 32)))
          :short   (short-array       (mapv short  (range 32)))
          :int     (int-array         (mapv int    (range 32)))
          :long    (long-array        (mapv long   (range 32)))
          :float   (float-array       (mapv float  (range 32)))
          :double  (double-array      (mapv double (range 32)))
          :char    (char-array        (mapv char   (range 32)))
          :str     (into-array String (mapv str    (range 32)))
          :object  (object-array      (mapv vector (range 32)))}}))))

(comment
  [(=      (stress-data {:comparable? true}) (stress-data {:comparable? true}))
   (let [d (stress-data {:comparable? true})] (= (thaw (freeze d)) d))])

;;;; Deprecated

(enc/deprecated
  (def  ^:no-doc ^:deprecated freeze-fallback-as-str      "Prefer `write-unfreezable`."  write-unfreezable)
  (defn ^:no-doc ^:deprecated set-freeze-fallback!        "Prefer `alter-var-root`." [x] (alter-var-root #'*freeze-fallback*        (constantly x)))
  (defn ^:no-doc ^:deprecated set-auto-freeze-compressor! "Prefer `alter-var-root`." [x] (alter-var-root #'*auto-freeze-compressor* (constantly x)))
  (defn ^:no-doc ^:deprecated swap-custom-readers!        "Prefer `alter-var-root`." [f] (alter-var-root #'*custom-readers* f))
  (defn ^:no-doc ^:deprecated swap-serializable-whitelist!
    "Prefer:
      (alter-var-root *thaw-serializable-allowlist*    f) and/or
      (alter-var-root *freeze-serializable-allow-list* f) instead."
    [f]
    (alter-var-root *freeze-serializable-allowlist* (fn [old] (f (truss/have set? old))))
    (alter-var-root *thaw-serializable-allowlist*   (fn [old] (f (truss/have set? old))))))

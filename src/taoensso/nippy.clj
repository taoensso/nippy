(ns taoensso.nippy
  "High-performance serialization library for Clojure."
  {:author "Peter Taoussanis (@ptaoussanis)"}
  (:require
   [clojure.string  :as str]
   [clojure.java.io :as jio]
   [taoensso.truss  :as truss]
   [taoensso.encore :as enc]
   [taoensso.nippy
    [impl        :as impl]
    [compression :as compression]
    [encryption  :as encryption]])

  (:import
   [java.nio ByteBuffer]
   [java.nio.charset StandardCharsets]
   [java.io ByteArrayInputStream ByteArrayOutputStream DataInputStream
    DataOutputStream Serializable ObjectOutputStream ObjectInputStream
    DataOutput DataInput]
   [java.lang.reflect Constructor]
   [java.net URI]
   [java.util UUID]
   [java.util.regex Pattern]
   [clojure.lang Keyword Symbol BigInt Ratio
    APersistentMap APersistentVector APersistentSet
    IPersistentMap IPersistentList ; IPersistentVector IPersistentSet
    PersistentQueue PersistentTreeMap PersistentTreeSet
    MapEntry LazySeq IRecord ISeq IType]))

(enc/assert-min-encore-version [3 160 1])

(comment
  (set! *unchecked-math* :warn-on-boxed)
  (set! *unchecked-math* false)
  (thaw (freeze stress-data)))

;;;; Nippy data format
;; * 4-byte header (Nippy v2.x+) (may be disabled but incl. by default) [1]
;; { * 1-byte type id
;;   * Arb-length payload determined by freezer for this type [2] } ...
;;
;; [1] Inclusion of header is *strongly* recommended. Purpose:
;;   * Sanity check (confirm that data appears to be Nippy data)
;;   * Nippy version check (=> supports changes to data schema over time)
;;   * Supports :auto thaw compressor, encryptor
;;   * Supports :auto freeze compressor (since this depends on :auto thaw
;;     compressor)
;;
;; [2] See `IFreezable` protocol for type-specific payload formats,
;;     `thaw-from-in!` for reference type-specific thaw implementations
;;
(def ^:private head-sig "First 3 bytes of Nippy header" (.getBytes "NPY" StandardCharsets/UTF_8))
(def ^:private ^:const head-version "Current Nippy header format version" 1)
(def ^:private ^:const head-meta
  "Final byte of 4-byte Nippy header stores version-dependent metadata"

  ;; Currently:
  ;;   - 6x compressors: #{nil :zstd :lz4 #_:lzo :lzma2 :snappy :else}
  ;;   - 4x encryptors:  #{nil :aes128-cbc-sha512 :aes128-gcm-sha512 :else}

  {(byte 0)  {:version 1 :compressor-id nil     :encryptor-id nil}
   (byte 2)  {:version 1 :compressor-id nil     :encryptor-id :aes128-cbc-sha512}
   (byte 14) {:version 1 :compressor-id nil     :encryptor-id :aes128-gcm-sha512}
   (byte 4)  {:version 1 :compressor-id nil     :encryptor-id :else}

   (byte 1)  {:version 1 :compressor-id :snappy :encryptor-id nil}
   (byte 3)  {:version 1 :compressor-id :snappy :encryptor-id :aes128-cbc-sha512}
   (byte 15) {:version 1 :compressor-id :snappy :encryptor-id :aes128-gcm-sha512}
   (byte 7)  {:version 1 :compressor-id :snappy :encryptor-id :else}

   (byte 8)  {:version 1 :compressor-id :lz4    :encryptor-id nil}
   (byte 9)  {:version 1 :compressor-id :lz4    :encryptor-id :aes128-cbc-sha512}
   (byte 16) {:version 1 :compressor-id :lz4    :encryptor-id :aes128-gcm-sha512}
   (byte 10) {:version 1 :compressor-id :lz4    :encryptor-id :else}

   (byte 11) {:version 1 :compressor-id :lzma2  :encryptor-id nil}
   (byte 12) {:version 1 :compressor-id :lzma2  :encryptor-id :aes128-cbc-sha512}
   (byte 17) {:version 1 :compressor-id :lzma2  :encryptor-id :aes128-gcm-sha512}
   (byte 13) {:version 1 :compressor-id :lzma2  :encryptor-id :else}

   (byte 20) {:version 1 :compressor-id :zstd   :encryptor-id nil}
   (byte 21) {:version 1 :compressor-id :zstd   :encryptor-id :aes128-cbc-sha512}
   (byte 22) {:version 1 :compressor-id :zstd   :encryptor-id :aes128-gcm-sha512}
   (byte 23) {:version 1 :compressor-id :zstd   :encryptor-id :else}

   (byte 5)  {:version 1 :compressor-id :else   :encryptor-id nil}
   (byte 18) {:version 1 :compressor-id :else   :encryptor-id :aes128-cbc-sha512}
   (byte 19) {:version 1 :compressor-id :else   :encryptor-id :aes128-gcm-sha512}
   (byte 6)  {:version 1 :compressor-id :else   :encryptor-id :else}})

(comment (count (sort (keys head-meta))))

(defmacro ^:private when-debug [& body] (when #_true false `(do ~@body)))

(def ^:private types-spec
  "Private representation of Nippy's internal type schema,
    {<type-id> [<type-kw> ?<payload-info>]}.

  See `public-types-spec` for more info."

  {3   [:nil      []]
   8   [:true     []]
   9   [:false    []]
   10  [:char     [[:bytes 2]]]

   40  [:byte     [[:bytes 1]]]
   41  [:short    [[:bytes 2]]]
   42  [:integer  [[:bytes 4]]]

   0   [:long-0      []]

   87  [:long-pos-sm [[:bytes 1]]]
   88  [:long-pos-md [[:bytes 2]]]
   89  [:long-pos-lg [[:bytes 4]]]

   93  [:long-neg-sm [[:bytes 1]]]
   94  [:long-neg-md [[:bytes 2]]]
   95  [:long-neg-lg [[:bytes 4]]]

   43  [:long-xl     [[:bytes 8]]]

   55  [:double-0 []]
   60  [:float    [[:bytes 4]]]
   61  [:double   [[:bytes 8]]]

   91  [:uuid      [[:bytes 16]]]
   90  [:util-date [[:bytes 8]]]
   92  [:sql-date  [[:bytes 8]]]

   ;; JVM >=8
   79  [:time-instant  [[:bytes 12]]]
   83  [:time-duration [[:bytes 12]]]
   84  [:time-period   [[:bytes 12]]]

   34  [:str-0     []]
   96  [:str-sm*   [[:bytes {:read 1 :unsigned? true}]]]
   16  [:str-md    [[:bytes {:read 2}]]]
   13  [:str-lg    [[:bytes {:read 4}]]]

   106 [:kw-sm     [[:bytes {:read 1}]]]
   85  [:kw-md     [[:bytes {:read 2}]]]

   56  [:sym-sm    [[:bytes {:read 1}]]]
   86  [:sym-md    [[:bytes {:read 2}]]]

   47  [:reader-sm [[:bytes {:read 1}]]]
   51  [:reader-md [[:bytes {:read 2}]]]
   52  [:reader-lg [[:bytes {:read 4}]]]

   17  [:vec-0     []]
   113 [:vec-2     [[:elements 2]]]
   114 [:vec-3     [[:elements 3]]]
   97  [:vec-sm*   [[:elements {:read 1 :unsigned? true}]]]
   69  [:vec-md    [[:elements {:read 2}]]]
   21  [:vec-lg    [[:elements {:read 4}]]]

   18  [:set-0     []]
   98  [:set-sm*   [[:elements {:read 1 :unsigned? true}]]]
   32  [:set-md    [[:elements {:read 2}]]]
   23  [:set-lg    [[:elements {:read 4}]]]

   19  [:map-0     []]
   99  [:map-sm*   [[:elements {:read 1 :multiplier 2 :unsigned? true}]]]
   33  [:map-md    [[:elements {:read 2 :multiplier 2}]]]
   30  [:map-lg    [[:elements {:read 4 :multiplier 2}]]]

   103 [:map-entry [[:elements 2]]]

   35  [:list-0    []]
   36  [:list-sm   [[:elements {:read 1}]]]
   54  [:list-md   [[:elements {:read 2}]]]
   20  [:list-lg   [[:elements {:read 4}]]]

   37  [:seq-0     []]
   38  [:seq-sm    [[:elements {:read 1}]]]
   39  [:seq-md    [[:elements {:read 2}]]]
   24  [:seq-lg    [[:elements {:read 4}]]]

   28  [:sorted-set-lg [[:elements {:read 4}]]]
   31  [:sorted-map-lg [[:elements {:read 4 :multiplier 2}]]]
   26  [:queue-lg      [[:elements {:read 4}]]]

   25  [:meta  [[:elements 1]]]
   58  [:regex [[:elements 1]]]
   71  [:uri   [[:elements 1]]]

   ;; BigInteger based
   44  [:bigint     [[:bytes {:read 4}]]]
   45  [:biginteger [[:bytes {:read 4}]]]
   62  [:bigdec     [[:bytes 4]
                     [:bytes {:read 4}]]]
   70  [:ratio      [[:bytes {:read 4}]
                     [:bytes {:read 4}]]]

   ;; Arrays
   53  [:byte-array-0    []]
   7   [:byte-array-sm   [[:elements {:read 1}]]]
   15  [:byte-array-md   [[:elements {:read 2}]]]
   2   [:byte-array-lg   [[:elements {:read 4}]]]

   109 [:int-array-lg    [[:elements {:read 4}]]] ; Added v3.5.0 (2025-04-15)
   108 [:long-array-lg   [[:elements {:read 4}]]] ; Added v3.5.0 (2025-04-15)

   117 [:float-array-lg  [[:elements {:read 4}]]] ; Added v3.5.0 (2025-04-15)
   116 [:double-array-lg [[:elements {:read 4}]]] ; Added v3.5.0 (2025-04-15)

   107 [:string-array-lg [[:elements {:read 4}]]] ; Added v3.5.0 (2025-04-15)
   115 [:object-array-lg [[:elements {:read 4}]]]

   ;; Serializable
   75  [:sz-quarantined-sm [[:bytes {:read 1}] [:elements 1]]]
   76  [:sz-quarantined-md [[:bytes {:read 2}] [:elements 1]]]

   48  [:record-sm         [[:bytes {:read 1}] [:elements 1]]]
   49  [:record-md         [[:bytes {:read 2}] [:elements 1]]]

   104 [:meta-protocol-key []]

   ;; Necessarily without size information
   81  [:type               nil]
   82  [:prefixed-custom-md nil]
   59  [:cached-0           nil]
   63  [:cached-1           nil]
   64  [:cached-2           nil]
   65  [:cached-3           nil]
   66  [:cached-4           nil]
   72  [:cached-5           nil]
   73  [:cached-6           nil]
   74  [:cached-7           nil]
   67  [:cached-sm          nil]
   68  [:cached-md          nil]

   ;;; DEPRECATED (only support thawing)
   ;; Desc-sorted by deprecation date

   105 [:str-sm_  [[:bytes    {:read 1}]]]               ; [2023-08-02 v3.3.0] Switch to unsigned sm*
   110 [:vec-sm_  [[:elements {:read 1}]]]               ; [2023-08-02 v3.3.0] Switch to unsigned sm*
   111 [:set-sm_  [[:elements {:read 1}]]]               ; [2023-08-02 v3.3.0] Switch to unsigned sm*
   112 [:map-sm_  [[:elements {:read 1 :multiplier 2}]]] ; [2023-08-02 v3.3.0] Switch to unsigned sm*

   100 [:long-sm_ [[:bytes 1]]] ; [2023-08-02 v3.3.0] Switch to 2x pos/neg ids
   101 [:long-md_ [[:bytes 2]]] ; [2023-08-02 v3.3.0] Switch to 2x pos/neg ids
   102 [:long-lg_ [[:bytes 4]]] ; [2023-08-02 v3.3.0] Switch to 2x pos/neg ids

   78  [:sym-md_ [[:bytes {:read 4}]]] ; [2020-11-18 v3.1.1] Buggy size field, Ref. #138
   77  [:kw-md_  [[:bytes {:read 4}]]] ; [2020-11-18 v3.1.1] Buggy size field, Ref. #138

   6   [:sz-unquarantined-lg_ nil] ; [2020-07-24 v2.15.0] Unskippable, Ref. #130
   50  [:sz-unquarantined-md_ nil] ; [2020-07-24 v2.15.0] Unskippable, Ref. #130
   46  [:sz-unquarantined-sm_ nil] ; [2020-07-24 v2.15.0] Unskippable, Ref. #130

   14  [:kw-lg_     [[:bytes {:read 4}]]]               ; [2020-09-20 v3.0.0] Unrealistic
   57  [:sym-lg_    [[:bytes {:read 4}]]]               ; [2020-09-20 v3.0.0] Unrealistic
   80  [:record-lg_ [[:bytes {:read 4}] [:elements 1]]] ; [2020-09-20 v3.0.0] Unrealistic

   5   [:reader-lg_  [[:bytes {:read 4}]]] ; [2016-07-24 v2.12.0] Identical to :reader-lg, historical accident
   4   [:boolean_    [[:bytes 1]]]         ; [2016-07-24 v2.12.0] For switch to true/false ids

   29  [:sorted-map_ [[:elements {:read 4}]]] ; [2016-02-25 v2.11.0] For count/2
   27  [:map__       [[:elements {:read 4}]]] ; [2016-02-25 v2.11.0] For count/2

   12  [:kw_     [[:bytes {:read 2}]]] ; [2013-07-22 v2.0.0] For consistecy with str impln

   1   [:reader_ [[:bytes {:read 2}]]] ; [2012-07-20 v0.9.2] For >64k length support
   11  [:str_    [[:bytes {:read 2}]]] ; [2012-07-20 v0.9.2] For >64k length support

   22  [:map_    [[:elements {:read 4 :multiplier 2}]]] ; [2012-07-07 v0.9.0] For more efficient thaw impln
   })

(comment
  (count ; Eval to check for unused type-ids
    (enc/reduce-n (fn [acc in] (if-not (types-spec in) (conj acc in) acc))
      [] Byte/MAX_VALUE)))

(defmacro ^:private defids []
  `(do
     ~@(map
         (fn [[id# [kw#]]]
           (let [kw#  (str "id-" (name   kw#))
                 sym# (with-meta (symbol kw#) {:const true :private true})]
             `(def ~sym# (byte ~id#))))
         types-spec)))

(comment (macroexpand '(defids)))

(defids)

(def public-types-spec
  "Public representation of Nippy's internal type schema.
  For use by tooling and advanced users.

  **HIGHLY EXPERIMENTAL!**
  Subject to breaking change without notice.
  Currently completely untested, may contain bugs.
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

    types-spec
    types-spec))

(comment (get public-types-spec 96))

;;;; Ns imports (for convenience of lib consumers)

(enc/defaliases
  compression/compress
  compression/decompress
  compression/zstd-compressor
  compression/lz4-compressor
  compression/lz4hc-compressor
  #_compression/lzo-compressor
  compression/snappy-compressor
  compression/lzma2-compressor

  encryption/encrypt
  encryption/decrypt

  encryption/aes128-gcm-encryptor
  encryption/aes128-cbc-encryptor
  encryption/aes128-gcm-encryptor
  {:src encryption/aes128-gcm-encryptor, :alias aes128-encryptor})

;;;; Dynamic config
;; See also `nippy.tools` ns for further dynamic config support

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

    3. [Advanced] Custom (fn [^java.io.DataOutput out obj]) that must
        write an appropriate object type id and payload to the given
       `DataOutput` stream."

  nil)

(enc/defonce ^:dynamic *custom-readers* "{<hash-or-byte-id> (fn [data-input])->read}" nil)
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
;; Unfortunately quite a bit of complexity to do this safely

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
    - Does NOT implement Nippy's `Freezable`    protocol.
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

(defn- freeze-serializable-allowed? [x] (impl/serializable-allowed?                            *freeze-serializable-allowlist*  x))
(defn-   thaw-serializable-allowed? [x] (impl/serializable-allowed? (or *serializable-whitelist* *thaw-serializable-allowlist*) x))

(comment
  (enc/qb 1e6 (freeze-serializable-allowed? "foo")) ; 65.63
  (binding [*freeze-serializable-allowlist* #{"foo.*" "bar"}]
    (freeze-serializable-allowed? "foo.bar")))

;;;; Freezing interface (don't use these directly, use `extend-freeze` instead!)

(defprotocol IFreezeToBBuf "Private protocol for freezing objects to a `ByteBuffer`." (-freeze->bbuf! [x ^ByteBuffer bb ^DataOutput dout]))
(defprotocol IFreezeToDOut "Private protocol for freezing objects to a `DataOutput`." (-freeze->dout! [_                ^DataOutput dout]))
(defprotocol IFreezable    "Private protocol for freezable objects."                  (-freezable?  [_]))

;;;; Freezing

(do
  (def ^:private ^:const range-ubyte  (-    Byte/MAX_VALUE    Byte/MIN_VALUE))
  (def ^:private ^:const range-ushort (-   Short/MAX_VALUE   Short/MIN_VALUE))
  (def ^:private ^:const range-uint   (- Integer/MAX_VALUE Integer/MIN_VALUE))

  (defmacro ^:private sm-count?* [n] `(<= ~n     range-ubyte)) ; Unsigned
  (defmacro ^:private sm-count?  [n] `(<= ~n  Byte/MAX_VALUE))
  (defmacro ^:private md-count?  [n] `(<= ~n Short/MAX_VALUE))

  (defmacro ^:private write-sm-count  [out n] `(.writeByte  ~out    ~n))
  (defmacro ^:private write-md-count  [out n] `(.writeShort ~out    ~n))
  (defmacro ^:private write-lg-count  [out n] `(.writeInt   ~out    ~n))

  (defmacro ^:private read-sm-count* [in] `(- (.readByte  ~in) Byte/MIN_VALUE))
  (defmacro ^:private read-sm-count  [in]    `(.readByte  ~in))
  (defmacro ^:private read-md-count  [in]    `(.readShort ~in))
  (defmacro ^:private read-lg-count  [in]    `(.readInt   ~in)))

(defmacro write-id        [            dout        id] `(.writeByte ~dout ~id))
(defn-    write-bytes-sm  [^DataOutput dout ^bytes ba] (let [len (alength ba)] (write-sm-count dout len) (.write dout ba 0 len)))
(defn-    write-bytes-md  [^DataOutput dout ^bytes ba] (let [len (alength ba)] (write-md-count dout len) (.write dout ba 0 len)))
(defn-    write-bytes-lg  [^DataOutput dout ^bytes ba] (let [len (alength ba)] (write-lg-count dout len) (.write dout ba 0 len)))
(defn-    write-bytes     [^DataOutput dout ^bytes ba]
  (let [len (alength ba)]
    (if (zero? len)
      (write-id dout id-byte-array-0)
      (do
        (enc/cond
          (sm-count? len) (do (write-id dout id-byte-array-sm) (write-sm-count dout len))
          (md-count? len) (do (write-id dout id-byte-array-md) (write-md-count dout len))
          :else           (do (write-id dout id-byte-array-lg) (write-lg-count dout len)))

        (.write dout ba 0 len)))))

;; @TODO These should be private?
(defmacro write-id-bb        [bb id] `(.put      ~bb (unchecked-byte  ~id)))
(defmacro write-sm-count-bb* [bb  n] `(.put      ~bb (unchecked-byte (+ ~n Byte/MIN_VALUE))))
(defmacro write-sm-count-bb  [bb  n] `(.put      ~bb (unchecked-byte    ~n)))
(defmacro write-md-count-bb  [bb  n] `(.putShort ~bb (unchecked-short   ~n)))
(defmacro write-lg-count-bb  [bb  n] `(.putInt   ~bb (int               ~n)))

(defn- write-bytes-sm-bb  [^ByteBuffer bb ^bytes ba] (let [len (alength ba)] (write-sm-count-bb  bb len) (.put bb ba 0 len)))
(defn- write-bytes-md-bb  [^ByteBuffer bb ^bytes ba] (let [len (alength ba)] (write-md-count-bb  bb len) (.put bb ba 0 len)))
(defn- write-bytes-lg-bb  [^ByteBuffer bb ^bytes ba] (let [len (alength ba)] (write-lg-count-bb  bb len) (.put bb ba 0 len)))
(defn- write-bytes-bb     [^ByteBuffer bb ^bytes ba]
  (let [len (alength ba)]
    (if (zero? len)
      (write-id-bb bb id-byte-array-0)
      (do
        (enc/cond
          (sm-count? len) (do (write-id-bb bb id-byte-array-sm) (write-sm-count-bb bb len))
          (md-count? len) (do (write-id-bb bb id-byte-array-md) (write-md-count-bb bb len))
          :else           (do (write-id-bb bb id-byte-array-lg) (write-lg-count-bb bb len)))

        (.put bb ba 0 len)))))

(defn- write-biginteger-bb [^ByteBuffer bb ^BigInteger n] (write-bytes-lg-bb bb (.toByteArray n)))

(defn- write-str-bb [^ByteBuffer bb ^String s]
  (if (identical? s "")
    (write-id-bb bb id-str-0)
    (let [ba  (.getBytes s StandardCharsets/UTF_8)
          len (alength ba)]
      (enc/cond
        (when     impl/pack-unsigned? (sm-count?* len)) (do (write-id-bb bb id-str-sm*) (write-sm-count-bb* bb len))
        (when-not impl/pack-unsigned? (sm-count?  len)) (do (write-id-bb bb id-str-sm_) (write-sm-count-bb  bb len))
                                      (md-count?  len)  (do (write-id-bb bb id-str-md)  (write-md-count-bb  bb len))
        :else                                           (do (write-id-bb bb id-str-lg)  (write-lg-count-bb  bb len)))
      (.put bb ba 0 len))))

(defn- write-kw-bb [^ByteBuffer bb kw]
  (let [s   (if-let [ns (namespace kw)] (str ns "/" (name kw)) (name kw))
        ba  (.getBytes s StandardCharsets/UTF_8)
        len (alength ba)]
    (enc/cond
      (sm-count? len) (do (write-id-bb bb id-kw-sm) (write-sm-count-bb bb len))
      (md-count? len) (do (write-id-bb bb id-kw-md) (write-md-count-bb bb len))
      :else           (truss/ex-info! "Keyword too long" {:name s}))
    (.put bb ba 0 len)))

(defn- write-sym-bb [^ByteBuffer bb s]
  (let [s   (if-let [ns (namespace s)] (str ns "/" (name s)) (name s))
        ba  (.getBytes s StandardCharsets/UTF_8)
        len (alength ba)]
    (enc/cond
      (sm-count? len) (do (write-id-bb bb id-sym-sm) (write-sm-count-bb bb len))
      (md-count? len) (do (write-id-bb bb id-sym-md) (write-md-count-bb bb len))
      :else           (truss/ex-info! "Symbol too long" {:name s}))
    (.put bb ba 0 len)))

(defn- write-long-legacy-bb [^ByteBuffer bb ^long n]
  (enc/cond
    (zero? n) (write-id-bb bb id-long-0)
    (pos?  n)
    (enc/cond
      (<= n    Byte/MAX_VALUE) (do (write-id-bb bb id-long-sm_) (.put      bb (unchecked-byte  n)))
      (<= n   Short/MAX_VALUE) (do (write-id-bb bb id-long-md_) (.putShort bb (unchecked-short n)))
      (<= n Integer/MAX_VALUE) (do (write-id-bb bb id-long-lg_) (.putInt   bb (int n)))
      :else                    (do (write-id-bb bb id-long-xl)  (.putLong  bb      n)))

    :else
    (enc/cond
      (>= n    Byte/MIN_VALUE) (do (write-id-bb bb id-long-sm_) (.put      bb (unchecked-byte  n)))
      (>= n   Short/MIN_VALUE) (do (write-id-bb bb id-long-md_) (.putShort bb (unchecked-short n)))
      (>= n Integer/MIN_VALUE) (do (write-id-bb bb id-long-lg_) (.putInt   bb (int n)))
      :else                    (do (write-id-bb bb id-long-xl)  (.putLong  bb      n)))))

(defn- write-long-bb [^ByteBuffer bb ^long n]
  (enc/cond
    (not impl/pack-unsigned?) (write-long-legacy-bb bb n)
    (zero? n)                 (write-id-bb          bb id-long-0)
    (pos?  n)
    (enc/cond
      (<= n range-ubyte)  (do (write-id-bb bb id-long-pos-sm) (.put      bb (unchecked-byte  (+ n    Byte/MIN_VALUE))))
      (<= n range-ushort) (do (write-id-bb bb id-long-pos-md) (.putShort bb (unchecked-short (+ n   Short/MIN_VALUE))))
      (<= n range-uint)   (do (write-id-bb bb id-long-pos-lg) (.putInt   bb (int             (+ n Integer/MIN_VALUE))))
      :else               (do (write-id-bb bb id-long-xl)     (.putLong  bb                     n)))

    :else
    (let [y (- n)]
      (enc/cond
        (<= y range-ubyte)  (do (write-id-bb bb id-long-neg-sm) (.put      bb (unchecked-byte  (+ y    Byte/MIN_VALUE))))
        (<= y range-ushort) (do (write-id-bb bb id-long-neg-md) (.putShort bb (unchecked-short (+ y   Short/MIN_VALUE))))
        (<= y range-uint)   (do (write-id-bb bb id-long-neg-lg) (.putInt   bb (int             (+ y Integer/MIN_VALUE))))
        :else               (do (write-id-bb bb id-long-xl)     (.putLong  bb                     n))))))

;; @TODO Refactor these names, continue from here...
(declare freeze-with-meta-bb! freeze-without-meta-bb!)

(defn- write-counted-coll-bb
  ([^ByteBuffer bb dout id-lg coll]
   (let [cnt (count coll)]
     (write-id-bb       bb id-lg)
     (write-lg-count-bb bb cnt)
     (reduce (fn [_ in] (freeze-with-meta-bb! bb dout in)) nil coll)))

  ([^ByteBuffer bb dout id-empty id-sm id-md id-lg coll]
   (let [cnt (count coll)]
     (if (zero? cnt)
       (write-id-bb bb id-empty)
       (do
         (enc/cond
           (sm-count? cnt) (do (write-id-bb bb id-sm) (write-sm-count-bb bb cnt))
           (md-count? cnt) (do (write-id-bb bb id-md) (write-md-count-bb bb cnt))
           :else           (do (write-id-bb bb id-lg) (write-lg-count-bb bb cnt)))
         (reduce (fn [_ in] (freeze-with-meta-bb! bb dout in)) nil coll))))))

(def ^:private ^:const meta-protocol-key ::meta-protocol-key)

(defn- write-map-bb [^ByteBuffer bb dout m is-metadata?]
  (let [cnt (count m)]
    (if (zero? cnt)
      (write-id-bb bb id-map-0)
      (do
        (enc/cond
          (when     impl/pack-unsigned? (sm-count?* cnt)) (do (write-id-bb bb id-map-sm*) (write-sm-count-bb* bb cnt))
          (when-not impl/pack-unsigned? (sm-count?  cnt)) (do (write-id-bb bb id-map-sm_) (write-sm-count-bb  bb cnt))
                                        (md-count?  cnt)  (do (write-id-bb bb id-map-md)  (write-md-count-bb  bb cnt))
          :else                                           (do (write-id-bb bb id-map-lg)  (write-lg-count-bb  bb cnt)))

        (reduce-kv
          (fn [_ k v]
            (if (enc/and? is-metadata? (fn? v) (qualified-symbol? k))
              (do
                (if (impl/target-release>= 340)
                  (write-id-bb             bb   id-meta-protocol-key)
                  (freeze-without-meta-bb! bb dout meta-protocol-key))
                (write-id-bb bb id-nil))
              (do
                (freeze-with-meta-bb! bb dout k)
                (freeze-with-meta-bb! bb dout v))))
          nil
          m)))))

(defn- write-set-bb [^ByteBuffer bb dout s]
  (let [cnt (count s)]
    (if (zero? cnt)
      (write-id-bb bb id-set-0)
      (do
        (enc/cond
          (when     impl/pack-unsigned? (sm-count?* cnt)) (do (write-id-bb bb id-set-sm*) (write-sm-count-bb* bb cnt))
          (when-not impl/pack-unsigned? (sm-count?  cnt)) (do (write-id-bb bb id-set-sm_) (write-sm-count-bb  bb cnt))
                                        (md-count?  cnt)  (do (write-id-bb bb id-set-md)  (write-md-count-bb  bb cnt))
          :else                                           (do (write-id-bb bb id-set-lg)  (write-lg-count-bb  bb cnt)))
        (reduce (fn [_ in] (freeze-with-meta-bb! bb dout in)) nil s)))))

(deftype Cached [val])

(def ^ThreadLocal -cache-proxy
  "{[<x> <meta>] <idx>} for freezing, {<idx> <x-with-meta>} for thawing."
  ;; Nb: don't use an auto initialValue; can cause thread-local state to
  ;; accidentally hang around with the use of `freeze-to-out!`, etc.
  ;; Safer to require explicit activation through `with-cache`.
  (proxy [ThreadLocal] []))

(extend-protocol IFreezeToBBuf
  nil        (-freeze->bbuf! [            _ ^ByteBuffer bb _] (write-id-bb bb id-nil))
  Boolean    (-freeze->bbuf! [^Boolean    x ^ByteBuffer bb _] (if (.booleanValue x) (write-id-bb bb id-true) (write-id-bb bb id-false)))
  Character  (-freeze->bbuf! [^Character  x ^ByteBuffer bb _] (do (write-id-bb bb id-char)    (.putChar  bb (unchecked-char (int x)))))
  Byte       (-freeze->bbuf! [^Byte       x ^ByteBuffer bb _] (do (write-id-bb bb id-byte)    (.put      bb (unchecked-byte      x))))
  Short      (-freeze->bbuf! [^Short      x ^ByteBuffer bb _] (do (write-id-bb bb id-short)   (.putShort bb (unchecked-short     x))))
  Integer    (-freeze->bbuf! [^Integer    x ^ByteBuffer bb _] (do (write-id-bb bb id-integer) (.putInt   bb                 (int x))))
  Long       (-freeze->bbuf! [^Long       x ^ByteBuffer bb _] (write-long-bb bb x))
  Float      (-freeze->bbuf! [^Float      x ^ByteBuffer bb _] (do (write-id-bb bb id-float) (.putFloat bb x)))
  Double     (-freeze->bbuf! [^Double     x ^ByteBuffer bb _] (if (zero? ^double x) (write-id-bb bb id-double-0) (do (write-id-bb bb id-double) (.putDouble bb x))))
  BigInt     (-freeze->bbuf! [^BigInt     x ^ByteBuffer bb _] (do (write-id-bb bb id-bigint)     (write-biginteger-bb bb (.toBigInteger  x))))
  BigInteger (-freeze->bbuf! [^BigInteger x ^ByteBuffer bb _] (do (write-id-bb bb id-biginteger) (write-biginteger-bb bb                 x)))
  BigDecimal (-freeze->bbuf! [^BigDecimal x ^ByteBuffer bb _] (do (write-id-bb bb id-bigdec)     (write-biginteger-bb bb (.unscaledValue x)) (.putInt bb (.scale x))))
  Ratio      (-freeze->bbuf! [^Ratio      x ^ByteBuffer bb _] (do (write-id-bb bb id-ratio)      (write-biginteger-bb bb (.numerator x)) (write-biginteger-bb bb (.denominator x))))

  ;; MapEntry before APersistentVector: MapEntry extends APersistentVector but
  ;; needs its own wire format (id-map-entry, not the vector format).
  MapEntry          (-freeze->bbuf! [                x ^ByteBuffer bb dout] (do (write-id-bb bb id-map-entry) (freeze-with-meta-bb! bb dout (key x)) (freeze-with-meta-bb! bb dout (val x))))
  java.sql.Date     (-freeze->bbuf! [^java.sql.Date  x ^ByteBuffer bb    _] (do (write-id-bb bb id-sql-date)  (.putLong bb (.getTime  x))))
  java.util.Date    (-freeze->bbuf! [^java.util.Date x ^ByteBuffer bb    _] (do (write-id-bb bb id-util-date) (.putLong bb (.getTime  x))))
  URI               (-freeze->bbuf! [^URI            x ^ByteBuffer bb    _] (do (write-id-bb bb id-uri)  (write-str-bb  bb (.toString x))))
  UUID              (-freeze->bbuf! [^UUID           x ^ByteBuffer bb    _] (do (write-id-bb bb id-uuid)   (.putLong    bb (.getMostSignificantBits  x))
                                                                                                           (.putLong    bb (.getLeastSignificantBits x))))
  String            (-freeze->bbuf! [^String         x ^ByteBuffer bb    _] (write-str-bb bb x))
  Keyword           (-freeze->bbuf! [                x ^ByteBuffer bb    _] (write-kw-bb  bb x))
  Symbol            (-freeze->bbuf! [                x ^ByteBuffer bb    _] (write-sym-bb bb x))
  Pattern           (-freeze->bbuf! [^Pattern        x ^ByteBuffer bb    _] (do (write-id-bb bb id-regex) (write-str-bb bb (str x))))
  PersistentQueue   (-freeze->bbuf! [                x ^ByteBuffer bb dout] (write-counted-coll-bb bb dout id-queue-lg      x))
  PersistentTreeSet (-freeze->bbuf! [                x ^ByteBuffer bb dout] (write-counted-coll-bb bb dout id-sorted-set-lg x))
  PersistentTreeMap (-freeze->bbuf! [                x ^ByteBuffer bb dout]
                      (do (write-id-bb bb id-sorted-map-lg)
                          (write-lg-count-bb bb (count x))
                          (reduce-kv
                            (fn [_ k v]
                              (freeze-with-meta-bb! bb dout k)
                              (freeze-with-meta-bb! bb dout v))
                            nil x)))

  ;; Cached before IType: Cached is a deftype (implements IType) but needs its
  ;; own cache-tracking logic. Protocol dispatch picks Cached (more specific class)
  ;; over IType (interface), so no ordering issue — but we're explicit for clarity.
  Cached
  (-freeze->bbuf! [x ^ByteBuffer bb dout]
    (let [x-val (.-val ^Cached x)]
      (if-let [cache_ (.get -cache-proxy)]
        (let [cache  @cache_
              k      [x-val (meta x-val)]
              ?idx   (get cache k)
              idx    (long (or ?idx (let [idx (count cache)] (vswap! cache_ assoc k idx) idx)))
              first? (nil? ?idx)]
          (enc/cond
            (sm-count? idx)
            (case (int idx)
              0 (do (write-id-bb bb id-cached-0) (when first? (freeze-with-meta-bb! bb dout x-val)))
              1 (do (write-id-bb bb id-cached-1) (when first? (freeze-with-meta-bb! bb dout x-val)))
              2 (do (write-id-bb bb id-cached-2) (when first? (freeze-with-meta-bb! bb dout x-val)))
              3 (do (write-id-bb bb id-cached-3) (when first? (freeze-with-meta-bb! bb dout x-val)))
              4 (do (write-id-bb bb id-cached-4) (when first? (freeze-with-meta-bb! bb dout x-val)))
              5 (do (write-id-bb bb id-cached-5) (when first? (freeze-with-meta-bb! bb dout x-val)))
              6 (do (write-id-bb bb id-cached-6) (when first? (freeze-with-meta-bb! bb dout x-val)))
              7 (do (write-id-bb bb id-cached-7) (when first? (freeze-with-meta-bb! bb dout x-val)))
              (do
                (write-id-bb       bb id-cached-sm)
                (write-sm-count-bb bb idx)
                (when first? (freeze-with-meta-bb! bb dout x-val))))
            (md-count? idx)
            (do
              (write-id-bb       bb id-cached-md)
              (write-md-count-bb bb idx)
              (when first? (freeze-with-meta-bb! bb dout x-val)))
            :else
            (freeze-with-meta-bb! bb dout x-val)))
        (freeze-with-meta-bb! bb dout x-val))))

  ;; IRecord: check (-freezable? x) first so user extend-freeze handlers win
  ;; over the built-in record serialization.
  IRecord
  (-freeze->bbuf! [x ^ByteBuffer bb dout]
    (if (-freezable? x)
      (-freeze->dout! x (dout))
      (let [class-name    (.getName (class x))
            class-name-ba (.getBytes class-name StandardCharsets/UTF_8)
            len           (alength   class-name-ba)]
        (enc/cond
          (sm-count? len) (do (write-id-bb bb id-record-sm) (write-bytes-sm-bb bb class-name-ba))
          (md-count? len) (do (write-id-bb bb id-record-md) (write-bytes-md-bb bb class-name-ba))
          :else           (truss/ex-info! "Record class name too long" {:name class-name}))
        (freeze-without-meta-bb! bb dout (into {} x)))))

  APersistentVector
  (-freeze->bbuf! [x ^ByteBuffer bb dout]
    (let [cnt (count x)]
      (if (zero? cnt)
        (write-id-bb bb id-vec-0)
        (do
          (enc/cond
            (when     impl/pack-unsigned? (sm-count?* cnt)) (do (write-id-bb bb id-vec-sm*) (write-sm-count-bb* bb cnt))
            (when-not impl/pack-unsigned? (sm-count?  cnt)) (do (write-id-bb bb id-vec-sm_) (write-sm-count-bb  bb cnt))
            (md-count?  cnt)  (do (write-id-bb bb id-vec-md)  (write-md-count-bb  bb cnt))
            :else                               (do (write-id-bb bb id-vec-lg)  (write-lg-count-bb  bb cnt)))
          (reduce (fn [_ in] (freeze-with-meta-bb! bb dout in)) nil x)))))

  APersistentSet  (-freeze->bbuf! [x ^ByteBuffer bb dout] (write-set-bb          bb dout x))
  APersistentMap  (-freeze->bbuf! [x ^ByteBuffer bb dout] (write-map-bb          bb dout x false))
  IPersistentList (-freeze->bbuf! [x ^ByteBuffer bb dout] (write-counted-coll-bb bb dout id-list-0 id-list-sm id-list-md id-list-lg x))

  ;; IType: deftypes other than Cached. Check (-freezable? x) first so user
  ;; extend-freeze handlers win over field-based serialization.
  IType
  (-freeze->bbuf! [x ^ByteBuffer bb dout]
    (if (-freezable? x)
      (-freeze->dout! x (dout))
      (let [c (class x)]
        (write-id-bb  bb id-type)
        (write-str-bb bb (.getName c))
        (run! (fn [^java.lang.reflect.Field f] (freeze-without-meta-bb! bb dout (.get f x)))
          (impl/get-basis-fields c)))))

  LazySeq
  (-freeze->bbuf! [x ^ByteBuffer bb dout]
    (write-counted-coll-bb bb dout id-seq-0 id-seq-sm id-seq-md id-seq-lg
      (if (counted? x) x (into [] x)))))

(def ^:private array-class-bytes   (Class/forName "[B"))
(def ^:private array-class-longs   (Class/forName "[J"))
(def ^:private array-class-ints    (Class/forName "[I"))
(def ^:private array-class-doubles (Class/forName "[D"))
(def ^:private array-class-floats  (Class/forName "[F"))
(def ^:private array-class-strings (Class/forName "[Ljava.lang.String;"))
(def ^:private array-class-objects (Class/forName "[Ljava.lang.Object;"))

;; Array types: extend individually since extend-protocol doesn't support
;; array class literals. Keeping these out of the Object fallback gives O(1)
;; dispatch for the common case (byte-array in particular).
(extend array-class-bytes   IFreezeToBBuf {:-freeze->bbuf! (fn [^bytes                 x ^ByteBuffer bb _   ] (write-bytes-bb bb x))})
(extend array-class-longs   IFreezeToBBuf {:-freeze->bbuf! (fn [^longs                 x ^ByteBuffer bb out*] (let [cnt (alength x)] (write-id-bb bb id-long-array-lg)   (write-lg-count-bb bb cnt) (enc/reduce-n (fn [_ i] (freeze-with-meta-bb! bb out* (aget x i))) nil cnt)))})
(extend array-class-ints    IFreezeToBBuf {:-freeze->bbuf! (fn [^ints                  x ^ByteBuffer bb out*] (let [cnt (alength x)] (write-id-bb bb id-int-array-lg)    (write-lg-count-bb bb cnt) (enc/reduce-n (fn [_ i] (freeze-with-meta-bb! bb out* (aget x i))) nil cnt)))})
(extend array-class-doubles IFreezeToBBuf {:-freeze->bbuf! (fn [^doubles               x ^ByteBuffer bb out*] (let [cnt (alength x)] (write-id-bb bb id-double-array-lg) (write-lg-count-bb bb cnt) (enc/reduce-n (fn [_ i] (freeze-with-meta-bb! bb out* (aget x i))) nil cnt)))})
(extend array-class-floats  IFreezeToBBuf {:-freeze->bbuf! (fn [^floats                x ^ByteBuffer bb out*] (let [cnt (alength x)] (write-id-bb bb id-float-array-lg)  (write-lg-count-bb bb cnt) (enc/reduce-n (fn [_ i] (freeze-with-meta-bb! bb out* (aget x i))) nil cnt)))})
(extend array-class-strings IFreezeToBBuf {:-freeze->bbuf! (fn [^"[Ljava.lang.String;" x ^ByteBuffer bb out*] (let [cnt (alength x)] (write-id-bb bb id-string-array-lg) (write-lg-count-bb bb cnt) (enc/reduce-n (fn [_ i] (freeze-with-meta-bb! bb out* (aget x i))) nil cnt)))})
(extend array-class-objects IFreezeToBBuf {:-freeze->bbuf! (fn [^objects               x ^ByteBuffer bb out*] (let [cnt (alength x)] (write-id-bb bb id-object-array-lg) (write-lg-count-bb bb cnt) (enc/reduce-n (fn [_ i] (freeze-with-meta-bb! bb out* (aget x i))) nil cnt)))})

;; java.time extensions, conditional on JVM version
(enc/compile-if java.time.Instant
  (extend-protocol IFreezeToBBuf
    java.time.Instant
    (-freeze->bbuf! [^java.time.Instant x ^ByteBuffer bb _]
      (do (write-id-bb bb id-time-instant)
          (.putLong bb (.getEpochSecond x))
          (.putInt  bb (.getNano        x)))))
  nil)
(enc/compile-if java.time.Duration
  (extend-protocol IFreezeToBBuf
    java.time.Duration
    (-freeze->bbuf! [^java.time.Duration x ^ByteBuffer bb _]
      (do (write-id-bb bb id-time-duration)
          (.putLong bb (.getSeconds x))
          (.putInt  bb (.getNano    x)))))
  nil)
(enc/compile-if java.time.Period
  (extend-protocol IFreezeToBBuf
    java.time.Period
    (-freeze->bbuf! [^java.time.Period x ^ByteBuffer bb _]
      (do (write-id-bb bb id-time-period)
          (.putInt bb (.getYears  x))
          (.putInt bb (.getMonths x))
          (.putInt bb (.getDays   x)))))
  nil)

(defn- freeze-without-meta-bb! [^ByteBuffer bb out* x]
  (try
    (-freeze->bbuf! x bb out*)
    (catch IllegalArgumentException _
      (if (instance? ISeq x)
        (write-counted-coll-bb bb out* id-seq-0 id-seq-sm id-seq-md id-seq-lg
                               (if (counted? x) x (into [] x)))
        (-freeze->dout! x (out*))))))

(defn- freeze-with-meta-bb! [^ByteBuffer bb out* x]
  (when-let [m (when (and *incl-metadata?* (instance? clojure.lang.IObj x))
                  (not-empty (meta x)))]
    (write-id-bb bb id-meta)
    (write-map-bb bb out* m :is-metadata))
  (freeze-without-meta-bb! bb out* x))

(defn- natively-freezable?
  "Returns true iff x is a type handled directly by `freeze-without-meta-bb!`
  (i.e. not going through the IFreezable :else fallback)."
  [x]
  (or
    (nil?                     x)
    (instance? Boolean        x)
    (instance? Character      x)
    (instance? Byte           x)
    (instance? Short          x)
    (instance? Integer        x)
    (instance? Long           x)
    (instance? Float          x)
    (instance? Double         x)
    (instance? BigInt         x)
    (instance? BigInteger     x)
    (instance? BigDecimal     x)
    (instance? Ratio          x)
    (instance? String         x)
    (instance? Keyword        x)
    (instance? Symbol         x)
    (instance? MapEntry       x)
    (instance? java.sql.Date  x)
    (instance? java.util.Date x)
    (instance? URI            x)
    (instance? UUID           x)

    (instance? Pattern        x)
    (enc/compile-if java.time.Instant  (instance? java.time.Instant  x) false)
    (enc/compile-if java.time.Duration (instance? java.time.Duration x) false)
    (enc/compile-if java.time.Period   (instance? java.time.Period   x) false)
    (instance? PersistentQueue   x)
    (instance? PersistentTreeSet x)
    (instance? PersistentTreeMap x)
    (instance? IRecord           x)
    (instance? APersistentVector x)
    (instance? APersistentSet    x)
    (instance? APersistentMap    x)
    (instance? IPersistentList   x)
    (instance? IType             x)
    (instance? LazySeq           x)
    (instance? ISeq              x)
    (instance? Cached            x)

    (instance? array-class-bytes   x)
    (instance? array-class-longs   x)
    (instance? array-class-ints    x)
    (instance? array-class-doubles x)
    (instance? array-class-floats  x)
    (instance? array-class-strings x)
    (instance? array-class-objects x)))

(defn- write-serializable [^DataOutput out x]
  (when-debug (println (str "write-serializable: " (type x))))
  (when (and (instance? Serializable x) (not (fn? x)))
    (let [class-name (.getName (class x))] ; Reflect
      (when (freeze-serializable-allowed? class-name)
        (let [class-name-ba (.getBytes class-name StandardCharsets/UTF_8)
              len           (alength   class-name-ba)]

          (enc/cond
            (sm-count? len) (do (write-id out id-sz-quarantined-sm) (write-bytes-sm out class-name-ba))
            (md-count? len) (do (write-id out id-sz-quarantined-md) (write-bytes-md out class-name-ba))
            ;; :else        (do (write-id out id-sz-quarantined-lg) (write-bytes-md out class-name-ba)) ; Unrealistic
            :else           (truss/ex-info! "Serializable class name too long" {:name class-name}))

          ;; Legacy: write object directly to out.
          ;; (.writeObject (ObjectOutputStream. out) x)

          ;; Quarantined: write object to ba, then ba to out.
          ;; We'll have object length during thaw, allowing us to skip readObject.
          (let [quarantined-ba (ByteArrayOutputStream.)]
            (.writeObject (ObjectOutputStream. (DataOutputStream. quarantined-ba)) x)
            (write-bytes out (.toByteArray quarantined-ba)))

          true)))))

(defn- write-readable [^DataOutput out x]
  (when-debug (println (str "write-readable: " (type x))))
  (when (impl/seems-readable? x)
    (let [edn    (enc/pr-edn x)
          edn-ba (.getBytes ^String edn StandardCharsets/UTF_8)
          len    (alength edn-ba)]
      (enc/cond
        (sm-count? len) (do (write-id out id-reader-sm) (write-bytes-sm out edn-ba))
        (md-count? len) (do (write-id out id-reader-md) (write-bytes-md out edn-ba))
        :else           (do (write-id out id-reader-lg) (write-bytes-lg out edn-ba)))
      true)))

(defn ^:deprecated try-write-serializable [out x] (truss/catching :all (write-serializable out x)))
(defn ^:deprecated try-write-readable     [out x] (truss/catching :all (write-readable     out x)))

(defn- try-pr-edn [x]
  (try
    (enc/pr-edn x)
    (catch Throwable _
      (try
        (str x)
        (catch Throwable _
          :nippy/unprintable)))))

(declare freeze-raw!)

(defn write-unfreezable [^DataOutput out x]
  (let [m {:nippy/unfreezable {:type (type x) :content (try-pr-edn x)}}
        ^bytes ba (freeze-raw! m)]
    (.write out ba 0 (alength ba))))

;;; ByteBuffer adapters

(defn- assert-big-endian-byte-buffer
  ^ByteBuffer [^ByteBuffer bb]
  (when-not (= (.order bb) java.nio.ByteOrder/BIG_ENDIAN)
    (throw
      (IllegalArgumentException.
        (str "ByteBuffer must use BIG_ENDIAN order for DataInput/DataOutput semantics"
          " (got " (.order bb) ")."))))
  bb)

(defn- require-readable!   [^ByteBuffer bb ^long   n] (when (> n (.remaining bb)) (throw (java.io.EOFException. (str "ByteBuffer underflow: need " n " bytes, have " (.remaining bb) ".")))))
(defn- require-writable!   [^ByteBuffer bb ^long   n] (when (> n (.remaining bb)) (throw (java.nio.BufferOverflowException.))))
(defn- write-modified-utf! [^ByteBuffer bb ^String s]
  (let [strlen (.length s)
        utf-len
        (loop [idx 0 utf-len 0]
          (if (< idx strlen)
            (let [c (int (.charAt s idx))]
              (recur
                (inc idx)
                (long
                  (+ (long utf-len)
                     (long
                       (cond
                         (<= 0x0001 c 0x007F) 1
                         (> c 0x07FF)         3
                         :else                2))))))
            utf-len))]

    (when (> ^long utf-len 65535) (throw (java.io.UTFDataFormatException. (str "encoded string too long: " utf-len " bytes"))))

    (require-writable! bb (+ 2       ^long utf-len))
    (.putShort         bb (unchecked-short utf-len))
    (loop [idx 0]
      (when (< idx strlen)
        (let [c (int (.charAt s idx))]
          (cond
            (<= 0x0001 c 0x007F) (do (.put bb (unchecked-byte c)) (recur (inc idx)))
            (>         c 0x07FF)
            (do (.put bb (unchecked-byte (bit-or 0xE0          (unsigned-bit-shift-right c 12))))
                (.put bb (unchecked-byte (bit-or 0x80 (bit-and (unsigned-bit-shift-right c  6) 0x3F))))
                (.put bb (unchecked-byte (bit-or 0x80 (bit-and                           c     0x3F))))
                (recur (inc idx)))

            :else
            (do (.put bb (unchecked-byte (bit-or 0xC0 (unsigned-bit-shift-right c 6))))
                (.put bb (unchecked-byte (bit-or 0x80 (bit-and c 0x3F))))
                (recur (inc idx)))))))))

(defn buffer-data-input
  "Returns a DataInput adapter over given ByteBuffer.
  Reads from the buffer's current position and advances it."
  ^DataInput [^ByteBuffer bb]
  (let [bb (assert-big-endian-byte-buffer bb)]
    (reify DataInput
      (^void readFully [_ ^bytes ba]
       (let [len (alength ba)]
         (require-readable! bb len)
         (.get bb ba 0 len)
         nil))

      (^void readFully [_ ^bytes ba ^int off ^int len]
       (require-readable! bb len)
       (.get bb ba off len)
       nil)

      (^int skipBytes [_ ^int n]
       (let [n       (max n 0)
             skipped (min n (.remaining bb))]
         (.position bb (+ (.position bb) skipped))
         skipped))

      (^boolean   readBoolean [_] (require-readable! bb 1) (not (zero?   (int (.get bb)))))
      (^byte         readByte [_] (require-readable! bb 1)                    (.get bb))
      (^int  readUnsignedByte [_] (require-readable! bb 1) (bit-and 0xFF (int (.get bb))))

      (^short       readShort [_] (require-readable! bb 2)                      (.getShort bb))
      (^int readUnsignedShort [_] (require-readable! bb 2) (bit-and 0xFFFF (int (.getShort bb))))

      (^char     readChar [_] (require-readable! bb 2) (.getChar   bb))
      (^int       readInt [_] (require-readable! bb 4) (.getInt    bb))
      (^long     readLong [_] (require-readable! bb 8) (.getLong   bb))
      (^float   readFloat [_] (require-readable! bb 4) (.getFloat  bb))
      (^double readDouble [_] (require-readable! bb 8) (.getDouble bb))

      (^String readUTF  [this] (DataInputStream/readUTF this))
      (^String readLine [_]
       (when (.hasRemaining bb)
         (let [sb (StringBuilder.)]
           (loop []
             (if-not (.hasRemaining bb)
               (.toString sb)
               (let [b (bit-and 0xFF (int (.get bb)))]
                 (cond
                   (= b 10) (.toString sb)
                   (= b 13)
                   (do
                     (when (and (.hasRemaining bb) (= 10 (bit-and 0xFF (int (.get bb (.position bb))))))
                       (.position bb (inc (.position bb))))
                     (.toString sb))

                   :else (do (.append sb (char b)) (recur))))))))))))

(defn buffer-data-output
  "Returns a DataOutput adapter over given ByteBuffer.
  Writes at the buffer's current position and advances it."
  ^DataOutput [^ByteBuffer bb]
  (let [bb (assert-big-endian-byte-buffer bb)]
    (reify DataOutput
      (^void write [_ ^bytes ba]
       (let [len (alength ba)]
         (require-writable! bb len)
         (.put bb ba 0 len)
         nil))

      (^void write [_ ^bytes ba ^int off ^int len]
       (require-writable! bb len)
       (.put bb ba off len)
       nil)

      (^void write        [_ ^int     b] (require-writable! bb 1) (.put       bb (unchecked-byte  b))         nil)
      (^void writeBoolean [_ ^boolean b] (require-writable! bb 1) (.put       bb (unchecked-byte (if b 1 0))) nil)
      (^void writeByte    [_ ^int     b] (require-writable! bb 1) (.put       bb (unchecked-byte  b))         nil)
      (^void writeShort   [_ ^int     n] (require-writable! bb 2) (.putShort  bb (unchecked-short n))         nil)
      (^void writeChar    [_ ^int     c] (require-writable! bb 2) (.putChar   bb (unchecked-char  c))         nil)
      (^void writeInt     [_ ^int     n] (require-writable! bb 4) (.putInt    bb n) nil)
      (^void writeLong    [_ ^long    n] (require-writable! bb 8) (.putLong   bb n) nil)
      (^void writeFloat   [_ ^float   n] (require-writable! bb 4) (.putFloat  bb n) nil)
      (^void writeDouble  [_ ^double  n] (require-writable! bb 8) (.putDouble bb n) nil)
      (^void writeUTF     [_ ^String  s] (write-modified-utf! bb s) nil)

      (^void writeBytes [_ ^String s]
       (let [len (.length s)]
         (require-writable! bb len)
         (loop [idx 0]
           (when (< idx len)
             (.put bb (unchecked-byte (int (.charAt s idx))))
             (recur (inc idx))))
         nil))

      (^void writeChars [_ ^String s]
       (let [len (.length s)]
         (require-writable! bb (* 2 len))
         (loop [idx 0]
           (when (< idx len)
             (.putChar bb (.charAt s idx))
             (recur (inc idx))))
         nil))

      )))

(def ^:private -freeze-bb
  "Thread-local ByteBuffer for freeze operations. Grows as needed and is
  reused across calls to avoid per-call allocation."
  (proxy [ThreadLocal] [] (initialValue [] (ByteBuffer/allocate 512))))

(def ^:private -freeze-depth
  "Thread-local nesting depth for BB freeze operations.
  Reentrant freeze calls from custom serializers must not reuse the active
  buffer or they will clobber the parent payload."
  (proxy [ThreadLocal] [] (initialValue [] 0)))

(defn- freeze-raw-with-bb!
  "Freezes x to a raw (headerless) byte array via the BB path using the given
  ByteBuffer. Resets cache state before each retry so an overflow doesn't leave
  stale cache references behind."
  [x ^ByteBuffer bb cache_ cache-state]
  (loop [^ByteBuffer bb bb]
    (when cache_ (vreset! cache_ cache-state))
    (.clear bb)
    (let [out_ (volatile! nil)
          out* (fn [] (or @out_ (vreset! out_ (buffer-data-output bb))))
          result
          (try
            (freeze-with-meta-bb! bb out* x)
            (java.util.Arrays/copyOf (.array bb) (.position bb))
            (catch java.nio.BufferOverflowException _ nil))]
      (if result
        [result bb]
        (recur (ByteBuffer/allocate (* 2 (.capacity bb))))))))

(defn- freeze-raw!
  "Freezes x to a raw (headerless) byte array via the BB path.
  Does NOT manage the cache thread-local — callers are responsible."
  ^bytes [x]
  (let [^ThreadLocal freeze-depth -freeze-depth
        depth (long (.get freeze-depth))
        cache_ (.get -cache-proxy)
        cache-state (when cache_ @cache_)]
    (.set freeze-depth (inc depth))
    (try
      (if (zero? depth)
        (let [^ByteBuffer bb (.get ^ThreadLocal -freeze-bb)
              [result final-bb] (freeze-raw-with-bb! x bb cache_ cache-state)]
          (when-not (identical? final-bb bb)
            (.set ^ThreadLocal -freeze-bb final-bb))
          result)
        ;; Nested freezes need their own ByteBuffer so custom serializers can
        ;; safely call `freeze`, `fast-freeze`, or `freeze-to-out!`.
        (let [^ByteBuffer shared-bb (.get ^ThreadLocal -freeze-bb)]
          (first (freeze-raw-with-bb! x
                   (ByteBuffer/allocate (.capacity shared-bb))
                   cache_ cache-state))))
      (finally
        (.set freeze-depth depth)))))

;; Public `-freeze-with-meta!` with different arg order
(defn freeze-to-out!
  "Serializes arg (any Clojure data type) to a DataOutput.
  This is a low-level util: in most cases you'll want `freeze` instead."
  [^DataOutput data-output x]
  (let [ba (freeze-raw! x)]
    (.write data-output ba 0 (alength ba))))

(defn freeze-to-byte-buffer!
  "Serializes arg (any Clojure data type) to a ByteBuffer.
  This is a low-level util: in most cases you'll want `freeze` instead."
  [^ByteBuffer bb x]
  (let [bb   (assert-big-endian-byte-buffer bb)
        out_ (volatile! nil)
        out* (fn [] (or @out_ (vreset! out_ (buffer-data-output bb))))]
    (try
      (freeze-with-meta-bb! bb out* x)
      (catch java.nio.BufferOverflowException _
        (throw (java.io.EOFException.
                 (str "ByteBuffer overflow while freezing: remaining " (.remaining bb) " bytes.")))))))

;;;; Caching

(defmacro with-cache
  "Executes body with support for freezing/thawing cached values.

  This is a low-level util: you won't need to use this yourself unless
  you're using `freeze-to-out!` or `thaw-from-in!` (also low-level utils).

  See also `cache`."
  [& body]
  `(try
     (.set -cache-proxy (volatile! nil))
     (do ~@body)
     (finally (.remove -cache-proxy))))

(defn cache
  "Wraps value so that future writes of the same wrapped value with same
  metadata will be efficiently encoded as references to this one.

  (freeze [(cache \"foo\") (cache \"foo\") (cache \"foo\")])
    will incl. a single \"foo\", plus 2x single-byte references to \"foo\"."
  [x]
  (if (instance? Cached x) x (Cached. x)))

(comment (cache "foo"))

;;;; Unified byte reader

;; Private implementation detail. Abstracts over ByteBuffer and DataInput
;; so that a single thaw implementation can serve both. Using definterface
;; rather than defprotocol enables the Clojure compiler to emit typed
;; invokevirtual/invokeinterface bytecode when the parameter has a
;; ^IByteReader hint, letting the JIT devirtualize the hot read path.
(definterface ^:private IByteReader
  (^byte    readByte   [])
  (^short   readShort  [])
  (^int     readInt    [])
  (^long    readLong   [])
  (^float   readFloat  [])
  (^double  readDouble [])
  (^char    readChar   [])
  (readFully  [^bytes ba ^int off ^int len])
  (skipBytes  [^int n])
  (toDataInput []))

(deftype ByteBufferReader [^ByteBuffer bb]
  IByteReader
  (readByte   [_] (.get       bb))
  (readShort  [_] (.getShort  bb))
  (readInt    [_] (.getInt    bb))
  (readLong   [_] (.getLong   bb))
  (readFloat  [_] (.getFloat  bb))
  (readDouble [_] (.getDouble bb))
  (readChar   [_] (.getChar   bb))
  (readFully  [_ ^bytes ba ^int off ^int len] (.get bb ba off len))
  (skipBytes  [_ ^int n] (let [pos (.position bb)] (.position bb (+ pos n)) n))
  (toDataInput [_] (buffer-data-input bb)))

(deftype DataInputReader [^DataInput in]
  IByteReader
  (readByte   [_] (.readByte   in))
  (readShort  [_] (.readShort  in))
  (readInt    [_] (.readInt    in))
  (readLong   [_] (.readLong   in))
  (readFloat  [_] (.readFloat  in))
  (readDouble [_] (.readDouble in))
  (readChar   [_] (.readChar   in))
  (readFully  [_ ^bytes ba ^int off ^int len] (.readFully in ba off len))
  (skipBytes  [_ ^int n] (.skipBytes in n))
  (toDataInput [_] in))

(declare thaw-from-in!)
(declare thaw-from-byte-buffer*)
(declare thaw-from-reader!)
(def ^:private thaw-cached-r
  (let [not-found (Object.)]
    (fn [idx ^IByteReader r]
      (if-let [cache_ (.get -cache-proxy)]
        (let [v (get @cache_ idx not-found)]
          (if (identical? v not-found)
            (let [x (thaw-from-reader! r)]
              (vswap! cache_ assoc idx x)
              x)
            v))
        (truss/ex-info! "Can't thaw without cache available. See `with-cache`." {})))))

(comment
  (thaw (freeze [(cache "foo") (cache "foo") (cache "foo")]))
  (let [v1 (with-meta [] {:id :v1})
        v2 (with-meta [] {:id :v2})]
    (mapv meta
      (thaw (freeze [(cache v1) (cache v2) (cache v1) (cache v2)])))))


(extend-type Object
  IFreezable (-freezable? [_] nil)
  IFreezeToDOut
  (-freeze->dout! [x ^DataOutput out]
    (when-debug (println (str "freeze-fallback: " (type x))))
    (if-let [ff *freeze-fallback*]
      (if-not (identical? ff :write-unfreezable)
        (ff out x) ; Modern approach with ff
        (or        ; Legacy approach with ff
          (try-write-serializable out x)
          (try-write-readable     out x)
          (write-unfreezable      out x)))

      ;; Without ff
      (enc/cond
        :let [[r1 e1] (try [(write-serializable out x)] (catch java.nio.BufferOverflowException e (throw e)) (catch Throwable t [nil t]))], r1 r1
        :let [[r2 e2] (try [(write-readable     out x)] (catch java.nio.BufferOverflowException e (throw e)) (catch Throwable t [nil t]))], r2 r2

        :if-let [fff *final-freeze-fallback*] (fff out x) ; Deprecated
        :else
        (let [t (type x)]
          (truss/ex-info! (str "Failed to freeze type: " t)
            (enc/assoc-some
              {:type   t
               :as-str (try-pr-edn x)}
              {:serializable-error e1
               :readable-error     e2})
            (or e1 e2)))))))

;;;;

(def ^:private head-meta-id (reduce-kv #(assoc %1 %3 %2) {} head-meta))
(def ^:private get-head-ba
  (enc/fmemoize
    (fn [head-meta]
      (when-let [meta-id (get head-meta-id (assoc head-meta :version head-version))]
        (enc/ba-concat head-sig (byte-array [meta-id]))))))

(defn- wrap-header [data-ba head-meta]
  (if-let [head-ba (get-head-ba head-meta)]
    (enc/ba-concat head-ba data-ba)
    (truss/ex-info! (str "Unrecognized header meta: " head-meta)
      {:head-meta head-meta})))

(comment (wrap-header (.getBytes "foo") {:compressor-id :lz4
                                         :encryptor-id  nil}))

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
  (enc/qb 1e4
    (call-with-bindings :freeze {}                       (fn [] *freeze-fallback*))
    (call-with-bindings :freeze {:freeze-fallback "foo"} (fn [] *freeze-fallback*))))

(defn fast-freeze
  "Like `freeze` but:
    - Writes data without a Nippy header
    - Drops all support for compression and encryption
    - Must be thawed with `fast-thaw`

  Equivalent to (but a little faster than) `freeze` with opts
    {:no-header? true, :compressor nil, :encryptor nil}.

  Intended for use only by advanced users that clearly understand the tradeoffs.
  I STRONGLY recommend that most users prefer the standard `freeze` since:
    - The Nippy header is useful for data portability and preservation
    - Compression is often benefitial at little/no cost
    - The performance difference between `freeze` and `fast-freeze` is
      often negligible in practice."
  ^bytes [x]
  (.set -cache-proxy (volatile! nil))
  (try (freeze-raw! x) (finally (.remove -cache-proxy))))

(defn freeze
  "Serializes arg (any Clojure data type) to a byte array.
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
             encryptor  (when password encryptor)]

         (.set -cache-proxy (volatile! nil))
         (try
           (let [ba (freeze-raw! x)]
             (if (and (nil? compressor) (nil? encryptor))
               (if no-header?
                 ba
                 (wrap-header ba {:compressor-id nil :encryptor-id nil}))

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
                   ba
                   (wrap-header ba
                     {:compressor-id (when-let [c compressor] (or (compression/standard-header-ids (compression/header-id c)) :else))
                      :encryptor-id  (when-let [e encryptor]  (or (encryption/standard-header-ids  (encryption/header-id  e)) :else))})))))
           (finally (.remove -cache-proxy))))))))

;;;; Thawing

(declare ^:private read-bytes)
(defn- read-bytes-sm* [^DataInput in] (read-bytes in (read-sm-count* in)))
(defn- read-bytes-sm  [^DataInput in] (read-bytes in (read-sm-count  in)))
(defn- read-bytes-md  [^DataInput in] (read-bytes in (read-md-count  in)))
(defn- read-bytes-lg  [^DataInput in] (read-bytes in (read-lg-count  in)))
(defn- read-bytes
  ([^DataInput in len] (let [ba (byte-array len)] (.readFully in ba 0 len) ba))
  ([^DataInput in    ]
   (enc/case-eval (.readByte in)
     id-byte-array-0  (byte-array 0)
     id-byte-array-sm (read-bytes in (read-sm-count in))
     id-byte-array-md (read-bytes in (read-md-count in))
     id-byte-array-lg (read-bytes in (read-lg-count in)))))

(defmacro ^:private read-array [in thaw-type array-type array]
  (let [thawed-sym (with-meta 'thawed-sym {:tag thaw-type})
        array-sym  (with-meta 'array-sym  {:tag array-type})]
    `(let [~array-sym ~array]
       (enc/reduce-n
         (fn [_# idx#]
           (let [~thawed-sym (thaw-from-in! ~in)]
             (aset ~'array-sym idx# ~'thawed-sym)))
         nil (alength ~'array-sym))
       ~'array-sym)))

(defn- read-str-sm* [^DataInput in] (String. ^bytes (read-bytes in (read-sm-count* in)) StandardCharsets/UTF_8))
(defn- read-str-sm  [^DataInput in] (String. ^bytes (read-bytes in (read-sm-count  in)) StandardCharsets/UTF_8))
(defn- read-str-md  [^DataInput in] (String. ^bytes (read-bytes in (read-md-count  in)) StandardCharsets/UTF_8))
(defn- read-str-lg  [^DataInput in] (String. ^bytes (read-bytes in (read-lg-count  in)) StandardCharsets/UTF_8))
(defn- read-str
  ([^DataInput in len] (String. ^bytes (read-bytes in len) StandardCharsets/UTF_8))
  ([^DataInput in    ]
   (enc/case-eval (.readByte in)
     id-str-0  ""
     id-str-sm* (String. ^bytes (read-bytes in (read-sm-count* in)) StandardCharsets/UTF_8)
     id-str-sm_ (String. ^bytes (read-bytes in (read-sm-count  in)) StandardCharsets/UTF_8)
     id-str-md  (String. ^bytes (read-bytes in (read-md-count  in)) StandardCharsets/UTF_8)
     id-str-lg  (String. ^bytes (read-bytes in (read-lg-count  in)) StandardCharsets/UTF_8))))

(defn- read-biginteger [^DataInput in] (BigInteger. ^bytes (read-bytes in (.readInt in))))

(defmacro ^:private editable? [coll] `(instance? clojure.lang.IEditableCollection ~coll))

(defn- xform* [xform] (truss/catching-xform {:error/msg "Error thrown via `*thaw-xform*`"} xform))

(let [rf! (fn rf! ([x] (persistent! x)) ([acc x] (conj! acc x)))
      rf* (fn rf* ([x]              x)  ([acc x] (conj  acc x)))]

  (defn- read-into [to ^DataInput in ^long n]
    (let [transient? (when (editable? to) (> n 10))
          init       (if transient? (transient to) to)
          rf         (if transient? rf! rf*)
          rf         (if-let [xf *thaw-xform*] ((xform* xf) rf) rf)]

      (rf (enc/reduce-n (fn [acc _] (rf acc (thaw-from-in! in))) init n)))))

(let [rf1! (fn rf1! ([x] (persistent! x)) ([acc kv ] (assoc! acc (key kv) (val kv))))
      rf2! (fn rf2! ([x] (persistent! x)) ([acc k v] (assoc! acc      k         v)))
      rf1* (fn rf1* ([x]              x)  ([acc kv ] (assoc  acc (key kv) (val kv))))
      rf2* (fn rf2* ([x]              x)  ([acc k v] (assoc  acc      k         v)))]

  (defn- read-kvs-into [to ^DataInput in ^long n]
    (let [transient? (when (editable? to) (> n 10))
          init       (if transient? (transient to) to)
          rf1        (if transient? rf1! rf1*)
          rf2        (if transient? rf2! rf2*)]

      (if-let [xf *thaw-xform*]
        (let [rf ((xform* xf) rf1)] (rf (enc/reduce-n (fn [acc _] (rf acc (enc/map-entry (thaw-from-in! in) (thaw-from-in! in)))) init n)))
        (let [rf              rf2 ] (rf (enc/reduce-n (fn [acc _] (rf acc                (thaw-from-in! in) (thaw-from-in! in)))  init n)))))))

(defn- read-kvs-depr [to ^DataInput in] (read-kvs-into to in (quot (.readInt in) 2)))

(defn- read-custom! [in prefixed? type-id]
  (if-let [custom-reader (get *custom-readers* type-id)]
    (try
      (custom-reader in)
      (catch Exception e
        (truss/ex-info!
          (str "Reader exception for custom type id: " type-id)
          {:type-id type-id, :prefixed? prefixed?} e)))
    (truss/ex-info!
      (str "No reader provided for custom type id: " type-id)
      {:type-id type-id, :prefixed? prefixed?})))

(defn- read-edn [edn]
  (try
    (enc/read-edn {:readers *data-readers*} edn)
    (catch Exception e
      {:nippy/unthawable
       {:type  :reader
        :cause :exception

        :content   edn
        :exception e}})))

(defn- read-object [^DataInput in class-name]
  (try
    (let [content (.readObject (ObjectInputStream. in))]
      (try
        (let [class (Class/forName class-name)] (cast class content))
        (catch Exception e
          {:nippy/unthawable
           {:type  :serializable
            :cause :exception

            :class-name class-name
            :content    content
            :exception  e}})))

    (catch Exception e
      {:nippy/unthawable
       {:type  :serializable
        :cause :exception

        :class-name class-name
        :content    nil
        :exception  e}})))

(defn read-quarantined-serializable-object-unsafe!
  "Given a quarantined Serializable object like
  {:nippy/unthawable {:class-name <> :content <quarantined-ba>}}, reads and
  returns the object WITHOUT regard for `*thaw-serializable-allowlist*`.

  **MAY BE UNSAFE!** Don't call this unless you absolutely trust the payload
  to not contain any malicious code.

  See `*thaw-serializable-allowlist*` for more info."
  [m]
  (when-let [m (get m :nippy/unthawable)]
    (let [{:keys [class-name content]} m]
      (when (and class-name content)
        (read-object
          (DataInputStream. (ByteArrayInputStream. content))
          class-name)))))

(comment
  (read-quarantined-serializable-object-unsafe!
    (thaw (freeze (java.util.concurrent.Semaphore. 1)))))

(defn- read-sz-quarantined
  "Quarantined => object serialized to ba, then ba written to output stream.
  Has length prefix => can skip `readObject` in event of allowlist failure."
  [^DataInput in class-name]
  (let [quarantined-ba (read-bytes in)]
    (if (thaw-serializable-allowed? class-name)
      (read-object (DataInputStream. (ByteArrayInputStream. quarantined-ba)) class-name)
      {:nippy/unthawable
       {:type  :serializable
        :cause :quarantined

        :class-name class-name
        :content    quarantined-ba}})))

(defn- read-sz-unquarantined
  "Unquarantined => object serialized directly to output stream.
  No length prefix => cannot skip `readObject` in event of allowlist failure."
  [^DataInput in class-name]
  (if (thaw-serializable-allowed? class-name)
    (read-object in class-name)
    (truss/ex-info! ; No way to skip bytes, so best we can do is throw
      "Cannot thaw object: `taoensso.nippy/*thaw-serializable-allowlist*` check failed. This is a security feature. See `*thaw-serializable-allowlist*` docstring or https://github.com/ptaoussanis/nippy/issues/130 for details!"
      {:class-name class-name})))

(let [class-method-sig (into-array Class [IPersistentMap])]
  (defn- read-record [in class-name]
    (let [content (thaw-from-in! in)]
      (try
        (let [c   (clojure.lang.RT/classForName class-name)
              ctr (.getMethod c "create" class-method-sig)]
          (.invoke ctr c (into-array Object [content])))

        (catch Exception e
          {:nippy/unthawable
           {:type  :record
            :cause :exception

            :class-name class-name
            :content    content
            :exception  e}})))))

(defn- read-type [in class-name]
  (try
    (let [c (clojure.lang.RT/classForName class-name)
          num-fields (count (impl/get-basis-fields c))
          field-vals (object-array num-fields)

          ;; Ref. <https://github.com/clojure/clojure/blob/e78519c174fb506afa70e236af509e73160f022a/src/jvm/clojure/lang/Compiler.java#L4799>
          ^Constructor ctr (aget (.getConstructors c) 0)]

      (enc/reduce-n
        (fn [_ i] (aset field-vals i (thaw-from-in! in)))
        nil num-fields)

      (.newInstance ctr field-vals))

    (catch Exception e
      {:nippy/unthawable
       {:type  :type
        :cause :exception

        :class-name class-name
        :exception  e}})))


(defmacro ^:private read-sm-count-r* [r] `(- (int (.readByte  ~r)) Byte/MIN_VALUE))
(defmacro ^:private read-sm-count-r  [r]    `(int (.readByte  ~r)))
(defmacro ^:private read-md-count-r  [r]    `(int (.readShort ~r)))
(defmacro ^:private read-lg-count-r  [r]         `(.readInt   ~r))

(declare ^:private read-bytes-r)
(defn- read-bytes-r
  ([^IByteReader r len]
   (let [len (int len)
         ba  (byte-array len)]
     (.readFully r ba 0 len)
     ba))
  ([^IByteReader r]
   (enc/case-eval (int (.readByte r))
     id-byte-array-0  (byte-array 0)
     id-byte-array-sm (read-bytes-r r (read-sm-count-r r))
     id-byte-array-md (read-bytes-r r (read-md-count-r r))
     id-byte-array-lg (read-bytes-r r (read-lg-count-r r)))))

(defn- read-str-r
  ([^IByteReader r len] (String. ^bytes (read-bytes-r r len) StandardCharsets/UTF_8))
  ([^IByteReader r]
   (enc/case-eval (int (.readByte r))
     id-str-0   ""
     id-str-sm* (String. ^bytes (read-bytes-r r (read-sm-count-r* r)) StandardCharsets/UTF_8)
     id-str-sm_ (String. ^bytes (read-bytes-r r (read-sm-count-r  r)) StandardCharsets/UTF_8)
     id-str-md  (String. ^bytes (read-bytes-r r (read-md-count-r  r)) StandardCharsets/UTF_8)
     id-str-lg  (String. ^bytes (read-bytes-r r (read-lg-count-r  r)) StandardCharsets/UTF_8))))

(defn- read-biginteger-r [^IByteReader r] (BigInteger. ^bytes (read-bytes-r r (.readInt r))))

(defmacro ^:private read-array-r [r thaw-type array-type array]
  (let [thawed-sym (with-meta 'thawed-sym {:tag thaw-type})
        array-sym  (with-meta 'array-sym  {:tag array-type})]
    `(let [~array-sym ~array]
       (enc/reduce-n
         (fn [_# idx#]
           (let [~thawed-sym (thaw-from-reader! ~r)]
             (aset ~'array-sym idx# ~'thawed-sym)))
         nil (alength ~'array-sym))
       ~'array-sym)))

(let [rf! (fn rf! ([x] (persistent! x)) ([acc x] (conj! acc x)))
      rf* (fn rf* ([x]              x)  ([acc x] (conj  acc x)))]

  (defn- read-into-r [to ^IByteReader r ^long n]
    (let [transient? (when (editable? to) (> n 10))
          init       (if transient? (transient to) to)
          rf         (if transient? rf! rf*)
          rf         (if-let [xf *thaw-xform*] ((xform* xf) rf) rf)]
      (rf (enc/reduce-n (fn [acc _] (rf acc (thaw-from-reader! r))) init n)))))

(let [rf1! (fn rf1! ([x] (persistent! x)) ([acc kv ] (assoc! acc (key kv) (val kv))))
      rf2! (fn rf2! ([x] (persistent! x)) ([acc k v] (assoc! acc      k         v)))
      rf1* (fn rf1* ([x]              x)  ([acc kv ] (assoc  acc (key kv) (val kv))))
      rf2* (fn rf2* ([x]              x)  ([acc k v] (assoc  acc      k         v)))]

  (defn- read-kvs-into-r [to ^IByteReader r ^long n]
    (let [transient? (when (editable? to) (> n 10))
          init       (if transient? (transient to) to)
          rf1        (if transient? rf1! rf1*)
          rf2        (if transient? rf2! rf2*)]

      (if-let [xf *thaw-xform*]
        (let [rf ((xform* xf) rf1)] (rf (enc/reduce-n (fn [acc _] (rf acc (enc/map-entry (thaw-from-reader! r) (thaw-from-reader! r)))) init n)))
        (let [rf              rf2]  (rf (enc/reduce-n (fn [acc _] (rf acc                (thaw-from-reader! r) (thaw-from-reader! r)))  init n)))))))

(defn- read-kvs-depr-r [to ^IByteReader r] (read-kvs-into-r to r (quot (.readInt r) 2)))

(let [class-method-sig (into-array Class [IPersistentMap])]
  (defn- read-record-r [^IByteReader r class-name]
    (let [content (thaw-from-reader! r)]
      (try
        (let [c   (clojure.lang.RT/classForName class-name)
              ctr (.getMethod c "create" class-method-sig)]
          (.invoke ctr c (into-array Object [content])))

        (catch Exception e
          {:nippy/unthawable
           {:type  :record
            :cause :exception
            :class-name class-name
            :content    content
            :exception  e}})))))

(defn- read-type-r [^IByteReader r class-name]
  (try
    (let [c          (clojure.lang.RT/classForName class-name)
          num-fields (count (impl/get-basis-fields c))
          field-vals (object-array num-fields)
          ^Constructor ctr (aget (.getConstructors c) 0)]

      (enc/reduce-n
        (fn [_ i] (aset field-vals i (thaw-from-reader! r)))
        nil num-fields)

      (.newInstance ctr field-vals))

    (catch Exception e
      {:nippy/unthawable
       {:type  :type
        :cause :exception
        :class-name class-name
        :exception  e}})))

(defn- read-custom-r [^IByteReader r prefixed? type-id]
  (read-custom! (.toDataInput r) prefixed? type-id))

(defn- thaw-from-reader! [^IByteReader r]
  (let [type-id (int (.readByte r))]
    (when-debug (println (str "thaw-from-reader!: " type-id)))
    (try
      (enc/case-eval type-id
                     id-reader-sm       (read-edn             (read-str-r r (read-sm-count-r r)))
                     id-reader-md       (read-edn             (read-str-r r (read-md-count-r r)))
                     id-reader-lg       (read-edn             (read-str-r r (read-lg-count-r r)))
                     id-reader-lg_      (read-edn             (read-str-r r (read-lg-count-r r)))
                     id-record-sm       (read-record-r    r   (read-str-r r (read-sm-count-r r)))
                     id-record-md       (read-record-r    r   (read-str-r r (read-md-count-r r)))
                     id-record-lg_      (read-record-r    r   (read-str-r r (read-lg-count-r r)))

                     id-sz-quarantined-sm    (read-sz-quarantined   (.toDataInput r) (read-str-r r (read-sm-count-r r)))
                     id-sz-quarantined-md    (read-sz-quarantined   (.toDataInput r) (read-str-r r (read-md-count-r r)))
                     id-sz-unquarantined-sm_ (read-sz-unquarantined (.toDataInput r) (read-str-r r (read-sm-count-r r)))
                     id-sz-unquarantined-md_ (read-sz-unquarantined (.toDataInput r) (read-str-r r (read-md-count-r r)))
                     id-sz-unquarantined-lg_ (read-sz-unquarantined (.toDataInput r) (read-str-r r (read-lg-count-r r)))

                     id-type        (read-type-r r (thaw-from-reader! r))

                     id-nil         nil
                     id-true        true
                     id-false       false
                     id-char        (.readChar r)

                     id-meta-protocol-key meta-protocol-key
                     id-meta
                     (let [m (thaw-from-reader! r)
                           x (thaw-from-reader! r)]
                       (if-let [m (when *incl-metadata?* (not-empty (dissoc m meta-protocol-key)))]
                         (with-meta x m)
                         x))

                     id-cached-0    (thaw-cached-r 0 r)
                     id-cached-1    (thaw-cached-r 1 r)
                     id-cached-2    (thaw-cached-r 2 r)
                     id-cached-3    (thaw-cached-r 3 r)
                     id-cached-4    (thaw-cached-r 4 r)
                     id-cached-5    (thaw-cached-r 5 r)
                     id-cached-6    (thaw-cached-r 6 r)
                     id-cached-7    (thaw-cached-r 7 r)
                     id-cached-sm   (thaw-cached-r (read-sm-count-r r) r)
                     id-cached-md   (thaw-cached-r (read-md-count-r r) r)

                     id-byte-array-0    (byte-array 0)
                     id-byte-array-sm   (read-bytes-r r (read-sm-count-r r))
                     id-byte-array-md   (read-bytes-r r (read-md-count-r r))
                     id-byte-array-lg   (read-bytes-r r (read-lg-count-r r))

                     id-long-array-lg   (read-array-r r long   "[J" (long-array   (read-lg-count-r r)))
                     id-int-array-lg    (read-array-r r int    "[I" (int-array    (read-lg-count-r r)))
                     id-double-array-lg (read-array-r r double "[D" (double-array (read-lg-count-r r)))
                     id-float-array-lg  (read-array-r r float  "[F" (float-array  (read-lg-count-r r)))
                     id-string-array-lg (read-array-r r String "[Ljava.lang.String;" (make-array String (read-lg-count-r r)))
                     id-object-array-lg (read-array-r r Object "[Ljava.lang.Object;" (object-array      (read-lg-count-r r)))

                     id-str-0       ""
                     id-str-sm*              (read-str-r r (read-sm-count-r* r))
                     id-str-sm_              (read-str-r r (read-sm-count-r  r))
                     id-str-md               (read-str-r r (read-md-count-r  r))
                     id-str-lg               (read-str-r r (read-lg-count-r  r))

                     id-kw-sm       (keyword (read-str-r r (read-sm-count-r r)))
                     id-kw-md       (keyword (read-str-r r (read-md-count-r r)))
                     id-kw-md_      (keyword (read-str-r r (read-lg-count-r r)))
                     id-kw-lg_      (keyword (read-str-r r (read-lg-count-r r)))

                     id-sym-sm      (symbol  (read-str-r r (read-sm-count-r r)))
                     id-sym-md      (symbol  (read-str-r r (read-md-count-r r)))
                     id-sym-md_     (symbol  (read-str-r r (read-lg-count-r r)))
                     id-sym-lg_     (symbol  (read-str-r r (read-lg-count-r r)))
                     id-regex       (re-pattern (thaw-from-reader! r))

                     id-vec-0       []
                     id-vec-2       (read-into-r [] r 2)
                     id-vec-3       (read-into-r [] r 3)
                     id-vec-sm*     (read-into-r [] r (read-sm-count-r* r))
                     id-vec-sm_     (read-into-r [] r (read-sm-count-r  r))
                     id-vec-md      (read-into-r [] r (read-md-count-r  r))
                     id-vec-lg      (read-into-r [] r (read-lg-count-r  r))

                     id-set-0       #{}
                     id-set-sm*     (read-into-r    #{} r (read-sm-count-r* r))
                     id-set-sm_     (read-into-r    #{} r (read-sm-count-r  r))
                     id-set-md      (read-into-r    #{} r (read-md-count-r  r))
                     id-set-lg      (read-into-r    #{} r (read-lg-count-r  r))

                     id-map-0       {}
                     id-map-sm*     (read-kvs-into-r {} r (read-sm-count-r* r))
                     id-map-sm_     (read-kvs-into-r {} r (read-sm-count-r  r))
                     id-map-md      (read-kvs-into-r {} r (read-md-count-r  r))
                     id-map-lg      (read-kvs-into-r {} r (read-lg-count-r  r))

                     id-queue-lg      (read-into-r     PersistentQueue/EMPTY r (read-lg-count-r r))
                     id-sorted-set-lg (read-into-r     (sorted-set)          r (read-lg-count-r r))
                     id-sorted-map-lg (read-kvs-into-r (sorted-map)          r (read-lg-count-r r))

                     id-list-0            ()
                     id-list-sm     (into () (rseq (read-into-r [] r (read-sm-count-r r))))
                     id-list-md     (into () (rseq (read-into-r [] r (read-md-count-r r))))
                     id-list-lg     (into () (rseq (read-into-r [] r (read-lg-count-r r))))

                     id-seq-0       (lazy-seq nil)
                     id-seq-sm      (or (seq (read-into-r [] r (read-sm-count-r r))) (lazy-seq nil))
                     id-seq-md      (or (seq (read-into-r [] r (read-md-count-r r))) (lazy-seq nil))
                     id-seq-lg      (or (seq (read-into-r [] r (read-lg-count-r r))) (lazy-seq nil))

                     id-byte              (.readByte  r)
                     id-short             (.readShort r)
                     id-integer           (.readInt   r)
                     id-long-0      0
                     id-long-sm_    (long (.readByte  r))
                     id-long-md_    (long (.readShort r))
                     id-long-lg_    (long (.readInt   r))
                     id-long-xl           (.readLong  r)

                     id-long-pos-sm    (- (long (.readByte  r))    Byte/MIN_VALUE)
                     id-long-pos-md    (- (long (.readShort r))   Short/MIN_VALUE)
                     id-long-pos-lg    (- (long (.readInt   r)) Integer/MIN_VALUE)

                     id-long-neg-sm (- (- (long (.readByte  r))    Byte/MIN_VALUE))
                     id-long-neg-md (- (- (long (.readShort r))   Short/MIN_VALUE))
                     id-long-neg-lg (- (- (long (.readInt   r)) Integer/MIN_VALUE))

                     id-bigint      (bigint (read-biginteger-r r))
                     id-biginteger          (read-biginteger-r r)

                     id-float       (.readFloat  r)
                     id-double-0    0.0
                     id-double      (.readDouble r)

                     id-bigdec      (BigDecimal. ^BigInteger (read-biginteger-r r) (.readInt r))
                     id-ratio       (clojure.lang.Ratio.     (read-biginteger-r r) (read-biginteger-r r))

                     id-map-entry   (enc/map-entry (thaw-from-reader! r) (thaw-from-reader! r))

                     id-util-date   (java.util.Date. (.readLong r))
                     id-sql-date    (java.sql.Date.  (.readLong r))
                     id-uuid        (UUID. (.readLong r) (.readLong r))
                     id-uri         (URI. (thaw-from-reader! r))

                     id-time-instant
                     (let [secs  (.readLong r)
                           nanos (.readInt  r)]
                       (enc/compile-if java.time.Instant
                                       (java.time.Instant/ofEpochSecond secs nanos)
                                       {:nippy/unthawable
                                        {:type       :class
                                         :cause      :class-not-found
                                         :class-name "java.time.Instant"
                                         :content    {:epoch-second secs :nano nanos}}}))

                     id-time-duration
                     (let [secs  (.readLong r)
                           nanos (.readInt  r)]
                       (enc/compile-if java.time.Duration
                                       (java.time.Duration/ofSeconds secs nanos)
                                       {:nippy/unthawable
                                        {:type       :class
                                         :cause      :class-not-found
                                         :class-name "java.time.Duration"
                                         :content    {:seconds secs :nanos nanos}}}))

                     id-time-period
                     (let [years  (.readInt r)
                           months (.readInt r)
                           days   (.readInt r)]
                       (enc/compile-if java.time.Period
                                       (java.time.Period/of years months days)
                                       {:nippy/unthawable
                                        {:type       :class
                                         :cause      :class-not-found
                                         :class-name "java.time.Period"
                                         :content    {:years years :months months :days days}}}))

                     ;; Deprecated ------------------------------------------------------
                     id-boolean_    (not (zero? (int (.readByte r))))
                     id-sorted-map_ (read-kvs-depr-r (sorted-map) r)
                     id-map__       (read-kvs-depr-r {} r)
                     id-reader_     (read-edn (.readUTF ^DataInput (.toDataInput r)))
                     id-str_                  (.readUTF ^DataInput (.toDataInput r))
                     id-kw_         (keyword  (.readUTF ^DataInput (.toDataInput r)))
                     id-map_        (apply hash-map
                                           (enc/repeatedly-into [] (* 2 (.readInt r))
                                                                (fn [] (thaw-from-reader! r))))
                     ;; -----------------------------------------------------------------

                     id-prefixed-custom-md (read-custom-r r :prefixed (.readShort r))

                     (if (neg? type-id)
                       (read-custom-r r nil type-id)
                       (truss/ex-info!
                         (str "Unrecognized type id (" type-id "). Data frozen with newer Nippy version?")
                         {:type-id type-id})))

      (catch Throwable t
        (truss/ex-info! (str "Thaw failed against type-id: " type-id)
                        {:type-id type-id} t)))))

(defn thaw-from-in!
  "Deserializes a frozen object from given DataInput to its original Clojure
  data type.

  This is a low-level util: in most cases you'll want `thaw` instead."
  [^DataInput data-input]
  (thaw-from-reader! (DataInputReader. data-input)))

(defn- thaw-from-byte-buffer*
  [^ByteBuffer bb]
  (thaw-from-reader! (ByteBufferReader. bb)))

(defn thaw-from-byte-buffer!
  "Deserializes a frozen object from given ByteBuffer to its original Clojure
  data type.
  This is a low-level util: in most cases you'll want `thaw` instead."
  [^ByteBuffer bb]
  (assert-big-endian-byte-buffer bb)
  (thaw-from-byte-buffer* bb))

(let [head-sig head-sig] ; Not ^:const
  (defn- try-parse-header [^bytes ba]
    (let [len (alength ba)]
      (when (> len 4)
        (let [-head-sig (java.util.Arrays/copyOf ba 3)]
          (when (java.util.Arrays/equals -head-sig ^bytes head-sig)
            ;; Header appears to be well-formed
            (let [meta-id (aget ba 3)
                  data-ba (java.util.Arrays/copyOfRange ba 4 len)]
              [data-ba (get head-meta meta-id {:unrecognized-meta? true})])))))))

(defn- get-auto-compressor [compressor-id]
  (case compressor-id
    nil        nil
    :snappy    compression/snappy-compressor
    :lzma2     lzma2-compressor
    :lz4       lz4-compressor
    :zstd      zstd-compressor
    :no-header (truss/ex-info! ":auto not supported on headerless data." {})
    :else      (truss/ex-info! ":auto not supported for non-standard compressors." {})
    (do        (truss/ex-info! (str "Unrecognized :auto compressor id: " compressor-id)
                 {:compressor-id compressor-id}))))

(defn- get-auto-encryptor [encryptor-id]
  (case encryptor-id
    nil                nil
    :aes128-gcm-sha512 aes128-gcm-encryptor
    :aes128-cbc-sha512 aes128-cbc-encryptor
    :no-header (truss/ex-info! ":auto not supported on headerless data." {})
    :else      (truss/ex-info! ":auto not supported for non-standard encryptors." {})
    (do        (truss/ex-info! (str "Unrecognized :auto encryptor id: " encryptor-id)
                 {:encryptor-id encryptor-id}))))

(def ^:private err-msg-unknown-thaw-failure "Possible decryption/decompression error, unfrozen/damaged data, etc.")
(def ^:private err-msg-unrecognized-header  "Unrecognized (but apparently well-formed) header. Data frozen with newer Nippy version?")

(defn fast-thaw
  "Like `thaw` but:
    - Supports only data frozen with `fast-freeze`
    - Drops all support for compression and encryption

  Equivalent to (but a little faster than) `thaw` with opts:
    {:no-header? true, :compressor nil, :encryptor nil}."
  [^bytes ba]
  (with-cache (thaw-from-byte-buffer* (ByteBuffer/wrap ba))))

(defn thaw
  "Deserializes a frozen Nippy byte array to its original Clojure data type.
  To thaw custom types, extend the Clojure reader or see `extend-thaw`.

  ** By default, supports data frozen with Nippy v2+ ONLY **
  Add `{:v1-compatibility? true}` option to support thawing of data frozen with
  legacy versions of Nippy.

  Options include:
    :v1-compatibility? - support data frozen by legacy versions of Nippy?
    :compressor - :auto (checks header, default)  an ICompressor, or nil
    :encryptor  - :auto (checks header, default), an IEncryptor,  or nil"
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
                     '*freeze-fallback*             *freeze-fallback*
                     '*final-freeze-fallback*       *final-freeze-fallback*
                     '*auto-freeze-compressor*      *auto-freeze-compressor*
                     '*custom-readers*              *custom-readers*
                     '*incl-metadata?*              *incl-metadata?*
                     '*thaw-serializable-allowlist* *thaw-serializable-allowlist*
                     '*thaw-xform*                  *thaw-xform*)}

                  e)))

             thaw-data
             (fn [data-ba compressor-id encryptor-id ex-fn]
               (let [compressor (if (identical? compressor :auto) (get-auto-compressor compressor-id) compressor)
                     encryptor  (if (identical? encryptor  :auto) (get-auto-encryptor  encryptor-id)  encryptor)]

                 (when (and encryptor (not password))
                   (ex "Password required for decryption."))

                 (try
                   (let [ba data-ba
                         ba (if encryptor  (decrypt    encryptor password ba) ba)
                         ba (if compressor (decompress compressor         ba) ba)]

                     (with-cache (thaw-from-byte-buffer* (ByteBuffer/wrap ba))))

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
                              :as   head-meta}] (try-parse-header ba)]

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

;;;; Custom types

(defn- assert-custom-type-id [custom-type-id]
  (assert (or      (keyword? custom-type-id)
              (and (integer? custom-type-id) (<= 1 custom-type-id 128)))))

(defn- coerce-custom-type-id
  "* +ive byte id ->  -ive byte id (for unprefixed custom types)
   *   Keyword id -> Short hash id (for   prefixed custom types)"
  [custom-type-id]
  (assert-custom-type-id custom-type-id)
  (if-not (keyword? custom-type-id)
    (int (- ^long custom-type-id))
    (let [^int hash-id  (hash custom-type-id)
          short-hash-id (if (pos? hash-id)
                          (mod hash-id Short/MAX_VALUE)
                          (mod hash-id Short/MIN_VALUE))]

      ;; Make sure hash ids can't collide with byte ids (unlikely anyway):
      (assert (not (<= Byte/MIN_VALUE short-hash-id -1))
        "Custom type id hash collision; please choose a different id")

      (int short-hash-id))))

(comment (coerce-custom-type-id 77)
         (coerce-custom-type-id :foo/bar))

(defmacro extend-freeze
  "Extends Nippy to support freezing of a custom type (ideally concrete) with
  given id of form:

    * ℕ∈[1, 128]           - 0 byte overhead. You are responsible for managing ids.
    * (Namespaced) keyword - 2 byte overhead. Keyword will be hashed to 16 bit int,
                             collisions will throw at compile-time.

  NB: be careful about extending to interfaces, Ref. <http://goo.gl/6gGRlU>.

  (defrecord MyRec [data])
  (extend-freeze MyRec :foo/my-type [x data-output] ; Keyword id
    (.writeUTF [data-output] (:data x)))
  ;; or
  (extend-freeze MyRec 1 [x data-output] ; Byte id
    (.writeUTF [data-output] (:data x)))"

  [type custom-type-id [x out] & body]
  (assert-custom-type-id custom-type-id)
  (let [write-id-form
        (if (keyword? custom-type-id)
          ;; Prefixed [const byte id][cust hash id][payload]:
          `(do (write-id    ~out ~id-prefixed-custom-md)
               (.writeShort ~out ~(coerce-custom-type-id custom-type-id)))
          ;; Unprefixed [cust byte id][payload]:
          `(write-id ~out ~(coerce-custom-type-id custom-type-id)))]

    `(extend-type ~type
       IFreezable (~'-freezable? [~'x] true)
       IFreezeToDOut
       (~'-freeze->dout! [~x ~(with-meta out {:tag 'java.io.DataOutput})] ~write-id-form ~@body))))

(defmacro extend-thaw
  "Extends Nippy to support thawing of a custom type with given id:
  (extend-thaw :foo/my-type [data-input] ; Keyword id
    (MyRec. (.readUTF data-input)))
  ;; or
  (extend-thaw 1 [data-input] ; Byte id
    (MyRec. (.readUTF data-input)))"
  [custom-type-id [in] & body]
  (assert-custom-type-id custom-type-id)
  `(do
     (when (contains? *custom-readers* ~(coerce-custom-type-id custom-type-id))
       (println (str "Warning: resetting Nippy thaw for custom type with id: "
                  ~custom-type-id)))
     (alter-var-root #'*custom-readers*
       (fn [m#]
         (assoc m#
           ~(coerce-custom-type-id custom-type-id)
           (fn [~(with-meta in {:tag 'java.io.DataInput})]
             ~@body))))))

(comment
  *custom-readers*
  (defrecord MyRec [data])
  (extend-freeze MyRec 1 [x out] (.writeUTF out (:data x)))
  (extend-thaw 1 [in] (MyRec. (.readUTF in)))
  (thaw (freeze (MyRec. "Joe"))))

;;;; Stress data

(defrecord StressRecord [x])
(deftype   StressType [x ^:unsynchronized-mutable y]
  clojure.lang.IDeref (deref [_] [x y])
  Object (equals [_ other] (and (instance? StressType other) (= [x y] @other))))

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

;;;; Tools

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
       (or (-freezable? x) (natively-freezable? x))
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
           [data-ba ?nippy-header] (or (try-parse-header unwrapped-ba)
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

(defn freeze-to-string
  "Convenience util: like `freeze`, but returns a Base64-encoded string.
  See also `thaw-from-string`."
  ([x            ] (freeze-to-string x nil))
  ([x freeze-opts]
   (let [ba (freeze x freeze-opts)]
     (.encodeToString (java.util.Base64/getEncoder)
       ba))))

(defn thaw-from-string
  "Convenience util: like `thaw`, but takes a Base64-encoded string.
  See also `freeze-to-string`."
  ([s                  ] (thaw-from-string s nil))
  ([^String s thaw-opts]
   (let [ba (.decode (java.util.Base64/getDecoder) s)]
     (thaw ba thaw-opts))))

(comment (thaw-from-string (freeze-to-string {:a :A :b [:B1 :B2]})))

(defn freeze-to-file
  "Convenience util: like `freeze`, but writes to `(clojure.java.io/file <file>)`."
  ([file x            ] (freeze-to-file file x nil))
  ([file x freeze-opts]
   (let [^bytes ba (freeze x freeze-opts)]
     (with-open [out (jio/output-stream (jio/file file))]
       (.write out ba))
     ba)))

(defn thaw-from-file
  "Convenience util: like `thaw`, but reads from `(clojure.java.io/file <file>)`."
  ([file          ] (thaw-from-file file nil))
  ([file thaw-opts]
   (let [file (jio/file file)
         frozen-ba
         (let [ba (byte-array (.length file))]
           (with-open [in (DataInputStream. (jio/input-stream file))]
             (.readFully in ba)
             (do            ba)))]

     (thaw frozen-ba thaw-opts))))

(defn thaw-from-resource
  "Convenience util: like `thaw`, but reads from `(clojure.java.io/resource <res>)`."
  ([res          ] (thaw-from-resource res nil))
  ([res thaw-opts]
   (let [res (jio/resource res)
         frozen-ba
         (with-open [in  (jio/input-stream res)
                     out (ByteArrayOutputStream.)]
           (jio/copy in  out)
           (.toByteArray out))]

     (thaw frozen-ba thaw-opts))))

(comment
  (freeze-to-file "resources/foo.npy" "hello, world!")
  (thaw-from-file "resources/foo.npy")
  (thaw-from-resource       "foo.npy"))

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

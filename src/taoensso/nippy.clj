(ns taoensso.nippy
  "High-performance serialization library for Clojure."
  {:author "Peter Taoussanis (@ptaoussanis)"}
  (:require
   [clojure.string  :as str]
   [clojure.java.io :as jio]
   [taoensso.encore :as enc]
   [taoensso.nippy
    [impl        :as impl]
    [compression :as compression]
    [encryption  :as encryption]])

  (:import
   [java.nio.charset StandardCharsets]
   [java.io ByteArrayInputStream ByteArrayOutputStream DataInputStream
    DataOutputStream Serializable ObjectOutputStream ObjectInputStream
    DataOutput DataInput]
   [java.lang.reflect Method Field Constructor]
   [java.net URI]
   [java.util #_Date UUID]
   [java.util.regex Pattern]
   [clojure.lang Keyword Symbol BigInt Ratio
    APersistentMap APersistentVector APersistentSet
    IPersistentMap ; IPersistentVector IPersistentSet IPersistentList
    PersistentQueue PersistentTreeMap PersistentTreeSet PersistentList
    MapEntry LazySeq IRecord ISeq IType]))

(enc/assert-min-encore-version [3 98 0])

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

   53  [:bytes-0   []]
   7   [:bytes-sm  [[:bytes {:read 1}]]]
   15  [:bytes-md  [[:bytes {:read 2}]]]
   2   [:bytes-lg  [[:bytes {:read 4}]]]

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
   115 [:objects-lg    [[:elements {:read 4}]]]

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
      - [[:bytes 4]]               ; Type has a payload of exactly 4 bytes
      - [[:bytes 2] [:elements 2]] ; Type has a payload of exactly 2 bytes, then
                                   ; 2 elements

      - [[:bytes    {:read 2}]
         [:elements {:read 4 :multiplier 2 :unsigned? true}]]

        ; Type has payload of <short-count> bytes, then
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
  "Controls Nippy's behaviour when trying to freeze an item for which Nippy
  doesn't currently have a native freeze/thaw implementation.

  Possible values:

    1. `nil` (no freeze-fallback, default)
       Tries the following in order:
         - Freeze with Java's `Serializable` interface if possible
         - Freeze with Clojure's reader                if possible
         - Throw

    2. `:write-unfreezable` keyword
       Tries the following in order:
         - Freeze with Java's `Serializable` interface if possible
         - Freeze with Clojure's reader                if possible
         - Freeze a {:nippy/unfreezable {:type _}} placeholder value

    3. [Advanced] Custom (fn [^java.io.DataOutput out item]) that must
       write exactly one value to the given `DataOutput` stream"

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
  "Allows *any* class-name to be frozen using Java's `Serializable` interface.
  This is generally safe since RCE risk is present only when thawing.
  See also `*freeze-serializable-allowlist*`."
  #{"*"})

(def default-thaw-serializable-allowlist
  "A set of common safe class-names to allow to be frozen using Java's
  `Serializable` interface. PRs welcome for additions.
  See also `*thaw-serializable-allowlist*`."
  #{"[I" "[F" "[Z" "[B" "[C" "[D" "[S" "[J"

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
    - `#{\"java.lang.Throwable\", \"clojure.lang.*\"}` ; Set of class-names
    - `\"allow-and-record\"`                           ; Special value, see [2]

    Note that class-names in sets may contain \"*\" wildcards.

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

;;;; Freezing interface

(defprotocol  IFreezable
  "Private implementation detail.
  Protocol that types must implement to support native freezing by Nippy.
  Don't use this directly, instead see `extend-freeze`."
  (-freezable?           [_])
  (-freeze-without-meta! [_ data-output]))

(defprotocol IFreezableWithMeta
  "Private implementation detail.
  Wrapper protocol around `IFreezable` used to handle possible metadata."
  (-freeze-with-meta! [_ data-output]))

(defmacro write-id [out id] `(.writeByte ~out ~id))

(declare write-map)

(extend-protocol IFreezableWithMeta
  clojure.lang.IObj ; IMeta => `meta` will work, IObj => `with-meta` will work
  (-freeze-with-meta! [x ^DataOutput data-output]
    (when-let [m (when *incl-metadata?* (not-empty (meta x)))]
      (write-id  data-output id-meta)
      (write-map data-output m :is-metadata))
    (-freeze-without-meta! x data-output))

  nil    (-freeze-with-meta! [x data-output] (-freeze-without-meta! x data-output))
  Object (-freeze-with-meta! [x data-output] (-freeze-without-meta! x data-output)))

(defmacro ^:private freezer
  [type id freezable? form]
  (let [id-form (when id `(write-id ~'out ~id))]
    `(extend-type ~type
       IFreezable
       (~'-freezable?           [~'x] ~freezable?)
       (~'-freeze-without-meta! [~'x ~(with-meta 'out {:tag 'DataOutput})]
        ~id-form ~form))))

;;;; Freezing

(do
  (def ^:private ^:const range-ubyte  (-    Byte/MAX_VALUE    Byte/MIN_VALUE))
  (def ^:private ^:const range-ushort (-   Short/MAX_VALUE   Short/MIN_VALUE))
  (def ^:private ^:const range-uint   (- Integer/MAX_VALUE Integer/MIN_VALUE))
  
  (defmacro ^:private sm-count?* [n] `(<= ~n     range-ubyte)) ; Unsigned
  (defmacro ^:private sm-count?  [n] `(<= ~n  Byte/MAX_VALUE))
  (defmacro ^:private md-count?  [n] `(<= ~n Short/MAX_VALUE))

  (defmacro ^:private write-sm-count* [out n] `(.writeByte  ~out (+ ~n Byte/MIN_VALUE)))
  (defmacro ^:private write-sm-count  [out n] `(.writeByte  ~out    ~n))
  (defmacro ^:private write-md-count  [out n] `(.writeShort ~out    ~n))
  (defmacro ^:private write-lg-count  [out n] `(.writeInt   ~out    ~n))

  (defmacro ^:private read-sm-count* [in] `(- (.readByte  ~in) Byte/MIN_VALUE))
  (defmacro ^:private read-sm-count  [in]    `(.readByte  ~in))
  (defmacro ^:private read-md-count  [in]    `(.readShort ~in))
  (defmacro ^:private read-lg-count  [in]    `(.readInt   ~in)))

(defn- write-bytes-sm* [^DataOutput out ^bytes ba] (let [len (alength ba)] (write-sm-count* out len) (.write out ba 0 len)))
(defn- write-bytes-sm  [^DataOutput out ^bytes ba] (let [len (alength ba)] (write-sm-count  out len) (.write out ba 0 len)))
(defn- write-bytes-md  [^DataOutput out ^bytes ba] (let [len (alength ba)] (write-md-count  out len) (.write out ba 0 len)))
(defn- write-bytes-lg  [^DataOutput out ^bytes ba] (let [len (alength ba)] (write-lg-count  out len) (.write out ba 0 len)))
(defn- write-bytes     [^DataOutput out ^bytes ba]
  (let [len (alength ba)]
    (if (zero? len)
      (write-id out id-bytes-0)
      (do
        (enc/cond
          (sm-count? len) (do (write-id out id-bytes-sm) (write-sm-count out len))
          (md-count? len) (do (write-id out id-bytes-md) (write-md-count out len))
          :else           (do (write-id out id-bytes-lg) (write-lg-count out len)))

        (.write out ba 0 len)))))

(defn- write-biginteger [out ^BigInteger n] (write-bytes-lg out (.toByteArray n)))

(defn- write-str-sm* [^DataOutput out ^String s] (write-bytes-sm* out (.getBytes s StandardCharsets/UTF_8)))
(defn- write-str-sm  [^DataOutput out ^String s] (write-bytes-sm  out (.getBytes s StandardCharsets/UTF_8)))
(defn- write-str-md  [^DataOutput out ^String s] (write-bytes-md  out (.getBytes s StandardCharsets/UTF_8)))
(defn- write-str-lg  [^DataOutput out ^String s] (write-bytes-lg  out (.getBytes s StandardCharsets/UTF_8)))
(defn- write-str     [^DataOutput out ^String s]
  (if (identical? s "")
    (write-id out id-str-0)
    (let [ba  (.getBytes s StandardCharsets/UTF_8)
          len (alength ba)]
      (enc/cond
        (sm-count?* len) (do (write-id out id-str-sm*) (write-sm-count* out len))
        (md-count?  len) (do (write-id out id-str-md)  (write-md-count  out len))
        :else            (do (write-id out id-str-lg)  (write-lg-count  out len)))

      (.write out ba 0 len))))

(defn- write-kw [^DataOutput out kw]
  (let [s   (if-let [ns (namespace kw)] (str ns "/" (name kw)) (name kw))
        ba  (.getBytes s StandardCharsets/UTF_8)
        len (alength ba)]
    (enc/cond
      (sm-count? len) (do (write-id out id-kw-sm) (write-sm-count out len))
      (md-count? len) (do (write-id out id-kw-md) (write-md-count out len))
      ;; :else        (do (write-id out id-kw-lg) (write-lg-count out len)) ; Unrealistic
      :else           (throw (ex-info "Keyword too long" {:name s})))

    (.write out ba 0 len)))

(defn- write-sym [^DataOutput out s]
  (let [s   (if-let [ns (namespace s)] (str ns "/" (name s)) (name s))
        ba  (.getBytes s StandardCharsets/UTF_8)
        len (alength ba)]
    (enc/cond
      (sm-count? len) (do (write-id out id-sym-sm) (write-sm-count out len))
      (md-count? len) (do (write-id out id-sym-md) (write-md-count out len))
      ;; :else        (do (write-id out id-sym-lg) (write-lg-count out len)) ; Unrealistic
      :else           (throw (ex-info "Symbol too long" {:name s})))

    (.write out ba 0 len)))

(defn- write-long [^DataOutput out ^long n]
  (enc/cond
    (zero? n) (write-id out id-long-0)
    (pos?  n)
    (enc/cond
      (<= n range-ubyte)  (do (write-id out id-long-pos-sm) (.writeByte  out (+ n    Byte/MIN_VALUE)))
      (<= n range-ushort) (do (write-id out id-long-pos-md) (.writeShort out (+ n   Short/MIN_VALUE)))
      (<= n range-uint)   (do (write-id out id-long-pos-lg) (.writeInt   out (+ n Integer/MIN_VALUE)))
      :else               (do (write-id out id-long-xl)     (.writeLong  out    n)))

    :else
    (let [y (- n)]
      (enc/cond
        (<= y range-ubyte)  (do (write-id out id-long-neg-sm) (.writeByte  out (+ y    Byte/MIN_VALUE)))
        (<= y range-ushort) (do (write-id out id-long-neg-md) (.writeShort out (+ y   Short/MIN_VALUE)))
        (<= y range-uint)   (do (write-id out id-long-neg-lg) (.writeInt   out (+ y Integer/MIN_VALUE)))
        :else               (do (write-id out id-long-xl)     (.writeLong  out    n))))))

(defmacro ^:private -run!    [proc coll] `(do (reduce    #(~proc %2)    nil ~coll) nil))
(defmacro ^:private -run-kv! [proc    m] `(do (reduce-kv #(~proc %2 %3) nil    ~m) nil))

(defn- write-vec [^DataOutput out v]
  (let [cnt (count v)]
    (if (zero? cnt)
      (write-id out id-vec-0)
      (do
        (enc/cond
          (sm-count?* cnt)
          (enc/cond
            (== cnt 2) (write-id out id-vec-2)
            (== cnt 3) (write-id out id-vec-3)
            :else  (do (write-id out id-vec-sm*) (write-sm-count* out cnt)))

          (md-count? cnt) (do (write-id out id-vec-md) (write-md-count out cnt))
          :else           (do (write-id out id-vec-lg) (write-lg-count out cnt)))

        (-run! (fn [in] (-freeze-with-meta! in out)) v)))))

(defn- write-kvs
  ([^DataOutput out id-lg coll]
   (let [cnt (count coll)]
     (write-id       out id-lg)
     (write-lg-count out cnt)
     (-run-kv!
       (fn [k v]
         (-freeze-with-meta! k out)
         (-freeze-with-meta! v out))
       coll)))

  ([^DataOutput out id-empty id-sm id-md id-lg coll]
   (let [cnt (count coll)]
     (if (zero? cnt)
       (write-id out id-empty)
       (do
         (enc/cond
           (sm-count? cnt) (do (write-id out id-sm) (write-sm-count out cnt))
           (md-count? cnt) (do (write-id out id-md) (write-md-count out cnt))
           :else           (do (write-id out id-lg) (write-lg-count out cnt)))

         (-run-kv!
           (fn [k v]
             (-freeze-with-meta! k out)
             (-freeze-with-meta! v out))
           coll))))))

(defn- write-counted-coll
  ([^DataOutput out id-lg coll]
   (let [cnt (count coll)]
     ;; (assert (counted? coll))
     (write-id       out id-lg)
     (write-lg-count out cnt)
     (-run! (fn [in] (-freeze-with-meta! in out)) coll)))

  ([^DataOutput out id-empty id-sm id-md id-lg coll]
   (let [cnt (count coll)]
     ;; (assert (counted? coll))
     (if (zero? cnt)
       (write-id out id-empty)
       (do
         (enc/cond
           (sm-count? cnt) (do (write-id out id-sm) (write-sm-count out cnt))
           (md-count? cnt) (do (write-id out id-md) (write-md-count out cnt))
           :else           (do (write-id out id-lg) (write-lg-count out cnt)))

         (-run! (fn [in] (-freeze-with-meta! in out)) coll))))))

(defn- write-uncounted-coll
  ([^DataOutput out id-lg coll]
   ;; (assert (not (counted? coll)))
   (let [bas  (ByteArrayOutputStream. 32)
         sout (DataOutputStream. bas)
         ^long cnt (reduce (fn [^long cnt in] (-freeze-with-meta! in sout) (unchecked-inc cnt)) 0 coll)
         ba   (.toByteArray bas)]

     (write-id       out id-lg)
     (write-lg-count out cnt)
     (.write         out ba)))

  ([^DataOutput out id-empty id-sm id-md id-lg coll]
   (let [bas  (ByteArrayOutputStream. 32)
         sout (DataOutputStream. bas)
         ^long cnt (reduce (fn [^long cnt in] (-freeze-with-meta! in sout) (unchecked-inc cnt)) 0 coll)
         ba   (.toByteArray bas)]

     (if (zero? cnt)
       (write-id out id-empty)
       (do
         (enc/cond
           (sm-count? cnt) (do (write-id out id-sm) (write-sm-count out cnt))
           (md-count? cnt) (do (write-id out id-md) (write-md-count out cnt))
           :else           (do (write-id out id-lg) (write-lg-count out cnt)))

         (.write out ba))))))

(defn- write-coll
  ([out id-lg coll]
   (if (counted? coll)
     (write-counted-coll   out id-lg coll)
     (write-uncounted-coll out id-lg coll)))

  ([out id-empty id-sm id-md id-lg coll]
   (if (counted? coll)
     (write-counted-coll   out id-empty id-sm id-md id-lg coll)
     (write-uncounted-coll out id-empty id-sm id-md id-lg coll))))

;; Micro-optimization:
;; As (write-kvs out id-map-0 id-map-sm id-map-md id-map-lg x)
(defn- write-map [^DataOutput out m is-metadata?]
  (let [cnt (count m)]
    (if (zero? cnt)
      (write-id out id-map-0)
      (do
        (enc/cond
          (sm-count?* cnt) (do (write-id out id-map-sm*) (write-sm-count* out cnt))
          (md-count?  cnt) (do (write-id out id-map-md)  (write-md-count  out cnt))
          :else            (do (write-id out id-map-lg)  (write-lg-count  out cnt)))

        (-run-kv!
          (fn [k v]
            (if (and is-metadata? (fn? v) (qualified-symbol? k))
              (do
                ;; Strip Clojure v1.10+ metadata protocol extensions
                ;; (used by defprotocol `:extend-via-metadata`)
                (write-id out id-meta-protocol-key)
                (write-id out id-nil))
              (do
                (-freeze-with-meta! k out)
                (-freeze-with-meta! v out))))
          m)))))

(comment (meta (thaw (freeze (with-meta [] {:a :A, 'b/c (fn [])})))))

;; Micro-optimization:
;; As (write-counted-coll out id-set-0 id-set-sm id-set-md id-set-lg x)
(defn- write-set [^DataOutput out s]
  (let [cnt (count s)]
    (if (zero? cnt)
      (write-id out id-set-0)
      (do
        (enc/cond
          (sm-count?* cnt) (do (write-id out id-set-sm*) (write-sm-count* out cnt))
          (md-count?  cnt) (do (write-id out id-set-md)  (write-md-count  out cnt))
          :else            (do (write-id out id-set-lg)  (write-lg-count  out cnt)))

        (-run! (fn [in] (-freeze-with-meta! in out)) s)))))

(defn- write-objects [^DataOutput out ^objects ary]
  (let [len (alength ary)]
    (write-id       out id-objects-lg)
    (write-lg-count out len)
    (-run! (fn [in] (-freeze-with-meta! in out)) ary)))

(defn- write-serializable [^DataOutput out x ^String class-name]
  (when-debug (println (str "write-serializable: " (type x))))
  (let [class-name-ba (.getBytes class-name StandardCharsets/UTF_8)
        len           (alength   class-name-ba)]

    (enc/cond
      (sm-count? len) (do (write-id out id-sz-quarantined-sm) (write-bytes-sm out class-name-ba))
      (md-count? len) (do (write-id out id-sz-quarantined-md) (write-bytes-md out class-name-ba))
      ;; :else        (do (write-id out id-sz-quarantined-lg) (write-bytes-md out class-name-ba)) ; Unrealistic
      :else           (throw (ex-info "Serializable class name too long" {:name class-name})))

    ;; Legacy: write object directly to out.
    ;; (.writeObject (ObjectOutputStream. out) x)

    ;; Quarantined: write object to ba, then ba to out.
    ;; We'll have object length during thaw, allowing us to skip readObject.
    (let [quarantined-ba (ByteArrayOutputStream.)]
      (.writeObject (ObjectOutputStream. (DataOutputStream. quarantined-ba)) x)
      (write-bytes out (.toByteArray quarantined-ba)))))

(defn- write-readable [^DataOutput out x]
  (when-debug (println (str "write-readable: " (type x))))
  (let [edn    (enc/pr-edn x)
        edn-ba (.getBytes ^String edn StandardCharsets/UTF_8)
        len    (alength edn-ba)]
    (enc/cond
      (sm-count? len) (do (write-id out id-reader-sm) (write-bytes-sm out edn-ba))
      (md-count? len) (do (write-id out id-reader-md) (write-bytes-md out edn-ba))
      :else           (do (write-id out id-reader-lg) (write-bytes-lg out edn-ba)))))

(defn try-write-serializable [out x]
  (when (and (instance? Serializable x) (not (fn? x)))
    (try
      (let [class-name (.getName (class x))] ; Reflect
        (when (freeze-serializable-allowed? class-name)
          (write-serializable out x class-name)
          true))
      (catch Throwable _ nil))))

(defn try-write-readable [out x]
  (when (impl/seems-readable? x)
    (try
      (write-readable out x)
      true
      (catch Throwable _ nil))))

(defn- try-pr-edn [x]
  (try
    (enc/pr-edn x)
    (catch Throwable _
      (try
        (str x)
        (catch Throwable _
          :nippy/unprintable)))))

(defn write-unfreezable [out x]
  (-freeze-without-meta!
    {:nippy/unfreezable
     {:type    (type       x)
      :content (try-pr-edn x)}}
    out))

(defn throw-unfreezable [x]
  (let [t (type x)]
    (throw
      (ex-info (str "Unfreezable type: " t)
        {:type   t
         :as-str (try-pr-edn x)}))))

;; Public `-freeze-with-meta!` with different arg order
(defn freeze-to-out!
  "Serializes arg (any Clojure data type) to a DataOutput.
  This is a low-level util: in most cases you'll want `freeze` instead."
  [^DataOutput data-output x] (-freeze-with-meta! x data-output))

;;;; Caching ; Experimental

;; Nb: don't use an auto initialValue; can cause thread-local state to
;; accidentally hang around with the use of `freeze-to-out!`, etc.
;; Safer to require explicit activation through `with-cache`.
(def ^ThreadLocal -cache-proxy
  "{[<x> <meta>] <idx>} for freezing, {<idx> <x-with-meta>} for thawing."
  (proxy [ThreadLocal] []))

(defmacro ^:private with-cache
  "Executes body with support for freezing/thawing cached values.

  This is a low-level util: you won't need to use this yourself unless
  you're using `freeze-to-out!` or `thaw-from-in!` (also low-level utils).

  See also `cache`."
  [& body]
  `(try
     (.set -cache-proxy (volatile! nil))
     (do ~@body)
     (finally (.remove -cache-proxy))))

(deftype Cached [val])
(defn cache
  "Experimental, subject to change. Feedback welcome!

  Wraps value so that future writes of the same wrapped value with same
  metadata will be efficiently encoded as references to this one.

  (freeze [(cache \"foo\") (cache \"foo\") (cache \"foo\")])
    will incl. a single \"foo\", plus 2x single-byte references to \"foo\"."
  [x]
  (if (instance? Cached x) x (Cached. x)))

(comment (cache "foo"))

(freezer Cached nil true
  (let [x-val (.-val x)]
    (if-let [cache_ (.get -cache-proxy)]
      (let [cache @cache_
            k     #_x-val [x-val (meta x-val)] ; Also check meta for equality
            ?idx  (get cache k)
            ^int idx
            (or ?idx
              (let [idx (count cache)]
                (vswap! cache_ assoc k idx)
                idx))

            first-occurance? (nil? ?idx)]

        (enc/cond
          (sm-count? idx)
          (case (int idx)
            0 (do (write-id out id-cached-0) (when first-occurance? (-freeze-with-meta! x-val out)))
            1 (do (write-id out id-cached-1) (when first-occurance? (-freeze-with-meta! x-val out)))
            2 (do (write-id out id-cached-2) (when first-occurance? (-freeze-with-meta! x-val out)))
            3 (do (write-id out id-cached-3) (when first-occurance? (-freeze-with-meta! x-val out)))
            4 (do (write-id out id-cached-4) (when first-occurance? (-freeze-with-meta! x-val out)))
            5 (do (write-id out id-cached-5) (when first-occurance? (-freeze-with-meta! x-val out)))
            6 (do (write-id out id-cached-6) (when first-occurance? (-freeze-with-meta! x-val out)))
            7 (do (write-id out id-cached-7) (when first-occurance? (-freeze-with-meta! x-val out)))

            (do
              (write-id       out id-cached-sm)
              (write-sm-count out idx)
              (when first-occurance? (-freeze-with-meta! x-val out))))

          (md-count? idx)
          (do
            (write-id       out id-cached-md)
            (write-md-count out idx)
            (when first-occurance? (-freeze-with-meta! x-val out)))

          :else
          ;; (throw (ex-info "Max cache size exceeded" {:idx idx}))
          (-freeze-with-meta! x-val out) ; Just freeze uncached
          ))

      (-freeze-with-meta! x-val out))))

(declare thaw-from-in!)
(def ^:private thaw-cached
  (let [not-found (Object.)]
    (fn [idx in]
      (if-let [cache_ (.get -cache-proxy)]
        (let [v (get @cache_ idx not-found)]
          (if (identical? v not-found)
            (let [x (thaw-from-in! in)]
              (vswap! cache_ assoc idx x)
              x)
            v))
        (throw (ex-info "No cache_ established, can't thaw. See `with-cache`." {}))))))

(comment
  (thaw (freeze [(cache "foo") (cache "foo") (cache "foo")]))
  (let [v1 (with-meta [] {:id :v1})
        v2 (with-meta [] {:id :v2})]
    (mapv meta
      (thaw (freeze [(cache v1) (cache v2) (cache v1) (cache v2)])))))

;;;;

(freezer nil        id-nil        true nil)
(freezer (type ())  id-list-0     true nil)
(freezer Character  id-char       true (.writeChar       out (int x)))
(freezer Byte       id-byte       true (.writeByte       out x))
(freezer Short      id-short      true (.writeShort      out x))
(freezer Integer    id-integer    true (.writeInt        out x))
(freezer BigInt     id-bigint     true (write-biginteger out (.toBigInteger x)))
(freezer BigInteger id-biginteger true (write-biginteger out x))
(freezer Pattern    id-regex      true (write-str        out (str x)))
(freezer Float      id-float      true (.writeFloat      out x))
(freezer BigDecimal id-bigdec     true
  (do
    (write-biginteger out (.unscaledValue x))
    (.writeInt        out (.scale         x))))

(freezer Ratio id-ratio true
  (do
    (write-biginteger out (.numerator   x))
    (write-biginteger out (.denominator x))))

(freezer MapEntry id-map-entry true
  (do
    (-freeze-with-meta! (key x) out)
    (-freeze-with-meta! (val x) out)))

(freezer java.util.Date id-util-date true (.writeLong out (.getTime  x)))
(freezer java.sql.Date  id-sql-date  true (.writeLong out (.getTime  x)))
(freezer URI            id-uri       true (write-str  out (.toString x)))
(freezer UUID           id-uuid      true
  (do
    (.writeLong out (.getMostSignificantBits  x))
    (.writeLong out (.getLeastSignificantBits x))))

(freezer Boolean                               nil true (if (boolean x) (write-id out id-true) (write-id out id-false)))
(freezer (Class/forName "[B")                  nil true (write-bytes   out x))
(freezer (Class/forName "[Ljava.lang.Object;") nil true (write-objects out x))
(freezer String                                nil true (write-str   out x))
(freezer Keyword                               nil true (write-kw    out x))
(freezer Symbol                                nil true (write-sym   out x))
(freezer Long                                  nil true (write-long  out x))
(freezer Double                                nil true
  (if (zero? ^double x)
    (do (write-id out id-double-0))
    (do (write-id out id-double) (.writeDouble out x))))

(freezer PersistentQueue    nil true (write-counted-coll   out id-queue-lg      x))
(freezer PersistentTreeSet  nil true (write-counted-coll   out id-sorted-set-lg x))
(freezer PersistentTreeMap  nil true (write-kvs            out id-sorted-map-lg x))
(freezer APersistentVector  nil true (write-vec            out                  x))
(freezer APersistentSet     nil true (write-set            out                  x))
(freezer APersistentMap     nil true (write-map            out                  x false))
(freezer PersistentList     nil true (write-counted-coll   out id-list-0 id-list-sm id-list-md id-list-lg x))
(freezer LazySeq            nil true (write-uncounted-coll out  id-seq-0  id-seq-sm  id-seq-md  id-seq-lg x))
(freezer ISeq               nil true (write-coll           out  id-seq-0  id-seq-sm  id-seq-md  id-seq-lg x))
(freezer IRecord            nil true
  (let [class-name    (.getName (class x)) ; Reflect
        class-name-ba (.getBytes class-name StandardCharsets/UTF_8)
        len           (alength   class-name-ba)]
    (enc/cond
      (sm-count? len) (do (write-id out id-record-sm) (write-bytes-sm out class-name-ba))
      (md-count? len) (do (write-id out id-record-md) (write-bytes-md out class-name-ba))
      ;; :else        (do (write-id out id-record-lg) (write-bytes-md out class-name-ba)) ; Unrealistic
      :else           (throw (ex-info "Record class name too long" {:name class-name})))

    (-freeze-without-meta! (into {} x) out)))

(let [munged-name (enc/fmemoize #(munge (name %)))
      get-basis
      (do #_enc/fmemoize ; Small perf benefit not worth the loss of dynamism
        (fn [^java.lang.Class aclass]
          (let [basis-method (.getMethod aclass "getBasis" nil)]
            (.invoke basis-method nil nil))))]

  (freezer IType nil true
    (let [aclass     (class x)
          class-name (.getName aclass)]
      (write-id  out id-type)
      (write-str out class-name)
      (-run!
        (fn [b]
          (let [^Field cfield (.getField aclass (munged-name b))]
            (-freeze-without-meta! (.get cfield x) out)))
        (get-basis aclass)))))

(comment (do (deftype T1 [x]) (.invoke (.getMethod (class (T1. :x)) "getBasis" nil) nil nil)))

(enc/compile-if java.time.Instant
  (freezer      java.time.Instant id-time-instant true
    (do
      (.writeLong out (.getEpochSecond x))
      (.writeInt  out (.getNano        x)))))

(enc/compile-if java.time.Duration
  (freezer      java.time.Duration id-time-duration true
    (do
      (.writeLong out (.getSeconds x))
      (.writeInt  out (.getNano    x)))))

(enc/compile-if java.time.Period
  (freezer      java.time.Period id-time-period true
    (do
      (.writeInt  out (.getYears  x))
      (.writeInt  out (.getMonths x))
      (.writeInt  out (.getDays   x)))))

(freezer Object nil nil
  (do
    (when-debug (println (str "freeze-fallback: " (type x))))
    (if-let [ff *freeze-fallback*]
      (if-not (identical? ff :write-unfreezable)
        (ff out x) ; Modern approach with ff
        (or        ; Legacy approach with ff
          (try-write-serializable out x)
          (try-write-readable     out x)
          (write-unfreezable      out x)))

      ;; Without ff
      (or
        (try-write-serializable out x)
        (try-write-readable     out x)

        (when-let [fff *final-freeze-fallback*] (fff out x) true) ; Deprecated

        (throw-unfreezable x)))))

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
    (throw
      (ex-info (str "Unrecognized header meta: " head-meta)
        {:head-meta head-meta}))))

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

  Equivalent to (but a little faster than) `freeze` with opts:
    - :compressor nil
    - :encryptor  nil
    - :no-header? true"
  [x]
  (let [baos (ByteArrayOutputStream. 64)
        dos  (DataOutputStream. baos)]
    (with-cache (-freeze-with-meta! x dos))
    (.toByteArray baos)))

(defn freeze
  "Serializes arg (any Clojure data type) to a byte array.
  To freeze custom types, extend the Clojure reader or see `extend-freeze`."
  ([x] (freeze x nil))
  ([x {:as   opts
       :keys [compressor encryptor password serializable-allowlist incl-metadata?]
       :or   {compressor :auto
              encryptor  aes128-gcm-encryptor}}]

   (call-with-bindings :freeze opts
     (fn []
       (let [no-header? (or (get opts :no-header?) (get opts :skip-header?)) ; Undocumented
             encryptor  (when password encryptor)
             baos (ByteArrayOutputStream. 64)
             dos  (DataOutputStream. baos)]

         (if (and (nil? compressor) (nil? encryptor))
           (do ; Optimized case
             (when-not no-header? ; Avoid `wrap-header`'s array copy:
               (let [head-ba (get-head-ba {:compressor-id nil :encryptor-id nil})]
                 (.write dos head-ba 0 4)))
             (with-cache (-freeze-with-meta! x dos))
             (.toByteArray baos))

           (do
             (with-cache (-freeze-with-meta! x dos))
             (let [ba (.toByteArray baos)

                   compressor
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
                    :encryptor-id  (when-let [e encryptor]  (or (encryption/standard-header-ids  (encryption/header-id  e)) :else))}))))))))))

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
     id-bytes-0  (byte-array 0)
     id-bytes-sm (read-bytes in (read-sm-count in))
     id-bytes-md (read-bytes in (read-md-count in))
     id-bytes-lg (read-bytes in (read-lg-count in)))))

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

(defn- xform* [xform] (enc/catching-xform {:error/msg "Error thrown via `*thaw-xform*`"} xform))

(let [rf! (fn rf! ([x] (persistent! x)) ([acc x] (conj! acc x)))
      rf* (fn rf* ([x]              x)  ([acc x] (conj  acc x)))]

  (defn- read-into [to ^DataInput in ^long n]
    (let [transient? (and (editable? to) (> n 10))
          init       (if transient? (transient to) to)
          rf         (if transient? rf! rf*)
          rf         (if-let [xf *thaw-xform*] ((xform* xf) rf) rf)]

      (rf (enc/reduce-n (fn [acc _] (rf acc (thaw-from-in! in))) init n)))))

(let [rf1! (fn rf1! ([x] (persistent! x)) ([acc kv ] (assoc! acc (key kv) (val kv))))
      rf2! (fn rf2! ([x] (persistent! x)) ([acc k v] (assoc! acc      k         v)))
      rf1* (fn rf1* ([x]              x)  ([acc kv ] (assoc  acc (key kv) (val kv))))
      rf2* (fn rf2* ([x]              x)  ([acc k v] (assoc  acc      k         v)))]

  (defn- read-kvs-into [to ^DataInput in ^long n]
    (let [transient? (and (editable? to) (> n 10))
          init       (if transient? (transient to) to)
          rf1        (if transient? rf1! rf1*)
          rf2        (if transient? rf2! rf2*)]

      (if-let [xf *thaw-xform*]
        (let [rf ((xform* xf) rf1)] (rf (enc/reduce-n (fn [acc _] (rf acc (enc/map-entry (thaw-from-in! in) (thaw-from-in! in)))) init n)))
        (let [rf              rf2 ] (rf (enc/reduce-n (fn [acc _] (rf acc                (thaw-from-in! in) (thaw-from-in! in)))  init n)))))))

(defn- read-kvs-depr [to ^DataInput in] (read-kvs-into to in (quot (.readInt in) 2)))
(defn- read-objects [^objects ary ^DataInput in]
  (enc/reduce-n
    (fn [^objects ary i]
      (aset ary i (thaw-from-in! in))
      ary)
    ary (alength ary)))

(def ^:private class-method-sig (into-array Class [IPersistentMap]))

(defn- read-custom! [in prefixed? type-id]
  (if-let [custom-reader (get *custom-readers* type-id)]
    (try
      (custom-reader in)
      (catch Exception e
        (throw
          (ex-info
            (str "Reader exception for custom type id: " type-id)
            {:type-id type-id, :prefixed? prefixed?} e))))
    (throw
      (ex-info
        (str "No reader provided for custom type id: " type-id)
        {:type-id type-id, :prefixed? prefixed?}))))

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
    (throw ; No way to skip bytes, so best we can do is throw
      (ex-info "Cannot thaw object: `taoensso.nippy/*thaw-serializable-allowlist*` check failed. This is a security feature. See `*thaw-serializable-allowlist*` docstring or https://github.com/ptaoussanis/nippy/issues/130 for details!"
        {:class-name class-name}))))

(defn- read-record [in class-name]
  (let [content (thaw-from-in! in)]
    (try
      (let [class  (clojure.lang.RT/classForName class-name)
            method (.getMethod class "create" class-method-sig)]
        (.invoke method class (into-array Object [content])))
      (catch Exception e
        {:nippy/unthawable
         {:type  :record
          :cause :exception

          :class-name class-name
          :content    content
          :exception  e}}))))

(defn- read-type [in class-name]
  (try
    (let [aclass (clojure.lang.RT/classForName class-name)
          nbasis
          (let [basis-method (.getMethod aclass "getBasis" nil)
                basis        (.invoke basis-method nil nil)]
            (count basis))

          cvalues (object-array nbasis)]

      (enc/reduce-n
        (fn [_ i] (aset cvalues i (thaw-from-in! in)))
        nil nbasis)

      (let [ctors (.getConstructors aclass)
            ^Constructor ctor (aget ctors 0) ; Impl. detail? Ref. <https://goo.gl/XWmckR>
            ]
        (.newInstance ctor cvalues)))

    (catch Exception e
      {:nippy/unthawable
       {:type  :type
        :cause :exception

        :class-name class-name
        :exception  e}})))

(defn thaw-from-in!
  "Deserializes a frozen object from given DataInput to its original Clojure
  data type.

  This is a low-level util: in most cases you'll want `thaw` instead."
  [^DataInput data-input]
  (let [in      data-input
        type-id (.readByte in)]

    (when-debug (println (str "thaw-from-in!: " type-id)))
    (try
      (enc/case-eval type-id

        id-reader-sm       (read-edn             (read-str in (read-sm-count in)))
        id-reader-md       (read-edn             (read-str in (read-md-count in)))
        id-reader-lg       (read-edn             (read-str in (read-lg-count in)))
        id-reader-lg_      (read-edn             (read-str in (read-lg-count in)))
        id-record-sm       (read-record       in (read-str in (read-sm-count in)))
        id-record-md       (read-record       in (read-str in (read-md-count in)))
        id-record-lg_      (read-record       in (read-str in (read-lg-count in)))

        id-sz-quarantined-sm    (read-sz-quarantined   in (read-str in (read-sm-count in)))
        id-sz-quarantined-md    (read-sz-quarantined   in (read-str in (read-md-count in)))

        id-sz-unquarantined-sm_ (read-sz-unquarantined in (read-str in (read-sm-count in)))
        id-sz-unquarantined-md_ (read-sz-unquarantined in (read-str in (read-md-count in)))
        id-sz-unquarantined-lg_ (read-sz-unquarantined in (read-str in (read-lg-count in)))

        id-type        (read-type in (thaw-from-in! in))

        id-nil         nil
        id-true        true
        id-false       false
        id-char        (.readChar in)

        id-meta-protocol-key ::meta-protocol-key
        id-meta
        (let [m (thaw-from-in! in) ; Always consume from stream
              x (thaw-from-in! in)]
          (if-let [m (when *incl-metadata?* (not-empty (dissoc m ::meta-protocol-key)))]
            (with-meta x m)
            (do        x)))

        id-cached-0    (thaw-cached 0 in)
        id-cached-1    (thaw-cached 1 in)
        id-cached-2    (thaw-cached 2 in)
        id-cached-3    (thaw-cached 3 in)
        id-cached-4    (thaw-cached 4 in)
        id-cached-5    (thaw-cached 5 in)
        id-cached-6    (thaw-cached 6 in)
        id-cached-7    (thaw-cached 7 in)
        id-cached-sm   (thaw-cached (read-sm-count in) in)
        id-cached-md   (thaw-cached (read-md-count in) in)

        id-bytes-0     (byte-array 0)
        id-bytes-sm    (read-bytes in (read-sm-count in))
        id-bytes-md    (read-bytes in (read-md-count in))
        id-bytes-lg    (read-bytes in (read-lg-count in))

        id-objects-lg  (read-objects (object-array (read-lg-count in)) in)

        id-str-0       ""
        id-str-sm*              (read-str in (read-sm-count* in))
        id-str-sm_              (read-str in (read-sm-count  in))
        id-str-md               (read-str in (read-md-count  in))
        id-str-lg               (read-str in (read-lg-count  in))

        id-kw-sm       (keyword (read-str in (read-sm-count in)))
        id-kw-md       (keyword (read-str in (read-md-count in)))
        id-kw-md_      (keyword (read-str in (read-lg-count in)))
        id-kw-lg_      (keyword (read-str in (read-lg-count in)))

        id-sym-sm      (symbol  (read-str in (read-sm-count in)))
        id-sym-md      (symbol  (read-str in (read-md-count in)))
        id-sym-md_     (symbol  (read-str in (read-lg-count in)))
        id-sym-lg_     (symbol  (read-str in (read-lg-count in)))
        id-regex       (re-pattern (thaw-from-in! in))

        id-vec-0       []
        id-vec-2       (read-into [] in 2)
        id-vec-3       (read-into [] in 3)
        id-vec-sm*     (read-into [] in (read-sm-count* in))
        id-vec-sm_     (read-into [] in (read-sm-count  in))
        id-vec-md      (read-into [] in (read-md-count  in))
        id-vec-lg      (read-into [] in (read-lg-count  in))

        id-set-0       #{}
        id-set-sm*     (read-into    #{} in (read-sm-count* in))
        id-set-sm_     (read-into    #{} in (read-sm-count  in))
        id-set-md      (read-into    #{} in (read-md-count  in))
        id-set-lg      (read-into    #{} in (read-lg-count  in))

        id-map-0       {}
        id-map-sm*     (read-kvs-into {} in (read-sm-count* in))
        id-map-sm_     (read-kvs-into {} in (read-sm-count  in))
        id-map-md      (read-kvs-into {} in (read-md-count  in))
        id-map-lg      (read-kvs-into {} in (read-lg-count  in))

        id-queue-lg      (read-into     PersistentQueue/EMPTY in (read-lg-count in))
        id-sorted-set-lg (read-into     (sorted-set)          in (read-lg-count in))
        id-sorted-map-lg (read-kvs-into (sorted-map)          in (read-lg-count in))

        id-list-0            ()
        id-list-sm     (into () (rseq (read-into [] in (read-sm-count in))))
        id-list-md     (into () (rseq (read-into [] in (read-md-count in))))
        id-list-lg     (into () (rseq (read-into [] in (read-lg-count in))))

        id-seq-0       (lazy-seq nil)
        id-seq-sm      (or (seq (read-into [] in (read-sm-count in))) (lazy-seq nil))
        id-seq-md      (or (seq (read-into [] in (read-md-count in))) (lazy-seq nil))
        id-seq-lg      (or (seq (read-into [] in (read-lg-count in))) (lazy-seq nil))

        id-byte              (.readByte  in)
        id-short             (.readShort in)
        id-integer           (.readInt   in)
        id-long-0      0
        id-long-sm_    (long (.readByte  in))
        id-long-md_    (long (.readShort in))
        id-long-lg_    (long (.readInt   in))
        id-long-xl           (.readLong  in)

        id-long-pos-sm    (- (long (.readByte  in))    Byte/MIN_VALUE)
        id-long-pos-md    (- (long (.readShort in))   Short/MIN_VALUE)
        id-long-pos-lg    (- (long (.readInt   in)) Integer/MIN_VALUE)

        id-long-neg-sm (- (- (long (.readByte  in))    Byte/MIN_VALUE))
        id-long-neg-md (- (- (long (.readShort in))   Short/MIN_VALUE))
        id-long-neg-lg (- (- (long (.readInt   in)) Integer/MIN_VALUE))

        id-bigint      (bigint (read-biginteger in))
        id-biginteger          (read-biginteger in)

        id-float       (.readFloat  in)
        id-double-0    0.0
        id-double      (.readDouble in)

        id-bigdec      (BigDecimal. ^BigInteger (read-biginteger in) (.readInt        in))
        id-ratio       (clojure.lang.Ratio.     (read-biginteger in) (read-biginteger in))

        id-map-entry   (enc/map-entry (thaw-from-in! in) (thaw-from-in! in))

        id-util-date   (java.util.Date. (.readLong in))
        id-sql-date    (java.sql.Date.  (.readLong in))
        id-uuid        (UUID. (.readLong in) (.readLong in))
        id-uri         (URI. (thaw-from-in! in))

        id-time-instant
        (let [secs  (.readLong in)
              nanos (.readInt  in)]

          (enc/compile-if java.time.Instant
            (java.time.Instant/ofEpochSecond secs nanos)
            {:nippy/unthawable
             {:type  :class
              :cause :class-not-found

              :class-name "java.time.Instant"
              :content    {:epoch-second secs :nano nanos}}}))

        id-time-duration
        (let [secs  (.readLong in)
              nanos (.readInt  in)]

          (enc/compile-if java.time.Duration
            (java.time.Duration/ofSeconds secs nanos)
            {:nippy/unthawable
             {:type  :class
              :cause :class-not-found

              :class-name "java.time.Duration"
              :content    {:seconds secs :nanos nanos}}}))

        id-time-period
        (let [years  (.readInt in)
              months (.readInt in)
              days   (.readInt in)]

          (enc/compile-if java.time.Period
            (java.time.Period/of years months days)
            {:nippy/unthawable
             {:type  :class
              :cause :class-not-found

              :class-name "java.time.Period"
              :content    {:years years :months months :days days}}}))

        ;; Deprecated ------------------------------------------------------
        id-boolean_    (.readBoolean in)
        id-sorted-map_ (read-kvs-depr (sorted-map) in)
        id-map__       (read-kvs-depr {} in)
        id-reader_     (read-edn (.readUTF in))
        id-str_                  (.readUTF in)
        id-kw_         (keyword  (.readUTF in))
        id-map_        (apply hash-map
                         (enc/repeatedly-into [] (* 2 (.readInt in))
                           (fn [] (thaw-from-in! in))))
        ;; -----------------------------------------------------------------

        id-prefixed-custom-md (read-custom! in :prefixed (.readShort in))

        (if (neg? type-id)
          (read-custom! in nil type-id) ; Unprefixed custom type
          (throw
            (ex-info
              (str "Unrecognized type id (" type-id "). Data frozen with newer Nippy version?")
              {:type-id type-id}))))

      (catch Throwable t
        (throw
          (ex-info (str "Thaw failed against type-id: " type-id)
            {:type-id type-id} t))))))

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
    :no-header (throw (ex-info ":auto not supported on headerless data." {}))
    :else      (throw (ex-info ":auto not supported for non-standard compressors." {}))
    (do        (throw (ex-info (str "Unrecognized :auto compressor id: " compressor-id)
                        {:compressor-id compressor-id})))))

(defn- get-auto-encryptor [encryptor-id]
  (case encryptor-id
    nil                nil
    :aes128-gcm-sha512 aes128-gcm-encryptor
    :aes128-cbc-sha512 aes128-cbc-encryptor
    :no-header (throw (ex-info ":auto not supported on headerless data." {}))
    :else      (throw (ex-info ":auto not supported for non-standard encryptors." {}))
    (do        (throw (ex-info (str "Unrecognized :auto encryptor id: " encryptor-id)
                        {:encryptor-id encryptor-id})))))

(def ^:private err-msg-unknown-thaw-failure "Possible decryption/decompression error, unfrozen/damaged data, etc.")
(def ^:private err-msg-unrecognized-header  "Unrecognized (but apparently well-formed) header. Data frozen with newer Nippy version?")

(defn fast-thaw
  "Like `thaw` but:
    - Drops all support for compression and encryption
    - Supports only data frozen with `fast-freeze`

  Equivalent to (but a little faster than) `thaw` with opts:
    - :compressor nil
    - :encryptor  nil
    - :no-header? true"
  [^bytes ba]
  (let [dis (DataInputStream. (ByteArrayInputStream. ba))]
    (with-cache (thaw-from-in! dis))))

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
                (throw
                  (ex-info (str "Thaw failed. " msg)
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

                    e))))

             thaw-data
             (fn [data-ba compressor-id encryptor-id ex-fn]
               (let [compressor (if (identical? compressor :auto) (get-auto-compressor compressor-id) compressor)
                     encryptor  (if (identical? encryptor  :auto) (get-auto-encryptor  encryptor-id)  encryptor)]

                 (when (and encryptor (not password))
                   (ex "Password required for decryption."))

                 (try
                   (let [ba data-ba
                         ba (if encryptor  (decrypt    encryptor password ba) ba)
                         ba (if compressor (decompress compressor         ba) ba)
                         dis (DataInputStream. (ByteArrayInputStream. ba))]

                     (with-cache (thaw-from-in! dis)))

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

    * Keyword    - 2 byte overhead, keywords hashed to 16 bit id
    * ℕ∈[1, 128] - 0 byte overhead

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
       IFreezable
       (~'-freezable?           [~'x] true)
       (~'-freeze-without-meta! [~x ~(with-meta out {:tag 'java.io.DataOutput})] ~write-id-form ~@body))))

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

(defrecord StressRecord [my-data])
(deftype   StressType   [my-data]
  Object (equals [a b] (and (instance? StressType b) (= (.-my-data             a)
                                                        (.-my-data ^StressType b)))))

(defn stress-data
  "Returns map of reference stress data for use by tests, benchmarks, etc."
  [{:keys [comparable?] :as opts}]
  (let [rng      (java.util.Random. 123456) ; Seeded for determinism
        rand-nth (fn [coll] (nth coll (.nextInt rng (count coll))))
        all
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

         :regex          #"^(https?:)?//(www\?|\?)?"
         :sorted-set     (sorted-set 1 2 3 4 5)
         :sorted-map     (sorted-map :b 2 :a 1 :d 4 :c 3)
         :lazy-seq-empty (map identity ())
         :lazy-seq       (repeatedly 64 #(do nil))
         :queue          (into clojure.lang.PersistentQueue/EMPTY [:a :b :c :d :e :f :g])
         :queue-empty          clojure.lang.PersistentQueue/EMPTY

         :uuid       (java.util.UUID. 7232453380187312026 -7067939076204274491)
         :uri        (java.net.URI. "https://clojure.org")
         :defrecord  (StressRecord. "data")
         :deftype    (StressType.   "data")
         :bytes      (byte-array   [(byte 1) (byte 2) (byte 3)])
         :objects    (object-array [1 "two" {:data "data"}])

         :util-date (java.util.Date. 1577884455500)
         :sql-date  (java.sql.Date.  1577884455500)
         :instant   (enc/compile-if java.time.Instant  (java.time.Instant/parse "2020-01-01T13:14:15.50Z") ::skip)
         :duration  (enc/compile-if java.time.Duration (java.time.Duration/ofSeconds 100 100)              ::skip)
         :period    (enc/compile-if java.time.Period   (java.time.Period/of 1 1 1)                         ::skip)

         :throwable (Throwable. "Msg")
         :exception (Exception. "Msg")
         :ex-info   (ex-info    "Msg" {:data "data"})

         :many-longs    (vec (repeatedly 512         #(rand-nth (range 10))))
         :many-doubles  (vec (repeatedly 512 #(double (rand-nth (range 10)))))
         :many-strings  (vec (repeatedly 512         #(rand-nth ["foo" "bar" "baz" "qux"])))
         :many-keywords (vec (repeatedly 512
                               #(keyword
                                  (rand-nth ["foo" "bar" "baz" "qux" nil])
                                  (rand-nth ["foo" "bar" "baz" "qux"    ]))))}]

    (if comparable?
      (dissoc all :bytes :objects :throwable :exception :ex-info :regex)
      (do     all))))

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
       (-freezable? x)
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
    (alter-var-root *freeze-serializable-allowlist* (fn [old] (f (enc/have set? old))))
    (alter-var-root *thaw-serializable-allowlist*   (fn [old] (f (enc/have set? old))))))

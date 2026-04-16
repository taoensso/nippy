(ns taoensso.nippy.schema
  "Private low-level schema stuff, don't use."
  (:require
   [taoensso.encore :as enc]
   [taoensso.truss  :as truss]))

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

(def         head-sig "First 3 bytes of Nippy header" (.getBytes "NPY" java.nio.charset.StandardCharsets/UTF_8))
(def ^:const head-version "Current Nippy header format version" 1)
(def ^:const head-meta
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

(def types-spec
  "Representation of Nippy's internal type schema,
    {<type-id> [<type-kw> ?<payload-info>]}."

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

   107 [:string-array-lg [[:elements {:read 4}]]] ; Added v3.5.0 (2025-04-15)
   115 [:object-array-lg [[:elements {:read 4}]]]

   118 [:int-array-lg    [[:elements {:read 4}]]] ; Added v3.7.0 (TBD)
   119 [:long-array-lg   [[:elements {:read 4}]]]
   120 [:float-array-lg  [[:elements {:read 4}]]]
   121 [:double-array-lg [[:elements {:read 4}]]]

   ;; Serializable
   75  [:sz-sm [[:bytes {:read 1}] [:elements 1]]]
   76  [:sz-md [[:bytes {:read 2}] [:elements 1]]]

   48  [:record-sm [[:bytes {:read 1}] [:elements 1]]]
   49  [:record-md [[:bytes {:read 2}] [:elements 1]]]

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

   109 [:int-array-lg_    [[:elements {:read 4}]]] ; [2026-04-16 v3.7.0-alpha2] Switched to xBuffer impln
   108 [:long-array-lg_   [[:elements {:read 4}]]] ; ''
   117 [:float-array-lg_  [[:elements {:read 4}]]] ; ''
   116 [:double-array-lg_ [[:elements {:read 4}]]] ; ''

   105 [:str-sm_  [[:bytes    {:read 1}]]]               ; [2023-08-02 v3.3.0] Switch to unsigned sm*
   110 [:vec-sm_  [[:elements {:read 1}]]]               ; ''
   111 [:set-sm_  [[:elements {:read 1}]]]               ; ''
   112 [:map-sm_  [[:elements {:read 1 :multiplier 2}]]] ; ''

   100 [:long-sm_ [[:bytes 1]]] ; [2023-08-02 v3.3.0] Switch to 2x pos/neg ids
   101 [:long-md_ [[:bytes 2]]] ; ''
   102 [:long-lg_ [[:bytes 4]]] ; ''

   78  [:sym-md_ [[:bytes {:read 4}]]] ; [2020-11-18 v3.1.1] Buggy size field, Ref. #138
   77  [:kw-md_  [[:bytes {:read 4}]]] ; ''

   6   [:sz-lg_ nil] ; [2020-07-24 v2.15.0] Unskippable, Ref. #130
   50  [:sz-md_ nil] ; ''
   46  [:sz-sm_ nil] ; ''

   14  [:kw-lg_     [[:bytes {:read 4}]]]               ; [2020-09-20 v3.0.0] Unrealistic
   57  [:sym-lg_    [[:bytes {:read 4}]]]               ; ''
   80  [:record-lg_ [[:bytes {:read 4}] [:elements 1]]] ; ''

   5   [:reader-lg_ [[:bytes {:read 4}]]] ; [2016-07-24 v2.12.0] Identical to `:reader-lg`, historical accident
   4   [:boolean_   [[:bytes 1]]]         ; [2016-07-24 v2.12.0] For switch to true/false ids

   29  [:sorted-map_ [[:elements {:read 4}]]] ; [2016-02-25 v2.11.0] For count/2
   27  [:map__       [[:elements {:read 4}]]] ; ''

   12  [:kw_     [[:bytes {:read 2}]]] ; [2013-07-22 v2.0.0] For consistecy with str impln

   1   [:reader_ [[:bytes {:read 2}]]] ; [2012-07-20 v0.9.2] For >64k length support
   11  [:str_    [[:bytes {:read 2}]]] ; ''

   22  [:map_    [[:elements {:read 4 :multiplier 2}]]] ; [2012-07-07 v0.9.0] For more efficient thaw impln
   })

(comment
  (count ; Eval to check for unused type-ids
    (enc/reduce-n (fn [acc in] (if-not (types-spec in) (conj acc in) acc))
      [] Byte/MAX_VALUE)))

(defmacro defids
  "Defines `id-x` vars."
  []
  `(do
     ~@(map
         (fn [[id# [kw#]]]
           (let [kw#  (str "id-" (name   kw#))
                 sym# (with-meta (symbol kw#) {:const true})]
             `(def ~sym# (byte ~id#))))
         types-spec)))

(comment (macroexpand '(defids)))

(defids)

;;;;

(def head-meta-id (reduce-kv #(assoc %1 %3 %2) {} head-meta))
(def get-head-ba
  (enc/fmemoize
    (fn [head-meta]
      (when-let [meta-id (get head-meta-id (assoc head-meta :version head-version))]
        (enc/ba-concat head-sig (byte-array [meta-id]))))))

(defn wrap-header      [data-ba head-meta]
  (if-let [head-ba (get-head-ba head-meta)]
    (enc/ba-concat head-ba data-ba)
    (truss/ex-info! (str "Unrecognized header meta: " head-meta)
      {:head-meta head-meta})))

(comment (wrap-header (.getBytes "foo") {:compressor-id :lz4, :encryptor-id nil}))

(let [head-sig head-sig] ; Not ^:const
  (defn try-parse-header
    "Returns ?[data-ba head-meta]."
    [^bytes ba]
    (let [len (alength ba)]
      (when (> len 4)
        (let [-head-sig (java.util.Arrays/copyOf ba 3)]
          (when (java.util.Arrays/equals -head-sig ^bytes head-sig)
            ;; Header appears to be well-formed
            (let [meta-id (aget ba 3)
                  data-ba (java.util.Arrays/copyOfRange ba 4 len)]
              [data-ba (get head-meta meta-id {:unrecognized-meta? true})])))))))

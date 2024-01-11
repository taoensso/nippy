(ns taoensso.nippy-tests
  (:require
   [clojure.test :as test :refer [deftest testing is]]
   [clojure.test.check            :as tc]
   [clojure.test.check.generators :as tc-gens]
   [clojure.test.check.properties :as tc-props]
   [taoensso.encore               :as enc   :refer []]
   [taoensso.nippy                :as nippy :refer [freeze thaw]]
   [taoensso.nippy.compression    :as compr]
   [taoensso.nippy.crypto         :as crypto]
   [taoensso.nippy-benchmarks     :as benchmarks]))

(comment
  (remove-ns      'taoensso.nippy-tests)
  (test/run-tests 'taoensso.nippy-tests))

;;;; Config, etc.

(def test-data nippy/stress-data-comparable)
(def tc-gen-recursive-any-equatable
  (tc-gens/recursive-gen tc-gens/container-type
    tc-gens/any-equatable))

(defmacro gen-test [num-tests [data-sym] & body]
  `(let [tc-result#
         (tc/quick-check ~num-tests
           (tc-props/for-all [~data-sym tc-gen-recursive-any-equatable]
             ~@body))]
     (true? (:pass? tc-result#))))

(comment
  (tc-gens/sample tc-gen-recursive-any-equatable 10)
  (gen-test 10 [gen-data] true))

;;;; Core

(deftest _core
  [(println (str "Clojure version: " *clojure-version*))
   (is (= test-data ((comp thaw freeze) test-data)))
   (is (= test-data ((comp #(thaw   % {:no-header? true
                                       :compressor nippy/lz4-compressor
                                       :encryptor  nil})
                           #(freeze % {:no-header? true}))
                     test-data)))

   (is (= test-data ((comp #(thaw   % {:password [:salted "p"]})
                           #(freeze % {:password [:salted "p"]}))
                     test-data)))

   (is (= (vec (:objects nippy/stress-data))
          ((comp vec thaw freeze) (:objects nippy/stress-data))))

   (is (= test-data ((comp #(thaw   % {:compressor nippy/lzma2-compressor})
                           #(freeze % {:compressor nippy/lzma2-compressor}))
                     test-data)))

   (is (= test-data ((comp #(thaw   % {:compressor nippy/lzma2-compressor
                                       :password [:salted "p"]})
                           #(freeze % {:compressor nippy/lzma2-compressor
                                       :password [:salted "p"]}))
                     test-data)))

   (is (= test-data ((comp #(thaw   % {:compressor nippy/lz4-compressor})
                           #(freeze % {:compressor nippy/lz4-compressor}))
                     test-data)))

   (is (= test-data ((comp #(thaw   % {:compressor nippy/zstd-compressor})
                           #(freeze % {:compressor nippy/zstd-compressor}))
                     test-data)))

   (is (enc/throws? Exception (thaw (freeze test-data {:password "malformed"}))))
   (is (enc/throws? Exception (thaw (freeze test-data {:password [:salted "p"]}))))
   (is (enc/throws? Exception (thaw (freeze test-data {:password [:salted "p"]}))))

   (is
     (= "payload"
       (thaw (freeze "payload" {:password [:salted "pwd"] :encryptor nippy/aes128-cbc-encryptor})
         (do                   {:password [:salted "pwd"]})))
     "CBC auto-encryptor compatibility")

   (testing "Unsigned long types"
     (let [range-ushort+ (+ (long @#'nippy/range-ushort) 128)
           range-uint+   (+ (long @#'nippy/range-uint)   128)]

       [(let [r (range (long -2.5e6) (long 2.5e6))]      (= (thaw (freeze r)) r))
        (let [r (range (- range-ushort+) range-ushort+)] (= (thaw (freeze r)) r))
        (let [n    range-uint+]                          (= (thaw (freeze n)) n))
        (let [n (- range-uint+)]                         (= (thaw (freeze n)) n))]))

   (is (gen-test 1600 [gen-data] (= gen-data (thaw (freeze gen-data)))) "Generative")])

;;;; Custom types & records

(deftype   MyType [basic_field fancy-field!]) ; Note `fancy-field!` field name will be munged
(defrecord MyRec  [basic_field fancy-field!])

(deftest _types
  [(testing "Extend to custom type"
     [(is
        (enc/throws? Exception ; No thaw extension yet
          (do
            (alter-var-root #'nippy/*custom-readers* (constantly {}))
            (nippy/extend-freeze MyType 1 [x s]
              (.writeUTF s (.basic_field  x))
              (.writeUTF s (.fancy-field! x)))

            (thaw (freeze (MyType. "basic" "fancy"))))))

      (is
        (do
          (nippy/extend-thaw 1 [s] (MyType. (.readUTF s) (.readUTF s)))
          (let [mt1 (MyType. "basic" "fancy")
                ^MyType mt2 (thaw (freeze mt1))]
            (=
              [(.basic_field mt1) (.fancy-field! mt1)]
              [(.basic_field mt2) (.fancy-field! mt2)]))))])

   (testing "Extend to custom Record"
     (is
       (do
         (nippy/extend-freeze MyRec 2 [x s]
           (.writeUTF s (str "foo-" (:basic_field  x)))
           (.writeUTF s (str "foo-" (:fancy-field! x))))

         (nippy/extend-thaw 2 [s] (MyRec. (.readUTF s) (.readUTF s)))
         (=
           (do           (MyRec. "foo-basic" "foo-fancy"))
           (thaw (freeze (MyRec.     "basic"     "fancy")))))))

   (testing "Keyword (prefixed) extensions"
     (is
       (do
         (nippy/extend-freeze MyRec :nippy-tests/MyRec [x s]
           (.writeUTF s (:basic_field  x))
           (.writeUTF s (:fancy-field! x)))

         (nippy/extend-thaw :nippy-tests/MyRec [s] (MyRec. (.readUTF s) (.readUTF s)))
         (let [mr (MyRec. "basic" "fancy")]
           (=  mr (thaw (freeze mr)))))))])

;;;; Caching

(deftest _caching
  (let [stress [nippy/stress-data-comparable
                nippy/stress-data-comparable
                nippy/stress-data-comparable
                nippy/stress-data-comparable]
        cached (mapv nippy/cache stress)
        cached (mapv nippy/cache stress) ; <=1 wrap auto-enforced
        ]

    [(is (= stress (thaw (freeze stress {:compressor nil}))))
     (is (= stress (thaw (freeze cached {:compressor nil}))))
     (let [size-stress (count (freeze stress {:compressor nil}))
           size-cached (count (freeze cached {:compressor nil}))]
       (is (>= size-stress (* 3 size-cached)))
       (is (<  size-stress (* 4 size-cached))))]))

(deftest _caching-metadata
  (let [v1 (with-meta [] {:id :v1})
        v2 (with-meta [] {:id :v2})

        frozen-without-caching (freeze [v1 v2 v1 v2])
        frozen-with-caching
        (freeze [(nippy/cache v1)
                 (nippy/cache v2)
                 (nippy/cache v1)
                 (nippy/cache v2)])]

    [(is (> (count frozen-without-caching)
            (count frozen-with-caching)))

     (is (= (thaw frozen-without-caching)
            (thaw frozen-with-caching)))

     (is (= (mapv meta (thaw frozen-with-caching))
            [{:id :v1} {:id :v2} {:id :v1} {:id :v2}]))]))

;;;; Stable binary representation of vals

(deftest _stable-bin
  (testing "Stable binary representation of vals"

    (testing "x=y !=> f(x)=f(y)"
      ;; `x=y => f(x)=f(y)` is unfortunately NOT true in general
      ;; Be careful to never assume the above relationship!
      [(is (not= (vec (freeze {:a 1 :b 1})) (vec (freeze {:b 1 :a 1}))) "Small (array) map (not= (seq {:a 1 :b 1}) (seq {:b 1 :a 1}))")
       (is (not= (vec (freeze [[]]))        (vec (freeze ['()])))       "(= [] '()) is true")
       (is (= (vec (freeze (sorted-map :a 1 :b 1)))
              (vec (freeze (sorted-map :b 1 :a 1)))) "Sorted structures are immune")])

    (testing "x==y => f(x)=f(y)"
      ;; This weaker version of `x=y => f(x)=f(y)` does hold
      [(is (= (vec (freeze test-data))
              (vec (freeze test-data))))

       (is (every? true? (repeatedly 1000 (fn [] (= (vec (freeze test-data))
                                                    (vec (freeze test-data))))))
         "Try repeatedly to catch possible protocol interface races")

       (is (gen-test 400 [gen-data]
              (= (vec (freeze gen-data))
                 (vec (freeze gen-data)))) "Generative")])

    (testing "f(x)=f(f-1(f(x)))"
      [(is (= (vec (-> test-data freeze))
              (vec (-> test-data freeze thaw freeze))))

       (is (= (seq (freeze test-data))
              (seq (reduce (fn [frozen _] (freeze (thaw frozen)))
                     (freeze test-data) (range 1000))))
         "Try repeatedly to catch possible protocol interface races")

       (is (gen-test 400 [gen-data]
             (= (vec (-> gen-data freeze))
                (vec (-> gen-data freeze thaw freeze)))) "Generative")])

    (testing "f(x)=f(y) => x=y"
      (let [vals_ (atom {})]
        (gen-test 400 [gen-data]
          (let [bin (freeze     gen-data)
                ref (get @vals_ gen-data ::nx)]
            (swap! vals_ assoc bin gen-data)
            (or (= ref ::nx) (= ref bin))))))))

;;;; Thread safety

(deftest _thread-safe
  [(is
     (let [futures (mapv (fn [_] (future (= (thaw (freeze test-data)) test-data)))
                     (range 50))]
       (every? deref futures)))

   (is
     (let [futures
           (mapv
             (fn [_]
               (future
                 (= (thaw (freeze test-data {:password [:salted "password"]})
                                            {:password [:salted "password"]})
                   test-data)))
             (range 50))]
       (every? deref futures)))

   (is
     (let [futures
           (mapv
             (fn [_]
               (future
                 (= (thaw (freeze test-data {:password [:cached "password"]})
                                            {:password [:cached "password"]})
                   test-data)))
             (range 50))]
       (every? deref futures)))])

;;;; Redefs

(defrecord MyFoo [] Object (toString [_] "v1"))
(defrecord MyFoo [] Object (toString [_] "v2"))

(deftest _redefs
  (is (= (str (thaw (freeze (MyFoo.)))) "v2")))

;;;; Serializable

(do
  (def ^:private semcn              "java.util.concurrent.Semaphore")
  (def ^:private sem                (java.util.concurrent.Semaphore. 1))
  (defn-         sem? [x] (instance? java.util.concurrent.Semaphore x)))

(deftest _serializable
  [(is (= nippy/*thaw-serializable-allowlist* #{"base.1" "base.2" "add.1" "add.2"})
     "JVM properties override initial allowlist values")

   (is (enc/throws? Exception (nippy/freeze sem {:serializable-allowlist #{}}))
     "Can't freeze Serializable objects unless approved by allowlist")

   (is (sem?
         (nippy/thaw
           (nippy/freeze sem {:serializable-allowlist #{semcn}})
           {:serializable-allowlist #{semcn}}))

     "Can freeze and thaw Serializable objects if approved by allowlist")

   (is (sem?
         (nippy/thaw
           (nippy/freeze sem {:serializable-allowlist #{"java.util.concurrent.*"}})
           {:serializable-allowlist #{"java.util.concurrent.*"}}))

     "Strings in allowlist sets may contain \"*\" wildcards")

   (let [ba     (nippy/freeze sem #_{:serializable-allowlist "*"})
         thawed (nippy/thaw   ba    {:serializable-allowlist #{}})]

     [(is (= :quarantined (get-in thawed [:nippy/unthawable :cause]))
        "Serializable objects will be quarantined when approved for freezing but not thawing.")

      (is (sem? (nippy/read-quarantined-serializable-object-unsafe! thawed))
        "Quarantined Serializable objects can still be manually force-read.")

      (is (sem? (nippy/read-quarantined-serializable-object-unsafe!
                  (nippy/thaw (nippy/freeze thawed))))
        "Quarantined Serializable objects are themselves safely transportable.")])

   (let [obj
         (nippy/thaw
           (nippy/freeze sem)
           {:serializable-allowlist "allow-and-record"})]

     [(is (sem? obj)
        "Special \"allow-and-record\" allowlist permits any class")

      (is
        (contains? (nippy/get-recorded-serializable-classes) semcn)
        "Special \"allow-and-record\" allowlist records classes")])])

;;;; Metadata

(def my-var "Just a string")

(deftest _metadata
  [(is
     (:has-meta?
      (meta
        (nippy/thaw
          (nippy/freeze (with-meta [] {:has-meta? true}) {:incl-metadata? true})
          {:incl-metadata? true}
          )))

     "Metadata successfully included")

   (is
     (nil?
       (meta
         (nippy/thaw
           (nippy/freeze (with-meta [] {:has-meta? true}) {:incl-metadata? true})
           {:incl-metadata? false}
           )))

     "Metadata successfully excluded by thaw")

   (is
     (nil?
       (meta
         (nippy/thaw
           (nippy/freeze (with-meta [] {:has-meta? true}) {:incl-metadata? false})
           {:incl-metadata? true}
           )))

     "Metadata successfully excluded by freeze")

   (is (var? (nippy/read-quarantined-serializable-object-unsafe!
               (nippy/thaw (nippy/freeze #'my-var))))

     "Don't try to preserve metadata on vars")])

;;;; Freezable?

(deftest _freezable?
  [(is (= (nippy/freezable? :foo)                      :native))
   (is (= (nippy/freezable? [:a :b])                   :native))
   (is (= (nippy/freezable? [:a (fn [])])                  nil))
   (is (= (nippy/freezable? [:a (byte-array [1 2 3])]) :native))
   (is (= (nippy/freezable? [:a (java.util.Date.)])    :native))
   (is (= (nippy/freezable? (Exception.))                  nil))
   (is (= (nippy/freezable? (MyType. "a" "b"))         :native))
   (is (= (nippy/freezable? (MyRec.  "a" "b"))         :native))
   (is (= (nippy/freezable? (Exception.) {:allow-java-serializable? true})
         :maybe-java-serializable))])

;;;; thaw-xform

(deftest _thaw-xform
  [(is (= (binding [nippy/*thaw-xform* nil]                                           (thaw (freeze [1 2 :secret 3 4]))) [1 2 :secret   3 4]))
   (is (= (binding [nippy/*thaw-xform* (map (fn [x] (if (= x :secret) :redacted x)))] (thaw (freeze [1 2 :secret 3 4]))) [1 2 :redacted 3 4]))

   (is (= (binding [nippy/*thaw-xform* (remove (fn [x] (and (map-entry? x) (and (= (key x) :x) (val x)))))]
            (thaw (freeze {:a :A, :b :B, :x :X, :c {:x :X}, :d #{:d1 :d2 {:d3 :D3, :x :X}}})))
         {:a :A, :b :B, :c {}, :d #{:d1 :d2 {:d3 :D3}}}))

   (is (= (binding [nippy/*thaw-xform* (remove (fn [x] (and (map? x) (contains? x :x))))]
            (thaw (freeze {:a :A, :b :B, :x :X, :c {:x :X}, :d #{:d1 :d2 {:d3 :D3, :x :X}}})))
         {:a :A, :b :B, :x :X, :c {:x :X}, :d #{:d1 :d2}}))

   (is (= (binding [nippy/*thaw-xform* (map (fn [x] (/ 1 0)))] (thaw (freeze []))) []) "rf not run on empty colls")

   (let [ex (enc/throws :default (binding [nippy/*thaw-xform* (map (fn [x] (/ 1 0)))] (thaw (freeze [:a :b]))))]
     (is (= (-> ex enc/ex-cause enc/ex-cause ex-data :call) '(rf acc in)) "Error thrown via `*thaw-xform*`"))])

;;;; Compressors

(deftest _compressors
  (doseq [c [compr/zstd-compressor
             compr/lz4-compressor
             compr/lzo-compressor
             compr/snappy-compressor
             compr/lzma2-compressor]]

    (dotimes [_ 100]
      (is
        (nil? (enc/catching (compr/decompress c (crypto/rand-bytes 1024))))
        "Decompression never core dumps, even against invalid data"))))

;;;; Stable output format

(defn b64-encode [^bytes ba]
  (String. ^bytes (.encode (java.util.Base64/getEncoder) ba) "UTF-8"))

(defn md5-hash ^String [^bytes ba]
  (.digest (java.security.MessageDigest/getInstance "MD5") ba))

(def freeze-and-hash (comp b64-encode md5-hash nippy/freeze))

(deftest _stable-output-format
  (testing "Stable output format of vals"
    (let [;; Fixed version of Reference data used for consistency tests between Nippy implementations and Java runtime
          stable-test-data (-> test-data
                               (assoc :lazy-seq (lazy-seq '(0.19671559947161366 0.2438222579118563 0.45156117728435397 0.34058300699753474 0.1808929679060426 0.11636995743908862 0.14424150003786507 0.30750948264879463 0.4649144815628935 0.667767499541767))
                                      :uuid (java.util.UUID/fromString "80f7e434-496d-4127-af05-cae4ff184113")

                                      :util-date #inst"2024-01-11T09:35:58.686-00:00" ;; (instance? java.util.Date #inst"2024-01-11T09:35:58.686-00:00") => true)
                                      :time-instant (enc/compile-if java.time.Instant (java.time.Instant/parse "2024-01-11T09:35:58.686-00:00") nil)
                                      ;; generated using JDK 21.0.1
                                      :many-small-keywords [:aa :ab :ae :af :ak :am :an :ar :as :av :ay :az :ba :be :bg :bh :bi :bm :bn :bo :br :bs :ca :ce :ch :co :cr :cs :cu :cv :cy :da :de :dv :dz :ee :el :en :eo :es :et :eu :fa :ff :fi :fj :fo :fr :fy :ga :gd :gl :gn :gu :gv :ha :he :hi :ho :hr :ht :hu :hy :hz :ia :id :ie :ig :ii :ik :in :io :is :it :iu :iw :ja :ji :jv :ka :kg :ki :kj :kk :kl :km :kn :ko :kr :ks :ku :kv :kw :ky :la :lb :lg :li :ln :lo :lt :lu :lv :mg :mh :mi :mk :ml :mn :mo :mr :ms :mt :my :na :nb :nd :ne :ng :nl :nn :no :nr :nv :ny :oc :oj :om :or :os :pa :pi :pl :ps :pt :qu :rm :rn :ro :ru :rw :sa :sc :sd :se :sg :si :sk :sl :sm :sn :so :sq :sr :ss :st :su :sv :sw :ta :te :tg :th :ti :tk :tl :tn :to :tr :ts :tt :tw :ty :ug :uk :ur :uz :ve :vi :vo :wa :wo :xh :yi :yo :za :zh :zu]
                                      :many-small-strings "[\"Andorra\" \"United Arab Emirates\" \"Afghanistan\" \"Antigua & Barbuda\" \"Anguilla\" \"Albania\" \"Armenia\" \"Angola\" \"Antarctica\" \"Argentina\" \"American Samoa\" \"Austria\" \"Australia\" \"Aruba\" \"Åland Islands\" \"Azerbaijan\" \"Bosnia & Herzegovina\" \"Barbados\" \"Bangladesh\" \"Belgium\" \"Burkina Faso\" \"Bulgaria\" \"Bahrain\" \"Burundi\" \"Benin\" \"St Barthélemy\" \"Bermuda\" \"Brunei\" \"Bolivia\" \"Caribbean Netherlands\" \"Brazil\" \"Bahamas\" \"Bhutan\" \"Bouvet Island\" \"Botswana\" \"Belarus\" \"Belize\" \"Canada\" \"Cocos (Keeling) Islands\" \"Congo - Kinshasa\" \"Central African Republic\" \"Congo - Brazzaville\" \"Switzerland\" \"Côte d’Ivoire\" \"Cook Islands\" \"Chile\" \"Cameroon\" \"China\" \"Colombia\" \"Costa Rica\" \"Cuba\" \"Cape Verde\" \"Curaçao\" \"Christmas Island\" \"Cyprus\" \"Czechia\" \"Germany\" \"Djibouti\" \"Denmark\" \"Dominica\" \"Dominican Republic\" \"Algeria\" \"Ecuador\" \"Estonia\" \"Egypt\" \"Western Sahara\" \"Eritrea\" \"Spain\" \"Ethiopia\" \"Finland\" \"Fiji\" \"Falkland Islands\" \"Micronesia\" \"Faroe Islands\" \"France\" \"Gabon\" \"United Kingdom\" \"Grenada\" \"Georgia\" \"French Guiana\" \"Guernsey\" \"Ghana\" \"Gibraltar\" \"Greenland\" \"Gambia\" \"Guinea\" \"Guadeloupe\" \"Equatorial Guinea\" \"Greece\" \"South Georgia & South Sandwich Islands\" \"Guatemala\" \"Guam\" \"Guinea-Bissau\" \"Guyana\" \"Hong Kong SAR China\" \"Heard & McDonald Islands\" \"Honduras\" \"Croatia\" \"Haiti\" \"Hungary\" \"Indonesia\" \"Ireland\" \"Israel\" \"Isle of Man\" \"India\" \"British Indian Ocean Territory\" \"Iraq\" \"Iran\" \"Iceland\" \"Italy\" \"Jersey\" \"Jamaica\" \"Jordan\" \"Japan\" \"Kenya\" \"Kyrgyzstan\" \"Cambodia\" \"Kiribati\" \"Comoros\" \"St Kitts & Nevis\" \"North Korea\" \"South Korea\" \"Kuwait\" \"Cayman Islands\" \"Kazakhstan\" \"Laos\" \"Lebanon\" \"St Lucia\" \"Liechtenstein\" \"Sri Lanka\" \"Liberia\" \"Lesotho\" \"Lithuania\" \"Luxembourg\" \"Latvia\" \"Libya\" \"Morocco\" \"Monaco\" \"Moldova\" \"Montenegro\" \"St Martin\" \"Madagascar\" \"Marshall Islands\" \"North Macedonia\" \"Mali\" \"Myanmar (Burma)\" \"Mongolia\" \"Macao SAR China\" \"Northern Mariana Islands\" \"Martinique\" \"Mauritania\" \"Montserrat\" \"Malta\" \"Mauritius\" \"Maldives\" \"Malawi\" \"Mexico\" \"Malaysia\" \"Mozambique\" \"Namibia\" \"New Caledonia\" \"Niger\" \"Norfolk Island\" \"Nigeria\" \"Nicaragua\" \"Netherlands\" \"Norway\" \"Nepal\" \"Nauru\" \"Niue\" \"New Zealand\" \"Oman\" \"Panama\" \"Peru\" \"French Polynesia\" \"Papua New Guinea\" \"Philippines\" \"Pakistan\" \"Poland\" \"St Pierre & Miquelon\" \"Pitcairn Islands\" \"Puerto Rico\" \"Palestinian Territories\" \"Portugal\" \"Palau\" \"Paraguay\" \"Qatar\" \"Réunion\" \"Romania\" \"Serbia\" \"Russia\" \"Rwanda\" \"Saudi Arabia\" \"Solomon Islands\" \"Seychelles\" \"Sudan\" \"Sweden\" \"Singapore\" \"St Helena\" \"Slovenia\" \"Svalbard & Jan Mayen\" \"Slovakia\" \"Sierra Leone\" \"San Marino\" \"Senegal\" \"Somalia\" \"Suriname\" \"South Sudan\" \"São Tomé & Príncipe\" \"El Salvador\" \"Sint Maarten\" \"Syria\" \"Eswatini\" \"Turks & Caicos Islands\" \"Chad\" \"French Southern Territories\" \"Togo\" \"Thailand\" \"Tajikistan\" \"Tokelau\" \"Timor-Leste\" \"Turkmenistan\" \"Tunisia\" \"Tonga\" \"Türkiye\" \"Trinidad & Tobago\" \"Tuvalu\" \"Taiwan\" \"Tanzania\" \"Ukraine\" \"Uganda\" \"US Outlying Islands\" \"United States\" \"Uruguay\" \"Uzbekistan\" \"Vatican City\" \"St Vincent & the Grenadines\" \"Venezuela\" \"British Virgin Islands\" \"US Virgin Islands\" \"Vietnam\" \"Vanuatu\" \"Wallis & Futuna\" \"Samoa\" \"Yemen\" \"Mayotte\" \"South Africa\" \"Zambia\" \"Zimbabwe\"]"))]

      (testing "assert assumptions"
        (is (true? (instance? java.util.Date #inst"2024-01-11T09:35:58.686-00:00"))))

      (testing "canary test for stress test"
        (is (= (freeze-and-hash stable-test-data) "LAVZGhT5YKbN56y99aVvMg=="))))))

;;;; Benchmarks

(deftest _benchmarks (is (benchmarks/bench {})))

# Setup

Add the [relevant dependency](../#latest-releases) to your project:

```clojure
Leiningen: [com.taoensso/nippy               "x-y-z"] ; or
deps.edn:   com.taoensso/nippy {:mvn/version "x-y-z"}
```

And setup your namespace imports:

```clojure
(ns my-app (:require [taoensso.nippy :as nippy]))
```

# De/serializing

As an example of what it can do, let's take a look at Nippy's own reference stress data:

```clojure
nippy/stress-data
=>
{:nil                   nil
 :true                  true
 :false                 false
 :boxed-false (Boolean. false)

 :char      \ಬ
 :str-short "ಬಾ ಇಲ್ಲಿ ಸಂಭವಿಸ"
 :str-long  (apply str (range 1000))
 :kw        :keyword
 :kw-ns     ::keyword
 :kw-long   (keyword
              (apply str "kw" (range 1000))
              (apply str "kw" (range 1000)))

 :sym       'foo
 :sym-ns    'foo/bar
 :sym-long   (symbol
               (apply str "sym" (range 1000))
               (apply str "sym" (range 1000)))

 :regex     #"^(https?:)?//(www\?|\?)?"

 ;;; Try reflect real-world data:
 :many-small-numbers  (vec (range 200))
 :many-small-keywords (->> (java.util.Locale/getISOLanguages)
                           (mapv keyword))
 :many-small-strings  (->> (java.util.Locale/getISOCountries)
                           (mapv #(.getDisplayCountry (java.util.Locale. "en" %))))

 :queue        (enc/queue [:a :b :c :d :e :f :g])
 :queue-empty  (enc/queue)
 :sorted-set   (sorted-set 1 2 3 4 5)
 :sorted-map   (sorted-map :b 2 :a 1 :d 4 :c 3)

 :list         (list 1 2 3 4 5 (list 6 7 8 (list 9 10 '(()))))
 :vector       [1 2 3 4 5 [6 7 8 [9 10 [[]]]]]
 :subvec       (subvec [1 2 3 4 5 6 7 8] 2 8)
 :map          {:a 1 :b 2 :c 3 :d {:e 4 :f {:g 5 :h 6 :i 7 :j {{} {}}}}}
 :set          #{1 2 3 4 5 #{6 7 8 #{9 10 #{#{}}}}}
 :meta         (with-meta {:a :A} {:metakey :metaval})
 :nested       [#{{1 [:a :b] 2 [:c :d] 3 [:e :f]} [#{{}}] #{:a :b}}
                #{{1 [:a :b] 2 [:c :d] 3 [:e :f]} [#{{}}] #{:a :b}}
                  [1 [1 2 [1 2 3 [1 2 3 4 [1 2 3 4 5]]]]]]

 :lazy-seq       (repeatedly 1000 rand)
 :lazy-seq-empty (map identity '())

 :byte         (byte   16)
 :short        (short  42)
 :integer      (int    3)
 :long         (long   3)
 :bigint       (bigint 31415926535897932384626433832795)

 :float        (float  3.14)
 :double       (double 3.14)
 :bigdec       (bigdec 3.1415926535897932384626433832795)

 :ratio        22/7
 :uri          (java.net.URI. "https://clojure.org/reference/data_structures")
 :uuid         (java.util.UUID/randomUUID)
 :util-date    (java.util.Date.)
 :sql-date     (java.sql.Date/valueOf "2023-06-21")

 ;;; JVM 8+
 :time-instant  (enc/compile-if java.time.Instant  (java.time.Instant/now)                nil)
 :time-duration (enc/compile-if java.time.Duration (java.time.Duration/ofSeconds 100 100) nil)
 :time-period   (enc/compile-if java.time.Period   (java.time.Period/of 1 1 1)            nil)

 :bytes         (byte-array [(byte 1) (byte 2) (byte 3)])
 :objects       (object-array [1 "two" {:data "data"}])

 :stress-record (StressRecord. "data")
 :stress-type   (StressType.   "data")

 ;; Serializable
 :throwable    (Throwable. "Yolo")
 :exception    (try (/ 1 0) (catch Exception e e))
 :ex-info      (ex-info "ExInfo" {:data "data"})}
```

Serialize it:

```clojure
(def frozen-stress-data (nippy/freeze nippy/stress-data))
=> #<byte[] [B@3253bcf3>
```

Deserialize it:

```clojure
(nippy/thaw frozen-stress-data)
=> {:bytes        (byte-array [(byte 1) (byte 2) (byte 3)])
    :nil          nil
    :boolean      true
    <...> }
```

Couldn't be simpler!

See also the lower-level [`freeze-to-out!`](https://taoensso.github.io/nippy/taoensso.nippy.html#var-freeze-to-out.21) and [`thaw-from-in!`](https://taoensso.github.io/nippy/taoensso.nippy.html#var-thaw-from-in.21) fns for operating on `DataOutput` and `DataInput` types directly.

# Encryption

> You may want to consider using Nippy with [Tempel](https://www.taoensso.com/tempel) for more comprehensive encryption options.

Nippy also gives you **dead simple data encryption**.  
Add a single option to your usual freeze/thaw calls like so:

```clojure
(nippy/freeze nippy/stress-data {:password [:salted "my-password"]}) ; Encrypt
(nippy/thaw   <encrypted-data>  {:password [:salted "my-password"]}) ; Decrypt
```

There's two default forms of encryption on offer: `:salted` and `:cached`. Each of these makes carefully-chosen trade-offs and is suited to one of two common use cases. See [`aes128-encryptor`](https://taoensso.github.io/nippy/taoensso.nippy.html#var-aes128-encryptor) for a detailed explanation of why/when you'd want one or the other.

# Custom types

It's easy to extend Nippy to your own data types:

```clojure
(defrecord MyType [data])

(nippy/extend-freeze MyType :my-type/foo ; A unique (namespaced) type identifier
  [x data-output]
  (.writeUTF data-output (:data x)))

(nippy/extend-thaw :my-type/foo ; Same type id
  [data-input]
  (MyType. (.readUTF data-input)))

(nippy/thaw (nippy/freeze (MyType. "Joe"))) => #taoensso.nippy.MyType{:data "Joe"}
```
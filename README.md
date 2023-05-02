<a href="https://www.taoensso.com" title="More stuff by @ptaoussanis at www.taoensso.com">
<img src="https://www.taoensso.com/taoensso-open-source.png" alt="Taoensso open-source" width="350"/></a>

**[CHANGELOG][]** | [API][] | current [Break Version][]:

```clojure
[com.taoensso/nippy "3.2.0"] ; See CHANGELOG for details
```

> See [here][backers] if to help support my open-source work, thanks! - [Peter Taoussanis][Taoensso.com]

# Nippy: the fastest serialization library for Clojure

Clojure's [rich data types] are *awesome*. And its [reader][] allows you to take your data just about anywhere. But the reader can be painfully slow when you've got a lot of data to crunch (like when you're serializing to a database).

Nippy is an attempt to provide a reliable, high-performance **drop-in alternative to the reader**. Used by the [Carmine Redis client][], the [Faraday DynamoDB client][], [PigPen][], [Onyx][] and others.

## Features
 * Small, simple **all-Clojure** library.
 * **Terrific performance** (the fastest for Clojure that I'm aware of).
 * Comprehesive **support for all standard data types**.
 * **Easily extendable to custom data types**.
 * Java's **Serializable** fallback when available.
 * **Reader-fallback** for all other types (including tagged literals)
 * **Full test coverage** for every supported type.
 * Fully pluggable **compression**, including built-in high-performance [LZ4][] compressor.
 * Fully pluggable **encryption**, including built-in high-strength AES128 enabled with a single `:password [:salted "my-password"]` option.
 * Utils for **easy integration into 3rd-party tools/libraries**.

## Getting started

Add the necessary dependency to your project:

```clojure
Leiningen: [com.taoensso/nippy "3.2.0"] ; or
deps.edn:   com.taoensso/nippy {:mvn/version "3.2.0"}
```

And setup your namespace imports:

```clojure
(ns my-ns (:require [taoensso.nippy :as nippy]))
```

### De/serializing

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
 :uri          (URI. "https://clojure.org/reference/data_structures")
 :uuid         (java.util.UUID/randomUUID)
 :date         (java.util.Date.)
 :sql-date     (java.sql.Date/valueOf "2023-06-21")

 ;;; JVM 8+
 :time-instant  (java.time.Instant/now)
 :time-duration (java.time.Duration/ofSeconds 100 100)
 :time-period   (java.time.Period/of 1 1 1)

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

See also the lower-level `freeze-to-out!` and `thaw-from-in!` fns for operating on `DataOutput` and `DataInput` types directly. 

### Encryption

Nippy also gives you **dead simple data encryption**. Add a single option to your usual freeze/thaw calls like so:

```clojure
(nippy/freeze nippy/stress-data {:password [:salted "my-password"]}) ; Encrypt
(nippy/thaw   <encrypted-data>  {:password [:salted "my-password"]}) ; Decrypt
```

There's two default forms of encryption on offer: `:salted` and `:cached`. Each of these makes carefully-chosen trade-offs and is suited to one of two common use cases. See the `aes128-encryptor` [API][] docs for a detailed explanation of why/when you'd want one or the other.

### Custom types

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

## Performance

Nippy is currently the **fastest serialization library for Clojure** that I'm aware of, and offers roundtrip times between **~10x and ~15x** faster than Clojure's `tools.reader.edn`, with a **~40% smaller output size**.

![benchmarks-png][]

Benchmark code is included with this repo, and can be easily run in your own environment.

## Contacting me / contributions

Please use the project's [GitHub issues page][] for all questions, ideas, etc. **Pull requests welcome**. See the project's [GitHub contributors page][] for a list of contributors.

Otherwise, you can reach me at [Taoensso.com][]. Happy hacking!

\- [Peter Taoussanis][Taoensso.com]

## License

Distributed under the [EPL v1.0][] \(same as Clojure).  
Copyright &copy; 2012-2022 [Peter Taoussanis][Taoensso.com].

<!--- Standard links -->
[Taoensso.com]: https://www.taoensso.com
[Break Version]: https://github.com/ptaoussanis/encore/blob/master/BREAK-VERSIONING.md
[backers]: https://taoensso.com/clojure/backers

<!--- Standard links (repo specific) -->
[CHANGELOG]: https://github.com/ptaoussanis/nippy/releases
[API]: http://ptaoussanis.github.io/nippy/
[GitHub issues page]: https://github.com/ptaoussanis/nippy/issues
[GitHub contributors page]: https://github.com/ptaoussanis/nippy/graphs/contributors
[EPL v1.0]: https://raw.githubusercontent.com/ptaoussanis/nippy/master/LICENSE
[Hero]: https://raw.githubusercontent.com/ptaoussanis/nippy/master/hero.png "Title"

<!--- Unique links -->
[rich data types]: http://clojure.org/reference/datatypes
[reader]: http://clojure.org/reference/reader
[Carmine Redis client]: https://github.com/ptaoussanis/carmine
[Faraday DynamoDB client]: https://github.com/ptaoussanis/faraday
[PigPen]: https://github.com/Netflix/PigPen
[Onyx]: https://github.com/onyx-platform/onyx
[LZ4]: https://code.google.com/p/lz4/
[benchmarks-png]: https://github.com/ptaoussanis/nippy/raw/master/benchmarks.png

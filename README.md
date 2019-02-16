<a href="https://www.taoensso.com" title="More stuff by @ptaoussanis at www.taoensso.com">
<img src="https://www.taoensso.com/taoensso-open-source.png" alt="Taoensso open-source" width="400"/></a>

**[CHANGELOG]** | [API] | current [Break Version]:

```clojure
[com.taoensso/nippy "2.14.0"]     ; Stable
[com.taoensso/nippy "2.15.0-RC1"] ; Dev, see CHANGELOG for details
```

> Please consider helping to [support my continued open-source Clojure/Script work]? 
> 
> Even small contributions can add up + make a big difference to help sustain my time writing, maintaining, and supporting Nippy and other Clojure/Script libraries. **Thank you!**
>
> \- Peter Taoussanis

# Nippy

## The fastest serialization library for Clojure

Clojure's [rich data types] are *awesome*. And its [reader] allows you to take your data just about anywhere. But the reader can be painfully slow when you've got a lot of data to crunch (like when you're serializing to a database).

Nippy is an attempt to provide a reliable, high-performance **drop-in alternative to the reader**. Used by the [Carmine Redis client], the [Faraday DynamoDB client], [PigPen], [Onyx] and others.

## Features
 * Small, uncomplicated **all-Clojure** library
 * **Terrific performance** (the fastest for Clojure that I'm aware of)
 * Comprehesive **support for all standard data types**
 * **Easily extendable to custom data types** (v2.1+)
 * Java's **Serializable** fallback when available (v2.5+)
 * **Reader-fallback** for all other types (including Clojure 1.4+ tagged literals)
 * **Full test coverage** for every supported type
 * Fully pluggable **compression**, including built-in high-performance [LZ4] compressor
 * Fully pluggable **encryption**, including built-in high-strength AES128 enabled with a single `:password [:salted "my-password"]` option (v2+)
 * Utils for **easy integration into 3rd-party tools/libraries** (v2+)

## Getting started

Add the necessary dependency to your project:

```clojure
[com.taoensso/nippy "2.14.0"]
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
{:bytes     (byte-array [(byte 1) (byte 2) (byte 3)])
 :nil       nil
 :true      true
 :false     false
 :char      \ಬ
 :str-short "ಬಾ ಇಲ್ಲಿ ಸಂಭವಿಸ"
 :str-long  (apply str (range 1000))
 :kw        :keyword
 :kw-ns     ::keyword
 :sym       'foo
 :sym-ns    'foo/bar
 :regex     #"^(https?:)?//(www\?|\?)?"

 :queue        (-> (PersistentQueue/EMPTY) (conj :a :b :c :d :e :f :g))
 :queue-empty  (PersistentQueue/EMPTY)
 :queue-empty  (enc/queue)
 :sorted-set   (sorted-set 1 2 3 4 5)
 :sorted-map   (sorted-map :b 2 :a 1 :d 4 :c 3)

 :list         (list 1 2 3 4 5 (list 6 7 8 (list 9 10)))
 :list-quoted  '(1 2 3 4 5 (6 7 8 (9 10)))
 :list-empty   (list)
 :vector       [1 2 3 4 5 [6 7 8 [9 10]]]
 :vector-empty []
 :map          {:a 1 :b 2 :c 3 :d {:e 4 :f {:g 5 :h 6 :i 7}}}
 :map-empty    {}
 :set          #{1 2 3 4 5 #{6 7 8 #{9 10}}}
 :set-empty    #{}
 :meta         (with-meta {:a :A} {:metakey :metaval})
 :nested       [#{{1 [:a :b] 2 [:c :d] 3 [:e :f]} [] #{:a :b}}
                #{{1 [:a :b] 2 [:c :d] 3 [:e :f]} [] #{:a :b}}
                [1 [1 2 [1 2 3 [1 2 3 4 [1 2 3 4 5]]]]]]

 :lazy-seq       (repeatedly 1000 rand)
 :lazy-seq-empty (map identity '())

 :byte         (byte 16)
 :short        (short 42)
 :integer      (int 3)
 :long         (long 3)
 :bigint       (bigint 31415926535897932384626433832795)

 :float        (float 3.14)
 :double       (double 3.14)
 :bigdec       (bigdec 3.1415926535897932384626433832795)

 :ratio        22/7
 :uuid         (java.util.UUID/randomUUID)
 :date         (java.util.Date.)

 :stress-record (StressRecord. "data")

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

### Encryption (v2+)

Nippy also gives you **dead simple data encryption**. Add a single option to your usual freeze/thaw calls like so:

```clojure
(nippy/freeze nippy/stress-data {:password [:salted "my-password"]}) ; Encrypt
(nippy/thaw   <encrypted-data>  {:password [:salted "my-password"]}) ; Decrypt
```

There's two default forms of encryption on offer: `:salted` and `:cached`. Each of these makes carefully-chosen trade-offs and is suited to one of two common use cases. See the `aes128-encryptor` [API] docs for a detailed explanation of why/when you'd want one or the other.

### Custom types (v2.1+)

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

![benchmarks-png]

[Detailed benchmark info] is available on Google Docs.

## Contacting me / contributions

Please use the project's [GitHub issues page] for all questions, ideas, etc. **Pull requests welcome**. See the project's [GitHub contributors page] for a list of contributors.

Otherwise, you can reach me at [Taoensso.com]. Happy hacking!

\- [Peter Taoussanis]

## License

Distributed under the [EPL v1.0] \(same as Clojure).  
Copyright &copy; 2012-2016 [Peter Taoussanis].

<!--- Standard links -->
[Taoensso.com]: https://www.taoensso.com
[Peter Taoussanis]: https://www.taoensso.com
[@ptaoussanis]: https://www.taoensso.com
[More by @ptaoussanis]: https://www.taoensso.com
[Break Version]: https://github.com/ptaoussanis/encore/blob/master/BREAK-VERSIONING.md
[support my continued open-source Clojure/Script work]: http://taoensso.com/clojure/backers

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
[Detailed benchmark info]: https://docs.google.com/spreadsheet/ccc?key=0AuSXb68FH4uhdE5kTTlocGZKSXppWG9sRzA5Y2pMVkE

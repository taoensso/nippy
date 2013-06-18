Current [semantic](http://semver.org/) version:

```clojure
[com.taoensso/nippy "1.2.1"]       ; Stable
[com.taoensso/nippy "2.0.0-beta1"] ; Development (notes below)
```

v2 adds pluggable compression, crypto support (also pluggable), an improved API (including much better error messages), easier integration into other tools/libraries, and hugely improved performance. 

v2 **is backwards compatible**, but please note that the old `freeze-to-bytes`/`thaw-from-bytes` API has been **deprecated** in favor of `freeze`/`thaw`. 

**PLEASE REPORT ANY PROBLEMS!**

# Nippy, a Clojure serialization library

Clojure's [rich data types](http://clojure.org/datatypes) are *awesome*. And its [reader](http://clojure.org/reader) allows you to take your data just about anywhere. But the reader can be painfully slow when you've got a lot of data to crunch (like when you're serializing to a database).

Nippy is an attempt to provide a reliable, high-performance **drop-in alternative to the reader**. It's used, among others, as the [Carmine Redis client](https://github.com/ptaoussanis/carmine) and [Faraday DynamoDB client](https://github.com/ptaoussanis/faraday) serializer.

## What's in the box™?
  * Small, uncomplicated **all-Clojure** library.
  * **Great performance**.
  * Comprehesive, extensible **support for all major data types**.
  * **Reader-fallback** for difficult/future types (including Clojure 1.4+ tagged literals).
  * **Full test coverage** for every supported type.
  * Fully pluggable **compression**, including built-in high-performance [Snappy](http://code.google.com/p/snappy/) compressor.
  * Fully pluggable **encryption**, including built-in high-strength AES128 enabled with a single `:password [:salted "my-password"]` option. (v2+)
  * Utils for **easy integration into 3rd-party tools/libraries**. (v2+)

## Getting started

### Dependencies

Add the necessary dependency to your [Leiningen](http://leiningen.org/) `project.clj` and `require` the library in your ns:

```clojure
[com.taoensso/nippy "1.2.1"] ; project.clj
(ns my-app (:require [taoensso.nippy :as nippy])) ; ns
```

### De/serializing

As an example of what Nippy can do, let's take a look at its own reference stress data:

```clojure
nippy/stress-data
=>
{:bytes        (byte-array [(byte 1) (byte 2) (byte 3)])
 :nil          nil
 :boolean      true

 :char-utf8    \ಬ
 :string-utf8  "ಬಾ ಇಲ್ಲಿ ಸಂಭವಿಸ"
 :string-long  (apply str (range 1000))
 :keyword      :keyword
 :ns-keyword   ::keyword

 :queue        (-> (PersistentQueue/EMPTY) (conj :a :b :c :d :e :f :g))
 :queue-empty  (PersistentQueue/EMPTY)
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
 :coll         (repeatedly 1000 rand)

 :byte         (byte 16)
 :short        (short 42)
 :integer      (int 3)
 :long         (long 3)
 :bigint       (bigint 31415926535897932384626433832795)

 :float        (float 3.14)
 :double       (double 3.14)
 :bigdec       (bigdec 3.1415926535897932384626433832795)

 :ratio        22/7
 :tagged-uuid  (java.util.UUID/randomUUID)
 :tagged-date  (java.util.Date.)}
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

### Encryption (currently in **ALPHA**)

Nippy v2+ also gives you **dead simple data encryption**. Add a single option to your usual freeze/thaw calls like so:

```clojure
(nippy/freeze nippy/stress-data {:password [:salted "my-password"]}) ; Encrypt
(nippy/thaw   <encrypted-data>  {:password [:salted "my-password"]}) ; Decrypt
```

There's two default forms of encryption on offer: `:salted` and `:cached`. Each of these makes carefully-chosen trade-offs and is suited to one of two common use cases. See the `default-aes128-encryptor` [docstring](http://ptaoussanis.github.io/nippy/taoensso.nippy.encryption.html) for a detailed explanation of why/when you'd want one or the other.

## Performance

![Comparison chart](https://github.com/ptaoussanis/nippy/raw/master/benchmarks/chart.png)

[Detailed benchmark information](https://docs.google.com/spreadsheet/ccc?key=0AuSXb68FH4uhdE5kTTlocGZKSXppWG9sRzA5Y2pMVkE&pli=1#gid=0) is available on Google Docs.

## Project links

  * [API documentation](http://ptaoussanis.github.io/nippy/).
  * My other [Clojure libraries](https://www.taoensso.com/clojure-libraries) (Redis & DynamoDB clients, logging+profiling, i18n+L10n, serialization, A/B testing).

##### This project supports the **CDS and ClojureWerkz project goals**:

  * [CDS](http://clojure-doc.org/), the **Clojure Documentation Site**, is a contributer-friendly community project aimed at producing top-notch Clojure tutorials and documentation.

  * [ClojureWerkz](http://clojurewerkz.org/) is a growing collection of open-source, batteries-included **Clojure libraries** that emphasise modern targets, great documentation, and thorough testing.

## Contact & contribution

Please use the [project's GitHub issues page](https://github.com/ptaoussanis/nippy/issues) for project questions/comments/suggestions/whatever **(pull requests welcome!)**. Am very open to ideas if you have any!

Otherwise reach me (Peter Taoussanis) at [taoensso.com](https://www.taoensso.com) or on Twitter ([@ptaoussanis](https://twitter.com/#!/ptaoussanis)). Cheers!

## License

Copyright &copy; 2012, 2013 Peter Taoussanis. Distributed under the [Eclipse Public License](http://www.eclipse.org/legal/epl-v10.html), the same as Clojure.

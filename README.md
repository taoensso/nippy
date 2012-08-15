Current [semantic](http://semver.org/) version:

```clojure
[com.taoensso/nippy "0.10.1"]
```

**Breaking changes** since _0.9.x_:
 * Affecting **users that were manually disabling compression**:
   * API has changed for `freeze-to-bytes` and `thaw-from-bytes` when not using default options.

# Nippy, a serialization library for Clojure

Clojure's [rich data types](http://clojure.org/datatypes) are *awesome*. And its [reader](http://clojure.org/reader) allows you to take your data just about anywhere. But the reader can be painfully slow when you've got a lot of data to crunch (like when you're serializing to a database).

Nippy is an attempt to provide a drop-in, high-performance alternative to the reader. It's a fork of [Deep-Freeze](https://github.com/halgari/deep-freeze) and is used as the [Carmine Redis client](https://github.com/ptaoussanis/carmine) serializer.

## What's In The Box?
 * Simple, **high-performance** all-Clojure de/serializer.
 * Comprehesive, extensible **support for all major data types**.
 * **Reader-fallback** for difficult/future types (including Clojure 1.4+ tagged literals).
 * **Full test coverage** for every supported type.
 * [Snappy](http://code.google.com/p/snappy/) **integrated de/compression** for efficient storage and network transfer.

## Status [![Build Status](https://secure.travis-ci.org/ptaoussanis/nippy.png?branch=master)](http://travis-ci.org/ptaoussanis/nippy)

Nippy is still currently *experimental*. It **has not yet been thoroughly tested in production** and its API is subject to change. To run tests against all supported Clojure versions, use:

```bash
lein2 all test
```

### Known issue with Java 7 on OSX

Nippy uses [Snappy](http://code.google.com/p/snappy-java/) which currently has a minor path issue with Java 7 on OSX. Please see [here](https://github.com/ptaoussanis/carmine/issues/5#issuecomment-6450607) for a workaround until a proper fix is available.

## Getting Started

### Leiningen

Depend on Nippy in your `project.clj`:

```clojure
[com.taoensso/nippy "0.10.1"]
```

and `require` the library:

```clojure
(ns my-app (:require [taoensso.nippy :as nippy]))
```

### De/Serializing

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
 :queue        (-> (PersistentQueue/EMPTY) (conj :a :b :c :d :e :f :g))
 :queue-empty  (PersistentQueue/EMPTY)
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

 ;; Clojure 1.4+
 ;; :tagged-uuid  (java.util.UUID/randomUUID)
 ;; :tagged-date  (java.util.Date.)
 }
```

Serialize it:

```clojure
(def frozen-stress-data (nippy/freeze-to-bytes nippy/stress-data))
=> #<byte[] [B@3253bcf3>
```

Deserialize it:

```clojure
(nippy/thaw-from-bytes frozen-stress-data)
=> {:bytes        (byte-array [(byte 1) (byte 2) (byte 3)])
    :nil          nil
    :boolean      true
    <...> }
```

Couldn't be simpler!

## Performance

![Performance comparison chart](https://github.com/ptaoussanis/nippy/raw/master/benchmarks/chart1.png)

![Data size chart](https://github.com/ptaoussanis/nippy/raw/master/benchmarks/chart2.png)

[Detailed benchmark information](https://docs.google.com/spreadsheet/ccc?key=0AuSXb68FH4uhdE5kTTlocGZKSXppWG9sRzA5Y2pMVkE&pli=1#gid=0) is available on Google Docs.

## Nippy supports the ClojureWerkz Project Goals

ClojureWerkz is a growing collection of open-source, batteries-included [Clojure libraries](http://clojurewerkz.org/) that emphasise modern targets, great documentation, and thorough testing.

## Contact & Contribution

Reach me (Peter Taoussanis) at *ptaoussanis at gmail.com* for questions/comments/suggestions/whatever. I'm very open to ideas if you have any!

I'm also on Twitter: [@ptaoussanis](https://twitter.com/#!/ptaoussanis).

## License

Copyright &copy; 2012 Peter Taoussanis

Distributed under the [Eclipse Public License](http://www.eclipse.org/legal/epl-v10.html), the same as Clojure.
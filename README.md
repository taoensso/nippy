**[API docs][]** | **[CHANGELOG][]** | [other Clojure libs][] | [Twitter][] | [contact/contrib](#contact--contributing) | current [Break Version][]:

```clojure
[com.taoensso/nippy "2.9.1"]        ; Stable
[com.taoensso/nippy "2.10.0-beta1"] ; Dev, see CHANGELOG for details
```

# Nippy, a Clojure serialization library

Clojure's [rich data types](http://clojure.org/datatypes) are *awesome*. And its [reader](http://clojure.org/reader) allows you to take your data just about anywhere. But the reader can be painfully slow when you've got a lot of data to crunch (like when you're serializing to a database).

Nippy is an attempt to provide a reliable, high-performance **drop-in alternative to the reader**. It's used as the serializer for the [Carmine Redis client](https://github.com/ptaoussanis/carmine), the [Faraday DynamoDB client](https://github.com/ptaoussanis/faraday), and a number of other projects.

## What's in the box™?
  * Small, uncomplicated **all-Clojure** library
  * **Terrific performance** (the fastest for Clojure that I'm aware of)
  * Comprehesive **support for all standard data types**
  * **Easily extendable to custom data types** (v2.1+)
  * Java's **Serializable** fallback when available (v2.5+)
  * **Reader-fallback** for all other types (including Clojure 1.4+ tagged literals)
  * **Full test coverage** for every supported type
  * Fully pluggable **compression**, including built-in high-performance [LZ4](https://code.google.com/p/lz4/) compressor
  * Fully pluggable **encryption**, including built-in high-strength AES128 enabled with a single `:password [:salted "my-password"]` option (v2+)
  * Utils for **easy integration into 3rd-party tools/libraries** (v2+)

## Getting started

### Dependencies

Add the necessary dependency to your [Leiningen][] `project.clj` and `require` the library in your ns:

```clojure
[com.taoensso/nippy "2.9.1"] ; project.clj
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
 :lazy-seq     (repeatedly 1000 rand)

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

 :stress-record (->StressRecord "data")

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

There's two default forms of encryption on offer: `:salted` and `:cached`. Each of these makes carefully-chosen trade-offs and is suited to one of two common use cases. See the `aes128-encryptor` [docstring](http://ptaoussanis.github.io/nippy/taoensso.nippy.encryption.html) for a detailed explanation of why/when you'd want one or the other.

### Custom types (v2.1+)

```clojure
(defrecord MyType [data])

(nippy/extend-freeze MyType :my-type/foo ; A unique (namespaced) type identifier
  [x data-output]
  (.writeUTF data-output (:data x)))

(nippy/extend-thaw :my-type/foo ; Same type id
  [data-input]
  (->MyType (.readUTF data-input)))

(nippy/thaw (nippy/freeze (->MyType "Joe"))) => #taoensso.nippy.MyType{:data "Joe"}
```

## Performance

![Comparison chart](https://github.com/ptaoussanis/nippy/raw/master/benchmarks.png)

[Detailed benchmark information](https://docs.google.com/spreadsheet/ccc?key=0AuSXb68FH4uhdE5kTTlocGZKSXppWG9sRzA5Y2pMVkE) is available on Google Docs.

## Contact & contributing

`lein start-dev` to get a (headless) development repl that you can connect to with [Cider][] (Emacs) or your IDE.

Please use the project's GitHub [issues page][] for project questions/comments/suggestions/whatever **(pull requests welcome!)**. Am very open to ideas if you have any!

Otherwise reach me (Peter Taoussanis) at [taoensso.com][] or on [Twitter][]. Cheers!

## License

Copyright &copy; 2012-2014 Peter Taoussanis. Distributed under the [Eclipse Public License][], the same as Clojure.


[API docs]: http://ptaoussanis.github.io/nippy/
[CHANGELOG]: https://github.com/ptaoussanis/nippy/releases
[other Clojure libs]: https://www.taoensso.com/clojure
[taoensso.com]: https://www.taoensso.com
[Twitter]: https://twitter.com/ptaoussanis
[issues page]: https://github.com/ptaoussanis/nippy/issues
[commit history]: https://github.com/ptaoussanis/nippy/commits/master
[Break Version]: https://github.com/ptaoussanis/encore/blob/master/BREAK-VERSIONING.md
[Leiningen]: http://leiningen.org/
[Cider]: https://github.com/clojure-emacs/cider
[CDS]: http://clojure-doc.org/
[ClojureWerkz]: http://clojurewerkz.org/
[Eclipse Public License]: https://raw2.github.com/ptaoussanis/nippy/master/LICENSE

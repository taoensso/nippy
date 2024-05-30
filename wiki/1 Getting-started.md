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

As an example of what it can do, let's take a look at Nippy's own reference [stress data](https://taoensso.github.io/nippy/taoensso.nippy.html#var-stress-data):

```clojure
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
 :queue-empty    (into clojure.lang.PersistentQueue/EMPTY [:a :b :c :d :e :f :g])
 :queue                clojure.lang.PersistentQueue/EMPTY

 :uuid       (java.util.UUID. 7232453380187312026 -7067939076204274491)
 :uri        (java.net.URI. "https://clojure.org")
 :defrecord  (nippy/StressRecord. "data")
 :deftype    (nippy/StressType.   "data")
 :bytes      (byte-array   [(byte 1) (byte 2) (byte 3)])
 :objects    (object-array [1 "two" {:data "data"}])

 :util-date (java.util.Date. 1577884455500)
 :sql-date  (java.sql.Date.  1577884455500)
 :instant   (java.time.Instant/parse "2020-01-01T13:14:15.50Z")
 :duration  (java.time.Duration/ofSeconds 100 100)
 :period    (java.time.Period/of 1 1 1)

 :throwable (Throwable. "Msg")
 :exception (Exception. "Msg")
 :ex-info   (ex-info    "Msg" {:data "data"})

 :many-longs    (vec (repeatedly 512         #(rand-nth (range 10))))
 :many-doubles  (vec (repeatedly 512 #(double (rand-nth (range 10)))))
 :many-strings  (vec (repeatedly 512         #(rand-nth ["foo" "bar" "baz" "qux"])))
 :many-keywords (vec (repeatedly 512
                       #(keyword
                          (rand-nth ["foo" "bar" "baz" "qux" nil])
                          (rand-nth ["foo" "bar" "baz" "qux"    ]))))}
```

Serialize it:

```clojure
(def frozen-stress-data (nippy/freeze (nippy/stress-data {})))
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

# Streaming

- To serialize directly to a `java.io.DataInput`, see [`freeze-to-out!`](https://taoensso.github.io/nippy/taoensso.nippy.html#var-freeze-to-out.21).
- To deserialize directly from a `java.io.DataOutput`, see [`thaw-from-in!`](https://taoensso.github.io/nippy/taoensso.nippy.html#var-thaw-from-in.21).

# Encryption

> You may want to consider using Nippy with [Tempel](https://www.taoensso.com/tempel) for more comprehensive encryption options.

Nippy also gives you **dead simple data encryption**.  
Add a single option to your usual freeze/thaw calls like so:

```clojure
(nippy/freeze (nippy/stress-data {}) {:password [:salted "my-password"]}) ; Encrypt
(nippy/thaw   <encrypted-data>       {:password [:salted "my-password"]}) ; Decrypt
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
Current [semantic](http://semver.org/) version:

```clojure
[com.taoensso/nippy "0.9.0"]
```

# Nippy, a serialization library for Clojure

Clojure's [rich data types](http://clojure.org/datatypes) are *awesome*. And its [reader](http://clojure.org/reader) allows you to take your data just about anywhere. But the reader can be painfully slow when you've got a lot of data to crunch (like when you're serializing to a database).

Nippy is an attempt to provide a drop-in, high-performance alternative to the reader. It's a fork of [Deep-Freeze](https://github.com/halgari/deep-freeze).

## What's In The Box?
 * Simple, **high-performance** all-Clojure de/serializer.
 * Comprehesive, extensible **support for all major data types**.
 * **Reader-fallback** for difficult/future types.
 * **Full test coverage** for every supported type.
 * [Snappy](http://code.google.com/p/snappy/) **integrated de/compression** for efficient storage and network transfer.

## Status [![Build Status](https://secure.travis-ci.org/ptaoussanis/nippy.png?branch=master)](http://travis-ci.org/ptaoussanis/nippy)

Nippy is relatively mature and is used as the [Carmine Redis client](https://github.com/ptaoussanis/carmine) serializer. The API is expected to remain more or less stable. To run tests against all supported Clojure versions, use:

```bash
lein2 all test
```

### Known issue with Java 7 on OSX

Nippy uses [Snappy](http://code.google.com/p/snappy-java/) which currently has a minor path issue with Java 7 on OSX. Please see [here](https://github.com/ptaoussanis/carmine/issues/5#issuecomment-6450607) for a workaround until a proper fix is available.

## Getting Started

### Leiningen

Depend on Nippy in your `project.clj`:

```clojure
[com.taoensso/nippy "0.9.0"]
```

and `require` the library:

```clojure
(ns my-app (:require [taoensso.nippy :as nippy]))
```

### De/Serializing

TODO

## Performance

TODO

![Performance comparison chart]()

[Detailed benchmark information](https://docs.google.com/spreadsheet/ccc?key=0AuSXb68FH4uhdE5kTTlocGZKSXppWG9sRzA5Y2pMVkE&pli=1#gid=0) is available on Google Docs.

## Nippy supports the ClojureWerkz Project Goals

ClojureWerkz is a growing collection of open-source, batteries-included [Clojure libraries](http://clojurewerkz.org/) that emphasise modern targets, great documentation, and thorough testing.

## Contact & Contribution

Reach me (Peter Taoussanis) at *ptaoussanis at gmail.com* for questions/comments/suggestions/whatever. I'm very open to ideas if you have any!

I'm also on Twitter: [@ptaoussanis](https://twitter.com/#!/ptaoussanis).

## License

Copyright &copy; 2012 Peter Taoussanis

Distributed under the [Eclipse Public License](http://www.eclipse.org/legal/epl-v10.html), the same as Clojure.
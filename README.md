<a href="https://www.taoensso.com/clojure" title="More stuff by @ptaoussanis at www.taoensso.com"><img src="https://www.taoensso.com/open-source.png" alt="Taoensso open source" width="340"/></a>  
[**API**][cljdoc] | [**Wiki**][GitHub wiki] | [Latest releases](#latest-releases) | [Get support][GitHub issues]

# Nippy

### Fast serialization library for Clojure

Clojure's rich data types are awesome. And its [reader](https://clojure.org/reference/reader) allows you to take your data just about anywhere. But the reader can be painfully slow when you've got a lot of data to crunch (like when you're serializing to a database).

Nippy is a mature, high-performance **drop-in alternative to the reader**.

It is used at scale by [Carmine](https://www.taoensso.com/carmine), [Faraday](https://www.taoensso.com/faraday), [PigPen](https://github.com/Netflix/PigPen), [Onyx](https://github.com/onyx-platform/onyx), [XTDB](https://github.com/xtdb/xtdb), [Datalevin](https://github.com/juji-io/datalevin), and others.

## Latest release/s

- `2025-05-18` `v3.6.0`: [release info](../../releases/tag/v3.6.0)

[![Clj tests][Clj tests SVG]][Clj tests URL]
[![Graal tests][Graal tests SVG]][Graal tests URL]

See [here][GitHub releases] for earlier releases.

## Why Nippy?

- Small, simple **pure-Clojure** library
- **Terrific performance**: the [best](#performance) for Clojure that I'm aware of
- Comprehensive support for [all standard data types](../../wiki/1-Getting-started#deserializing)
- Easily extendable to [custom data types](../../wiki/1-Getting-started#custom-types)
- **Robust test suite** incl. coverage of every supported type
- **Mature** and widely used in production for 12+ years
- Optional auto fallback to [Java Serializable](https://taoensso.github.io/nippy/taoensso.nippy.html#var-*freeze-serializable-allowlist*) for [safe](https://cljdoc.org/d/com.taoensso/nippy/CURRENT/api/taoensso.nippy#*freeze-serializable-allowlist*) types
- Optional auto fallback to Clojure Reader (including tagged literals)
- Optional smart **compression** with [LZ4](https://code.google.com/p/lz4/) or [Zstandard](https://facebook.github.io/zstd/)
- Optional [encryption](../../wiki/1-Getting-started#encryption) with AES128
- [Tools](https://taoensso.github.io/nippy/taoensso.nippy.tools.html) for easy + robust **integration into 3rd-party libraries**, etc.
- Powerful [thaw transducer](https://taoensso.github.io/nippy/taoensso.nippy.html#var-*thaw-xform*) for flexible data inspection and transformation

## Quick example

Nippy's super easy to use:

```clojure
(require '[taoensso.nippy :as nippy])

;; Freeze any Clojure value
(nippy/freeze <my-value>) ; => Serialized byte[]

;; Thaw the byte[] to get back the original value:
(nippy/thaw (nippy/freeze <my-value>)) ; => <my-value>
```

See the [wiki](https://github.com/taoensso/nippy/wiki/1-Getting-started#deserializing) for more.

## Operational considerations

### Data longevity

Nippy is widely used to store **long-lived** data and promises (as always) that **data serialized today should be readable by all future versions of Nippy**.

But please note that the **converse is not generally true**:

- Nippy `vX` **should** be able to read all data from Nippy `vY<=X` (backwards compatibility)
- Nippy `vX` **may/not** be able to read all data from Nippy `vY>X` (forwards compatibility)

### Rolling updates and rollback

From time to time, Nippy may introduce:

- Support for serializing **new types**
- Optimizations to the serialization of **pre-existing types**

To help ease **rolling updates** and to better support **rollback**, Nippy (since version v3.4) will always introduce such changes over **two version releases**:

- Release 1: to add **read support** for the new types
- Release 2: to add **write support** for the new types

Starting from v3.4, Nippy's release notes will **always clearly indicate** if a particular update sequence is recommended.

### Stability of byte output

It has **never been an objective** of Nippy to offer **predictable byte output**, and I'd generally **recommend against** depending on specific byte output.

However, I know that a small minority of users *do* have specialized needs in this area.

So starting with Nippy v3.4, Nippy's release notes will **always clearly indicate** if any changes to byte output are expected.

## Performance

Nippy is fast! Latest [benchmark](../../blob/master/test/taoensso/nippy_benchmarks.clj) results:

![benchmarks-png](../../raw/master/benchmarks.png)

PRs welcome to include other alternatives in the bench suite!

## Documentation

- [Wiki][GitHub wiki] (getting started, usage, etc.)
- API reference via [cljdoc][cljdoc]

## Funding

You can [help support][sponsor] continued work on this project, thank you!! 🙏

## License

Copyright &copy; 2012-2025 [Peter Taoussanis][].  
Licensed under [EPL 1.0](LICENSE.txt) (same as Clojure).

<!-- Common -->

[GitHub releases]: ../../releases
[GitHub issues]:   ../../issues
[GitHub wiki]:     ../../wiki

[Peter Taoussanis]: https://www.taoensso.com
[sponsor]:          https://www.taoensso.com/sponsor

<!-- Project -->

[cljdoc]: https://cljdoc.org/d/com.taoensso/nippy/CURRENT/api/taoensso.nippy

[Clojars SVG]: https://img.shields.io/clojars/v/com.taoensso/nippy.svg
[Clojars URL]: https://clojars.org/com.taoensso/nippy

[Clj tests SVG]:  https://github.com/taoensso/nippy/actions/workflows/clj-tests.yml/badge.svg
[Clj tests URL]:  https://github.com/taoensso/nippy/actions/workflows/clj-tests.yml
[Graal tests SVG]: https://github.com/taoensso/nippy/actions/workflows/graal-tests.yml/badge.svg
[Graal tests URL]: https://github.com/taoensso/nippy/actions/workflows/graal-tests.yml

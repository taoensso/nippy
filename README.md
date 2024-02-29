<a href="https://www.taoensso.com/clojure" title="More stuff by @ptaoussanis at www.taoensso.com"><img src="https://www.taoensso.com/open-source.png" alt="Taoensso open source" width="340"/></a>  
[**Documentation**](#documentation) | [Latest releases](#latest-releases) | [Get support][GitHub issues]

# Nippy

### The fastest serialization library for Clojure

Clojure's rich data types are awesome. And its [reader](https://clojure.org/reference/reader) allows you to take your data just about anywhere. But the reader can be painfully slow when you've got a lot of data to crunch (like when you're serializing to a database).

Nippy is an attempt to provide a reliable, high-performance **drop-in alternative to the reader**.

Used by [Carmine](https://www.taoensso.com/carmine), [Faraday](https://www.taoensso.com/faraday), [PigPen](https://github.com/Netflix/PigPen), [Onyx](https://github.com/onyx-platform/onyx), 
[XTDB](https://github.com/xtdb/xtdb), [Datalevin](https://github.com/juji-io/datalevin), and others.

## Latest release/s

- `2023-10-11` `3.3.0` (stable): [release info](../../releases/tag/v3.3.0)
- `2024-02-26` `3.4.0-RC2` (dev): [release info](../../releases/tag/v3.4.0-RC2)

[![Main tests][Main tests SVG]][Main tests URL]
[![Graal tests][Graal tests SVG]][Graal tests URL]

See [here][GitHub releases] for earlier releases.

## Why Nippy?

- Small, simple **all-Clojure** library
- **Terrific performance**: the [best](#performance) for Clojure that I'm aware of
- Comprehensive support for [all standard data types](../../wiki/1-Getting-started#deserializing)
- Easily extendable to [custom data types](../../wiki/1-Getting-started#custom-types)
- **Robust test suite**, incl. full coverage for every supported type
- Auto fallback to [Java Serializable](https://taoensso.github.io/nippy/taoensso.nippy.html#var-*freeze-serializable-allowlist*) when available
- Auto fallback to Clojure Reader for all other types (including tagged literals)
- Pluggable **compression** with built-in [LZ4](https://code.google.com/p/lz4/), [Zstandard](https://facebook.github.io/zstd/), etc.
- Pluggable [encryption](../../wiki/1-Getting-started#encryption) with built-in AES128
- [Tools](https://taoensso.github.io/nippy/taoensso.nippy.tools.html) for easy + robust **integration into 3rd-party libraries**, etc.
- Powerful [thaw transducer](https://taoensso.github.io/nippy/taoensso.nippy.html#var-*thaw-xform*) for flexible data inspection and transformation

## Performance

Since its earliest versions, Nippy has consistently been the **fastest serialization library for Clojure** that I'm aware of. Latest [benchmark](../../blob/master/test/taoensso/nippy_benchmarks.clj) results:

![benchmarks-png](../../raw/master/benchmarks.png)

## Documentation

- [Wiki][GitHub wiki] (getting started, usage, etc.)
- API reference: [Codox][Codox docs], [clj-doc][clj-doc docs]

## Funding

You can [help support][sponsor] continued work on this project, thank you!! 🙏

## License

Copyright &copy; 2012-2024 [Peter Taoussanis][].  
Licensed under [EPL 1.0](LICENSE.txt) (same as Clojure).

<!-- Common -->

[GitHub releases]: ../../releases
[GitHub issues]:   ../../issues
[GitHub wiki]:     ../../wiki

[Peter Taoussanis]: https://www.taoensso.com
[sponsor]:          https://www.taoensso.com/sponsor

<!-- Project -->

[Codox docs]:   https://taoensso.github.io/nippy/
[clj-doc docs]: https://cljdoc.org/d/com.taoensso/nippy/

[Clojars SVG]: https://img.shields.io/clojars/v/com.taoensso/nippy.svg
[Clojars URL]: https://clojars.org/com.taoensso/nippy

[Main tests SVG]:  https://github.com/taoensso/nippy/actions/workflows/main-tests.yml/badge.svg
[Main tests URL]:  https://github.com/taoensso/nippy/actions/workflows/main-tests.yml
[Graal tests SVG]: https://github.com/taoensso/nippy/actions/workflows/graal-tests.yml/badge.svg
[Graal tests URL]: https://github.com/taoensso/nippy/actions/workflows/graal-tests.yml

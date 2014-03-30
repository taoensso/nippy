## v2.6.0 / 2014 Mar 30

> **Major release** with efficiency improvements, reliability improvements, and some new utils.

### New

 * Low-level fns added: `freeze-to-out!`, `thaw-from-in!` for operating directly on DataOutputs/DataInputs.
 * Data size optimizations for some common small data types (small strings/keywords, small integers).
 * New test suite added to ensure a 1-to-1 value->binary representation mapping for all core data types. This will be a guarantee kept going forward.
 * New `:skip-header?` `freeze` option to freeze data without standard Nippy headers (can be useful in very performance sensitive environments).
 * New benchmarks added, notably a Fressian comparison.
 * Added experimental `freezable?` util fn to main ns.
 * Added some property-based [simple-check](https://github.com/reiddraper/simple-check) roundtrip tests.
 * Public utils now available for custom type extension: `write-bytes`, `write-biginteger`, `write-utf8`, `write-compact-long`, and respective readers.


### Changes

 * **BREAKING**: the experimental `Compressable-LZMA2` type has changed (less overhead).
 * **DEPRECATED**: `freeze-to-stream!`, `thaw-from-stream!` are deprecated in favor of the more general `freeze-to-out!`, `thaw-from-in!`.
 * **DEPRECATED**: `:legacy-mode` options. This was being used mainly for headerless freezing, so a new headerless mode is taking its place.
 * Now distinguish between `BigInteger` and `BigInt` on thawing (previously both thawed to `BigInt`s). (mlacorte).
 * Moved most utils to external `encore` dependency.


## v2.5.2 / 2013 Dec 7

### New

 * Test Serializable objects at freeze time for better reliability.
 * Thaw error messages now include failing type-id.

### Changes

 * Don't cache `serializable?`/`readable?` for types with gensym-style names (e.g. as used for anonymous fns, etc.).
 * Failed serialized/reader thaws will try return what they can (e.g. unreadable string) instead of just throwing.


## v2.5.1 / 2013 Dec 3

### New

 * Added experimental `inspect-ba` fn for examining data possibly frozen by Nippy.

### Changes

 * Now throw exception at freeze (rather than thaw) time when trying to serialize an unreadable object using the Clojure reader.


## v2.4.1 → v2.5.0
  * Refactored standard Freezable protocol implementations to de-emphasise interfaces as a matter of hygiene, Ref. http://goo.gl/IFXzvh.
  * BETA STATUS: Added an additional (pre-Reader) Serializable fallback. This should greatly extend the number of out-the-box-serializable types.
  * ISeq is now used as a fallback for non-concrete seq types, giving better type matching pre/post freeze for things like LazySeqs, etc.
  * Experimental: add `Compressable-LZMA2` type & (replaceable) de/serializer.


## v2.3.0 → v2.4.1
  * Added (alpha) LZMA2 (high-ratio) compressor.
  * Bump tools.reader dependency to 0.7.9.


## v2.2.0 → v2.3.0
  * Huge (~30%) improvement to freeze time courtesy of Zach Tellman (ztellman).


## v2.1.0 → v2.2.0
  * Dropped `:read-eval?`, `:print-dup?` options.

  Thanks to James Reeves (weavejester) for these changes!:
  * Switched to `tools.reader.edn` for safer reader fallback.
  * Added fast binary serialization for Date and UUID types.
  * Added fast binary serialization for record types.


## v2.0.0 → v2.1.0
  * Exposed low-level fns: `freeze-to-stream!`, `thaw-from-stream!`.
  * Added `extend-freeze` and `extend-thaw` for extending to custom types:

  * Added support for easily extending Nippy de/serialization to custom types:
    ```clojure
    (defrecord MyType [data])
    (nippy/extend-freeze MyType 1 [x steam] (.writeUTF stream (:data x)))
    (nippy/extend-thaw 1 [stream] (->MyType (.readUTF stream)))
    (nippy/thaw (nippy/freeze (->MyType "Joe"))) => #taoensso.nippy.MyType{:data "Joe"}
    ```


## v1.2.1 → v2.0.0
  * **MIGRATION NOTE**: Please be sure to use `lein clean` to clear old (v1) build artifacts!
  * Refactored for huge performance improvements (~40% roundtrip time).
  * New header format for better error messages.
  * New `taoensso.nippy.tools` ns for easier integration with 3rd-party tools.

  * **DEPRECATED**: `freeze-to-bytes` -> `freeze`, `thaw-from-bytes` -> `thaw`.
    See the new fn docstrings for updated opts, etc.

  * Added pluggable compression support:
    ```clojure
    (freeze "Hello") ; defaults to:
    (freeze "Hello" {:compressor taoensso.nippy.compression/snappy-compressor})

    ;; The :compressor value above can be replaced with nil (no compressor) or
    ;; an alternative Compressor implementing the appropriate protocol
    ```

  * Added pluggable crypto support:
    ```clojure
    (freeze "Hello") ; defaults to:
    (freeze "Hello" {:encryptor taoensso.nippy.encryption/aes128-encryptor}

    ;; The :encryptor value above can be replaced with nil (no encryptor) or
    ;; an alternative Encryptor implementing the appropriate protocol
    ```

    See the [README](https://github.com/ptaoussanis/nippy#encryption-currently-in-alpha) for an example using encryption.

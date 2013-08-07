## v2.1.0 → v2.2.0-RC1
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


## For older versions please see the [commit history][]

[commit history]: https://github.com/ptaoussanis/nippy/commits/master
[API docs]: http://ptaoussanis.github.io/nippy
[Taoensso libs]: https://www.taoensso.com/clojure-libraries
[Nippy GitHub]: https://github.com/ptaoussanis/nippy
[Nippy CHANGELOG]: https://github.com/ptaoussanis/carmine/blob/master/CHANGELOG.md
[Nippy API docs]: http://ptaoussanis.github.io/nippy

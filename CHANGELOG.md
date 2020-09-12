> This project uses [Break Versioning](https://github.com/ptaoussanis/encore/blob/master/BREAK-VERSIONING.md) as of **Aug 16, 2014**.

## v3.0.0-RC3 / 2020 Sep 12

```clojure
[com.taoensso/nippy "3.0.0-RC3"]
```

#### New since `v3.0.0-RC1`

  - Serializable: add `allow-and-record-any-serializable-class-unsafe` util.


## v3.0.0-RC1 / 2020 Sep 11

```clojure
[com.taoensso/nippy "3.0.0-RC1"]
```

> Major **BREAKING** feature + security release.  
> See [here](https://github.com/ptaoussanis/encore#recommended-steps-after-any-significant-dependency-update) for recommended steps when updating any Clojure/Script dependencies.

> This release is focused on smoothing out rough edges left by `CVE-2020-24164` [#130], and to **ease transition** from version of Nippy < `v2.15.0` final.

> Apologies for the recent breaking changes!

Note that there's separate documentation below if upgrading from `v2.15` or `v2.14`:

### Since `v2.15.0` (incl. BREAKING changes)

See [#130] for **detailed upgrade instructions**.

#### Changes

  - **[BREAKING]** Bumped minimum Clojure version from `v1.5` to `v1.7`.
  - **[BREAKING]** `*serializable-whitelist*` has been split into two separate vars: `*freeze-serializable-allowlist*`, `*thaw-serializable-allowlist`*. See [#130] for details.
  - **[BREAKING]** `:nippy/unthawable` responses now have a standardized form: `{:nippy/unthawable {:type _ :cause _ ...}`. Most folks won't care about this change unless you have code specifically to deal with `:nippy/unthawable` responses.
  - By default, `freeze` now allows the use of Java's Serializable for any class. `thaw` continues to be restrictive by default, and will quarantine any objects not on the class allowlist. See [#130] for details.

#### New

  - [#122] Option to disable freezing and/or thawing of metadata.
  - `freeze` and `thaw` now support opts: `:serializable-allowlist`, `:incl-metadata?`.
  - New `read-quarantined-serializable-object-unsafe!` util to read quarantined Serializable objects. See [#130] for details.

### Since `v2.14.0` (incl. BREAKING changes)

See [#130] for **detailed upgrade instructions**.

#### Changes

  - **[BREAKING]** [#130] `thaw` will now quarantine Serializable objects whose class is not allowed by `*thaw-serializable-allowlist*`. See [#130] for details.
  - **[BREAKING]** Bumped minimum Clojure version from `v1.5` to `v1.7`.
  - **[BREAKING]** `:nippy/unthawable` responses now have a standardized form: `{:nippy/unthawable {:type _ :cause _ ...}`. Most folks won't care about this change unless you have code specifically to deal with `:nippy/unthawable` responses.
  - [#101] Switch default encryptor from `AES-CBC` to `AES-GCM` (faster, includes integrity check)

#### New

  - [#127] Add utils: `freeze-to-string`, `thaw-from-string` (@piotr-yuxuan)
  - [#113 #114] Add support for object arrays (@isaksky)
  - [#83 #112] Add support for deftype (@isaksky)
  - [#83 #113] Add support for URIs (@isaksky)
  - [#126] `extend-freeze`: include id collision odds in docstring

#### Fixes

  - [#120] Update `freezable?` to cover `nil`


## v2.15.3 / 2020 Sep 10

```clojure
[com.taoensso/nippy "2.15.3"]
```

> Non-breaking release. See [here](https://github.com/ptaoussanis/encore#recommended-steps-after-any-significant-dependency-update) for recommended steps when updating any Clojure/Script dependencies.

#### Changes since `v2.15.2`

* `*serializable-whitelist*`: added a number of common classes to default value. See `default-serializable-whitelist` for details.


## v2.15.2 / 2020 Aug 31

```clojure
[com.taoensso/nippy "2.15.2"]
```

> This is a quick hotfix release, should be non-breaking.

Only change since `v2.15.0` is to bump Encore dependency (`v2.124.0`->`v2.126.2`).

Encore `v1.123.0` introduced an issue affecting Timbre.
Issue was addressed with Encore `v2.125.2`.

Updating Encore here doesn't affect Nippy, but may be helpful for users of
Nippy that also use Timbre and that haven't otherwise updated to a newer
version of Encore yet.


## v2.15.1 / 2020 Aug 27

```clojure
[com.taoensso/nippy "2.15.1"]
```

> This is a minor feature release. Should be non-breaking.

**New** since `v2.15.0`:

  - `*serializable-whitelist*`: sets of approved class names may now contain "*" wildcards.
  - `*serializable-whitelist*`: added JVM properties and env vars to override initial value. See updated docstring for details.

**Changes** since `v2.15.0`:

  - [#126] `extend-freeze`: include id collision odds in docstring.


## v2.15.0 / 2020 Jul 24

```clojure
[com.taoensso/nippy "2.15.0"]
```

> This is a **major** feature release. It may be **BREAKING**!

**BREAKING CHANGES** since `v2.14.0`:

* [#130] Add `*serialization-whitelist*`, **ENABLED BY DEFAULT**.

See https://github.com/ptaoussanis/nippy/issues/130 for details, incl. upgrade instructions.

Big thanks to **Timo Mihaljov** (@solita-timo-mihaljov) for an excellent report identifying this vulnerability!

**New** since `v2.14.0`:

* [#127] Add utils: `freeze-to-string`, `thaw-from-string` (@piotr-yuxuan)
* [#113 #114] Add support for object arrays (@isaksky)
* [#83 #112] Add support for deftype (@isaksky)
* [#83 #113] Add support for URIs (@isaksky)

**Changes** since `v2.14.0`:

* [#101] Switch default encryptor from `AES-CBC` to `AES-GCM` (faster, includes integrity check)
* Refactor encryption utils for extra flexibility in future
* Latest dependencies

**Fixes** since `v2.14.0`:

* [#120] Update `freezable?` to cover `nil`


## v2.14.2 / 2020 Jul 24

```clojure
[com.taoensso/nippy "2.14.2"]
```

> This is a non-breaking **hotfix security release**. PLEASE READ CAREFULLY.

**New** since `v2.14.0`:

* [#130] Add `*serialization-whitelist*` feature, **DISABLED BY DEFAULT**.

### *** SECURITY ADVISORY ***

To prevent a possible **Remote Code Execution (RCE) vulnerability** (#130), you must **opt-in** to use the new `*serialization-whitelist*` feature.

See the `nippy/*serialization-whitelist*` docstring for usage instructions.

## v2.14.0 / 2017 Dec 21

```clojure
[com.taoensso/nippy "2.14.0"]
```

> This is a minor, non-breaking maintenance release.

* **Impl**: Update a number of dependencies
* [#104] **Impl**: Micro-optimization: remove unnecessary runtime 'if' in extend-freeze macro (@scramjet)
* [#93] **Impl**: Pickup record redefinitions at REPL, etc. (@smee)

## v2.13.0 / 2017 Feb 13

```clojure
[com.taoensso/nippy "2.13.0"]
```

> This is a minor, non-breaking maintenance release.

* [#91] **New**: Add `freeze-to-file` and `thaw-from-file` utils (@Engelberg)
* [#85] **Impl**: Lazily create LZ4 instance, fixes issue with Google App Engine
* **Impl**: Bump 1-byte cacheable types from 5->8

## v2.13.0-RC1 / 2016 Dec 17

```clojure
[com.taoensso/nippy "2.13.0-RC1"]
```

> This should be a minor, non-breaking release.

* [#85] **Impl**: Lazily create LZ4 instance, fixes issue with Google App Engine
* **Impl**: Bump 1-byte cacheable types from 5->8

## v2.12.2 / 2016 Aug 23

```clojure
[com.taoensso/nippy "2.12.2"]
```

* **Hotfix**: private API typo

## v2.12.1 / 2016 Jul 26

```clojure
[com.taoensso/nippy "2.12.1"]
```

* **Hotfix**: thaw was missing support for deprecated serializable, record types [@rwilson]

## v2.12.0 / 2016 Jul 24

```clojure
[com.taoensso/nippy "2.12.0"]
```

> This is a **major release** that **may** involve some **breaking API changes** in rare cases for users of some low-level or obscure features that have been made private or removed. If your code compiles with this new version of Nippy, you should be fine.

> As with all Nippy releases: this version can read data written by older versions but older versions may not be able to read data written by _this_ version.

> No changes since `2.12.0-RC2`
> Changes since `2.11.1`:

* **BREAKING**: dropped support for `*final-freeze-fallback*` (rarely used)
* **BREAKING**: dropped support for `*default-freeze-compressor-selector*` (rarely used)
* **BREAKING**: made several implementation details private, incl. most low-level `write-<x>` and `read-<x>` fns (rarely used)
* **Performance**: several significant speed + space efficiency improvements, including more variable-sized types
* **New built-in types** (these previously fell back to the reader): regex patterns, symbols
* **New experimental caching feature** (please see `cache` docstring for details)
* **New**: `fast-freeze`, `fast-thaw` utils (please see docstrings for details)
* **Change**: `freeze` return val is no longer auto type hinted as `^bytes` (had a performance cost, rarely used)
* **Hotfix**: `fn?`s were incorrectly reporting true for `serializable?`
* **Hotfix**: *final-freeze-fallback* back compatibility was broken

## v2.12.0-RC2 / 2016 Jul 17

```clojure
[com.taoensso/nippy "2.12.0-RC2"]
```

> Changes since `2.12.0-RC1`:

* **New**: Experimental `cache` feature now supports metadata
* **Impl**: Some additional minor performance improvements

## v2.12.0-RC1 / 2016 Jun 23

```clojure
[com.taoensso/nippy "2.12.0-RC1"]
```

> This is a **major release** that **may** involve some **breaking API changes** in rare cases for users of some low-level or obscure features that have been made private or removed. If your code compiles with this new version of Nippy, you should be fine.

> As with all Nippy releases: this version can read data written by older versions but older versions may not be able to read data written by _this_ version.

**PLEASE REPORT ANY PROBLEMS**, thank you!

- @ptaoussanis

> No changes since `2.12.0-beta3`.
> Changes since `2.11.1`:

* **BREAKING**: dropped support for `*final-freeze-fallback*` (rarely used)
* **BREAKING**: dropped support for `*default-freeze-compressor-selector*` (rarely used)
* **BREAKING**: made several implementation details private, incl. most low-level `write-<x>` and `read-<x>` fns (rarely used)
* **Performance**: several significant speed + space efficiency improvements, including more variable-sized types
* **New built-in types** (these previously fell back to the reader): regex patterns, symbols
* **New experimental caching feature** (please see `cache` docstring for details)
* **New**: `fast-freeze`, `fast-thaw` utils (please see docstrings for details)
* **Change**: `freeze` return val is no longer auto type hinted as `^bytes` (had a performance cost, rarely used)
* **Hotfix**: `fn?`s were incorrectly reporting true for `serializable?`
* **Hotfix**: *final-freeze-fallback* back compatibility was broken

## v2.12.0-beta3 / 2016 Jun 17

```clojure
[com.taoensso/nippy "2.12.0-beta3"]
```

> Changes since **2.12.0-beta2**:

* **Hotfix**: `fn?`s were incorrectly reporting true for `serializable?`
* **Hotfix**: *final-freeze-fallback* back compatibility was broken

## v2.12.0-beta2 / 2016 Jun 10

```clojure
[com.taoensso/nippy "2.12.0-beta2"]
```

> This is a **major release** that **may** involve some **breaking API changes** in rare cases for users of some low-level or obscure features that have been made private or removed. If your code compiles with this new version of Nippy, you should be fine.

> As with all Nippy releases: this version can read data written by older versions but older versions may not be able to read data written by _this_ version.

* **BREAKING**: dropped support for `*final-freeze-fallback*` (rarely used)
* **BREAKING**: dropped support for `*default-freeze-compressor-selector*` (rarely used)
* **BREAKING**: made several implementation details private, incl. most low-level `write-<x>` and `read-<x>` fns (rarely used)
* **Performance**: several significant speed + space efficiency improvements, including more variable-sized types
* **New built-in types** (these previously fell back to the reader): regex patterns, symbols
* **New experimental caching feature** (please see `cache` docstring for details)
* **New**: `fast-freeze`, `fast-thaw` utils (please see docstrings for details)
* **Change**: `freeze` return val is no longer auto type hinted as `^bytes` (had a performance cost, rarely used)

**PLEASE REPORT ANY PROBLEMS**, thank you!

## v2.11.1 / 2016 Feb 25

> **Hotfix** for broken Clojure 1.5 support

```clojure
[com.taoensso/nippy "2.11.1"]
```

## v2.11.0 / 2016 Feb 25

> Identical to v2.11.0-beta1 (published December 13 2015)

```clojure
[com.taoensso/nippy "2.11.0"]
```

## v2.11.0-RC1 / 2016 Jan 23

> Identical to v2.11.0-beta1 (published December 13 2015)

```clojure
[com.taoensso/nippy "2.11.0-RC1"]
```

## v2.11.0-beta1 / 2015 Dec 13

> This is a major performance release that **drops default support for thawing Nippy v1 archives** but is otherwise non-breaking

* **BREAKING**: `thaw` now has `:v1-compatibility?` opt set to false by default (was true before) [1]
* **Performance**: optimize serialized size of small maps, sets, vectors, bytes
* **Performance**: optimized (no copy) `freeze` when using no compression or encryption
* **Implementation**: swap most macros for fns (make low-level utils easier to use)

```clojure
[com.taoensso/nippy "2.11.0-beta1"]
```

#### Notes

**[1]** Use `(thaw <frozen-byte-array> {:v1-compatibility? true})` to support thawing of data frozen with Nippy v1 (before ~June 2013)


## v2.10.0 / 2015 Sep 30

> This is a major feature/performance release that **drops support for Clojure 1.4** but is otherwise non-breaking

* **BREAKING**: drop support for Clojure 1.4 (**now requires Clojure 1.5+**)
* **Performance**: various small performance improvements
* **New**: dynamic `*default-freeze-compressor-selector*`, `set-default-freeze-compressor-selector!` util
* **New**: dynamic `*custom-readers*`, `swap-custom-readers!` util
* **New**: edn writes now override dynamic `*print-level*`, `*print-length*` for safety

```clojure
[com.taoensso/nippy "2.10.0"]
```


## v2.9.1 / 2015 Sep 14

> This is a hotfix release with an **important fix** for Nippy encryption users

* **Fix**: broken encryption thread-safety [#68]

```clojure
[com.taoensso/nippy "2.9.1"]
```


## v2.9.0 / 2015 Jun 1

> This is a major **non-breaking** release that improves performance and makes thawing more resilient to certain failures. Identical to **v2.9.0-RC3**.

* **Robustness**: improve error handling for unthawable records
* **Performance**: switch `doseq` -> (faster) `run!` calls
* **Performance**: eliminate some unnecessary boxed math
* **New**: allow intelligent auto-selection of `freeze` compression scheme using `:auto` compressor (now the default). This can result in significant speed+space improvements for users serializing many small values.

```clojure
[com.taoensso/nippy "2.9.0"]
```


## v2.8.0 / 2015 Feb 18

> This is a **maintenance release** with some minor fixes and some dependency updates.

 * **CHANGE**: Throw a clear error message on insufficient Encore dependency.
 * **FIX** [#59]: `freezable?` should return true for clojure.lang.PersistentVector (@chairmanwow).
 * **FIX** [#63]: Missing thaw exception cause (@cespare).

## v2.7.1 / 2014 Nov 27

> This is a **minor maintenance release** & should be a safe upgrade for users of v2.7.0/RC-1.

 * **CHANGE**: Improve some error messages by exposing trapped throwables when possible (@kul).
 * **FIX**: Nippy v1 thaw compatibility was broken in some cases.
 * Bumped dependencies.


## v2.7.0 / 2014 Oct 6

No changes from `v2.7.0-RC1`.


## v2.7.0-RC1 / 2014 Aug 27

> **Major release** with significant performance improvements, a new default compression type ([LZ4](http://blog.jpountz.net/post/28092106032/wow-lz4-is-fast)), and better support for a variety of compression/encryption tools.
>
> The data format is fully **backwards-compatible**, the API is backwards compatible **unless** you are using the `:headerless-meta` thaw option.

### Changes

 * A number of internal performance improvements.
 * Added [LZ4](http://blog.jpountz.net/post/28092106032/wow-lz4-is-fast) compressor, **replacing Snappy as the default** (often ~10+% faster with similar compression ratios). **Thanks to [mpenet](https://github.com/mpenet) for his work on this**!
 * **BREAKING**: the `thaw` `:headerless-meta` option has been dropped. Its purpose was to provide Nippy v1 compatibility, which is now done automatically. To prevent any surprises, `thaw` calls with this option will now **throw an assertion error**.
 * **IMPORTANT**: the `thaw` API has been improved (simplified). The default `:encryptor` and `:compressor` values are now both `:auto`, which'll choose intelligently based on data now included with the Nippy header. Behaviour remains the same for data written without a header: you must specify the correct `:compressor` and `:encryptor` values manually.
 * Promoted from Alpha status: `taoensso.nippy.compression` ns, `taoensso.nippy.encryption` ns, `taoensso.nippy.tools` ns, `extend-freeze`, `extend-thaw`.
 * All Nippy exceptions are now `ex-info`s.
 * `extend-thaw` now prints a warning when replacing a pre-existing type id.

### NEW

 * #50: `extend-freeze`, `extend-thaw` can now take arbitrary keyword type ids (see docstrings for more info).


## v2.6.3 / 2014 Apr 29

 * Fix #48: broken freeze/thaw identity for empty lazy seqs (@vgeshel).


## v2.6.2 / 2014 Apr 10

 * Fix #46: broken support for Clojure <1.5.0 (@kul).


## v2.6.1 / 2014 Apr 8

**CRITICAL FIX** for v2.6.0 released 9 days ago. **Please upgrade ASAP!**

### Problem

Small strings weren't getting a proper UTF-8 encoding:
`(.getBytes <string>)` was being used here instead of
`(.getBytes <string> "UTF-8")` as is correct and done elsewhere.

This means that small UTF-8 _strings may have been incorrectly stored_
in environments where UTF-8 is not the default JVM character encoding.

Bug was introduced in Nippy v2.6.0, released 9 days ago (2014 Mar 30).

*********************************************************************
Please check for possible errors in Unicode text written using Nippy
v2.6.0 if your JVM uses an alternative character encoding by default
*********************************************************************

Really sorry about this! Thanks to @xkihzew for the bug report.


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

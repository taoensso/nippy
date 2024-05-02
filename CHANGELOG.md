This project uses [**Break Versioning**](https://www.taoensso.com/break-versioning).

---

# `v3.4.0` (2024-04-30)

> **Dep**: Nippy is [on Clojars](https://clojars.org/com.taoensso/nippy/versions/3.4.0).  
> **Versioning**: Nippy uses [Break Versioning](https://www.taoensso.com/break-versioning).

This is a non-breaking **feature and maintenance** release and should be a safe update for existing users. But as always, please **test carefully and report any unexpected problems**, thank you! 🙏

**IMPORTANT**: data **frozen by Nippy version X** should always be **thawed by version >= X**, otherwise you run the risk of the thaw throwing when encountering unfamiliar types. Please note that this can affect **rolling updates**, and can limit your ability to **revert a Nippy update**. Please ensure adequate testing in your environment before updating against production data.

\- [Peter Taoussanis](https://www.taoensso.com)

## Changes since `v3.3.0` (2023-10-11)

* 82a050b [mod] Don't attach empty metadata (meta will now be `nil` rather than `{}`)

## Fixes since `v3.3.0` (2023-10-11)

* 92c4a83 [fix] Broken `*final-freeze-fallback*` default val

## New since `v3.3.0` (2023-10-11)

* fb6f75e [new] Smarter, faster, protocol-based `freezable?` util
* 6ad5aeb [new] Add `:zstd` compressor, new compressor backend
* 9db09e1 [new] [#163] Track serialized output in tests
* dcc6b08 [new] [#164] Update benchmarks
* f3ff7ae [new] Add native `MapEntry` freezer
* 37cf415 [new] [#171] Auto strip metadata protocol extensions
* Misc internal improvements

## Everything since `v3.4.0-RC3` (2024-04-10)

* Update dependencies

---

# `v3.4.0-RC3` (2024-04-10)

> 📦 [Available on Clojars](https://clojars.org/com.taoensso/nippy/versions/3.4.0-RC3), this project uses [Break Versioning](https://www.taoensso.com/break-versioning).

This is a non-breaking **feature and maintenance** pre-release.  
Please **test carefully and report any unexpected problems**, thank you! 🙏

## New since `v3.3.0`

* fb6f75e [new] Smarter, faster, protocol-based `freezable?` util
* 6ad5aeb [new] Add `:zstd` compressor, new compressor backend
* 9db09e1 [new] [#163] Track serialized output in tests
* dcc6b08 [new] [#164] Update benchmarks
* f3ff7ae [new] Add native `MapEntry` freezer
* 37cf415 [new] [#171] Auto strip metadata protocol extensions
* Misc internal improvements

## Everything since `v3.4.0-RC2`

* 82a050b [mod] Don't attach empty metadata
* 92c4a83 [fix] Broken `*final-freeze-fallback*` default val
* 37cf415 [new] [#171] Auto strip metadata protocol extensions
* Update dependencies


# `v3.4.0-RC2` (2024-02-26)

> 📦 [Available on Clojars](https://clojars.org/com.taoensso/nippy/versions/3.4.0-RC2), this project uses [Break Versioning](https://www.taoensso.com/break-versioning).

This is a non-breaking **feature and maintenance** pre-release.  
Please **test carefully and report any unexpected problems**, thank you! 🙏

## New since `v3.3.0`

* fb6f75e [new] Smarter, faster, protocol-based `freezable?` util
* 6ad5aeb [new] Add `:zstd` compressor, new compressor backend
* 9db09e1 [new] [#163] Track serialized output in tests
* dcc6b08 [new] [#164] Update benchmarks
* f3ff7ae [new] Add native `MapEntry` freezer
* Misc internal improvements

## Everything since `v3.4.0-RC1`

* cb5b7cf [fix] [#169] Can't auto-identify `:zstd` compressor when decompressing
* cb0b871 Revert [mod] 578c585 (upstream fix now available)
* Update dependencies

---

# `v3.4.0-RC1` (2024-02-06)

> 📦 [Available on Clojars](https://clojars.org/com.taoensso/nippy/versions/3.4.0-RC1), this project uses [Break Versioning](https://www.taoensso.com/break-versioning).

This is a non-breaking **feature and maintenance** pre-release.  
Please **test carefully and report any unexpected problems**, thank you! 🙏

## Changes since `v3.3.0`

* 578c585 [mod] Remove `nippy/snappy-compressor`

## New since `v3.3.0`

* fb6f75e [new] Smarter, faster, protocol-based `freezable?` util
* 6ad5aeb [new] Add `:zstd` compressor, new compressor backend
* 9db09e1 [new] [#163] Track serialized output in tests
* dcc6b08 [new] [#164] Update benchmarks
* f3ff7ae [new] Add native `MapEntry` freezer
* Misc internal improvements

---

# `v3.3.0` (2023-10-11)

> 📦 [Available on Clojars](https://clojars.org/com.taoensso/nippy/versions/3.3.3), this project uses [Break Versioning](https://www.taoensso.com/break-versioning).

Identical to `v3.3.0-RC2`.

This is a non-breaking **feature and maintenance** release.  
Please test carefully and report any unexpected problems, thank you! 🙏

## Changes since `v3.2.0`

* [mod] Due to micro-optimizations of some elementary types, Nippy v3.3 may produce **different serialized output** to earlier versions of Nippy. Most users won't care about this, but you could be affected if you depend on specific serialized byte values (for example by comparing serialized output between different versions of Nippy).

## Fixes since `v3.2.0`

* fa1cc66 [fix] [#143] Don't freeze meta info for types that don't support `with-meta`

## New since `v3.2.0`

* 89f98b4 [new] [#153] PoC: transducer support on thaw
* 60bc4e9 [new] [Storage efficiency] PoC: unsigned counts for small core colls
* 0a9d670 [new] [Storage efficiency] PoC: separate signed long types
* 8778fa4 [new] Include `:bindings` in ex-data of thaw failures
* aba153e [new] [#159] Add native impln for `java.sql.Date` (@philomates)
* d8b1825 [new] [#158] Add `java.lang.ClassCastException` to default thaw allow list (@carlosgeos)
* 8b7186a [new] Update [benchmark results](https://github.com/taoensso/nippy#performance)
* 3ac06b6 [new] Refactor tools ns, embed dynamic `*freeze-opts*` in wrappers
* 129ce95 [new] [#151] [#140] Add experimental `public-types-spec`

## Other improvements since `v3.2.0`

* Improved some docstrings
* Improved generative unit tests
* Updated internal dependencies

---

# `v3.4.0-beta1` (2023-09-26)

> 📦 [Available on Clojars](https://clojars.org/com.taoensso/nippy/versions/3.4.0-beta1)

This is a non-breaking **feature** pre-release.  
Please **test carefully and report any unexpected problems**, thank you! 🙏

## New since `v3.3.0-RC2`

* 6ad5aeb [new] Add `:zstd` compressor, new (faster) compressor backend, better docstrings
* fb6f75e [new] Smarter, faster, protocol-based `freezable?` util
* f3ff7ae [new] Add native `MapEntry` freezer
* fef079d [new] Add subvec to stress data
* Misc internal improvements

## Other improvements since `v3.3.0-RC2`

* e0cd003 [nop] Update docs
* 99970d5 [nop] Update benchmark results
* bcf7673 [nop] Move benchmarks ns under tests dir

---

# `v3.3.0-RC2` (2023-09-25)

> 📦 [Available on Clojars](https://clojars.org/com.taoensso/nippy/versions/3.3.0-RC2)

Identical to `v3.3.0-RC1` except:

* Improves some docstrings
* Improves generative unit tests
* Updates internal dependencies

If no unexpected problems come up, `v3.3.0` final is planned for release by the end of September.

---

# `v3.3.0-RC1` (2023-08-02)

> 📦 [Available on Clojars](https://clojars.org/com.taoensso/nippy/versions/3.3.0-RC1)

This is a non-breaking **feature and maintenance** pre-release.  
Please **test carefully and report any unexpected problems**, thank you! 🙏

## Fixes since `v3.2.0`

* fa1cc66 [fix] [#143] Don't freeze meta info for types that don't support `with-meta`

## New since `v3.2.0`

* 89f98b4 [new] [#153] PoC: transducer support on thaw
* 60bc4e9 [new] [Storage efficiency] PoC: unsigned counts for small core colls
* 0a9d670 [new] [Storage efficiency] PoC: separate signed long types
* 8778fa4 [new] Include `:bindings` in ex-data of thaw failures
* aba153e [new] [#159] Add native impln for `java.sql.Date` (@philomates)
* d8b1825 [new] [#158] Add `java.lang.ClassCastException` to default thaw allow list (@carlosgeos)
* 8b7186a [new] Update [benchmark results](https://github.com/taoensso/nippy#performance)
* 3ac06b6 [new] Refactor tools ns, embed dynamic `*freeze-opts*` in wrappers
* 129ce95 [new] [#151] [#140] Add experimental `public-types-spec`

---

# `v3.2.0` (2022-07-18)

> Identical to `v3.2.0-RC3` (2022 Jun 27)

```clojure
[com.taoensso/nippy "3.2.0"]
```

> This is a non-breaking maintenance release.  
> See [here](https://github.com/taoensso/encore#recommended-steps-after-any-significant-dependency-update) for recommended steps when updating any Clojure/Script dependencies.

## New since `v3.1.3`

* [#144] [New] Add `org.joda.time.DateTime` to `default-thaw-serializable-allowlist` (@slipset)
* [#146] [New] Add Graal native configurations (@FieryCod)

## Changes since `v3.1.3`

* Bump dependencies, incl. minimum Encore version

## Fixes since `v3.1.1`

* [#89 #150] [Fix] Boxed Booleans incorrectly freezing to primitive `true` (@RolT)
* [#148] [Fix] `tools/freeze` should use `*freeze-opts*` even for unwrapped vals
* [#145] [Fix] Freezing custom types with munged field names

The boxed Boolean bug has been around since the first version of Nippy and is mostly
relevant to users doing Java interop. For more info see: https://github.com/taoensso/nippy/commit/8909a32bdd654a136da385e0e09c9cc44416f964

---

# `v3.2.0-RC3` (2022-06-27)

```clojure
[com.taoensso/nippy "3.2.0-RC3"]
```

> This is a non-breaking maintenance release.  
> See [here](https://github.com/taoensso/encore#recommended-steps-after-any-significant-dependency-update) for recommended steps when updating any Clojure/Script dependencies.

## New since `v3.1.3`

* [#144] [New] Add `org.joda.time.DateTime` to `default-thaw-serializable-allowlist` (@slipset)
* [#146] [New] Add Graal native configurations (@FieryCod)

## Changes since `v3.1.3`

* Bump dependencies, incl. minimum Encore version

## Fixes since `v3.1.1`

* [#89 #150] [Fix] Boxed Booleans incorrectly freezing to primitive `true` (@RolT)
* [#148] [Fix] `tools/freeze` should use `*freeze-opts*` even for unwrapped vals
* [#145] [Fix] Freezing custom types with munged field names

The boxed Boolean bug has been around since the first version of Nippy and is mostly
relevant to users doing Java interop. For more info see: https://github.com/taoensso/nippy/commit/8909a32bdd654a136da385e0e09c9cc44416f964

---

# `v3.2.0-RC2` (2022-06-23)

```clojure
[com.taoensso/nippy "3.2.0-RC2"]
```

> This is a non-breaking maintenance release.  
> See [here](https://github.com/taoensso/encore#recommended-steps-after-any-significant-dependency-update) for recommended steps when updating any Clojure/Script dependencies.

## New since `v3.1.3`

* [#144] [New] Add `org.joda.time.DateTime` to `default-thaw-serializable-allowlist` (@slipset)
* [#146] [New] Add Graal native configurations (@FieryCod)

## Changes since `v3.1.3`

* Bump dependencies, incl. minimum Encore version

## Fixes since `v3.1.1`

* [#148] [Fix] `tools/freeze` should use `*freeze-opts*` even for unwrapped vals
* [#89 #150] [Fix] Boxed Booleans incorrectly freezing to primitive `true` (@RolT)

The boxed Boolean bug has been around since the first version of Nippy and is mostly
relevant to users doing Java interop. For more info see: https://github.com/taoensso/nippy/commit/8909a32bdd654a136da385e0e09c9cc44416f964

---

# `v3.1.3` (2022-06-23)

```clojure
[com.taoensso/nippy "3.1.3"]
```

> This is a non-breaking, bugfix release.  
> See [here](https://github.com/taoensso/encore#recommended-steps-after-any-significant-dependency-update) for recommended steps when updating any Clojure/Script dependencies.

## Fixes since `v3.1.1`

* [#148] [Fix] `tools/freeze` should use `*freeze-opts*` even for unwrapped vals
* [#89 #150] [Fix] Boxed Booleans incorrectly freezing to primitive `true` (@RolT)

The boxed Boolean bug has been around since the first version of Nippy and is mostly
relevant to users doing Java interop. For more info see: https://github.com/taoensso/nippy/commit/8909a32bdd654a136da385e0e09c9cc44416f964

---

# `v3.1.1` (2020-11-18)

```clojure
[com.taoensso/nippy "3.1.1"]
```

> This is a non-breaking, bugfix release. But please note that large keywords or symbols (with >127 characters) frozen with >=`v3.1.1` will need >=`v3.1.1` to thaw.

## Fixes since `v3.1.0`

* Large keywords and symbols (with >127 characters) can now thaw without throwing (@danmason).


[1] Keywords or symbols with >127 characters in their name

---

# `v3.1.0` (2020-11-06)

```clojure
[com.taoensso/nippy "3.1.0"]
```

> This is a non-breaking, minor feature release.

## New since `v3.0.0`

* [#135 #128] Added native `freeze/thaw` support for `java.time` classes on JVM 8+: `Instant`, `Duration`, `Period`.
* [#137] Add `thaw-from-resource` convenience util.
* Add (DEPRECATED) `swap-serializable-whitelist!` for backwards compatibility.

## Changes since `v3.0.0`

* Add several standard `java.time` classes to default `*thaw-serializable-whitelist*`.

---

# `v3.1.0-RC1` (2020-10-24)

```clojure
[com.taoensso/nippy "3.1.0-RC1"]
```

> This is a non-breaking, minor feature release.

## New since `v3.0.0`

* [#135 #128] Added native `freeze/thaw` support for `java.time.Instant` on JVM 8+ (@cnuernber).

---

# `v3.0.0` (2020-09-20)

```clojure
[com.taoensso/nippy "3.0.0"]
```

> This release is focused on smoothing out rough edges left by `CVE-2020-24164` [#130], and to **ease transition** from versions of Nippy < `v2.15.0 final`.

> See [here](https://github.com/taoensso/encore#recommended-steps-after-any-significant-dependency-update) for recommended steps when updating any Clojure/Script dependencies.

Note that there's **separate details** below for upgrading from `v2.15` vs `v2.14`:

## Upgrading from `v2.15` (usually non-breaking)

Usually a non-breaking drop-in replacement, but there's some changes you might like to take advantage of. See [#130] for **detailed upgrade instructions**.

### Changes

  - **[BREAKING]** Bumped minimum Clojure version from `v1.5` to `v1.7`.
  - **[BREAKING]** `:nippy/unthawable` responses now have a standardized form: `{:nippy/unthawable {:type _ :cause _ ...}`. Most folks won't care about this change unless they have code specifically to deal with `:nippy/unthawable` responses.
  - [Deprecated] `*serializable-whitelist*` has been split into two separate vars: `*freeze-serializable-allowlist*`, `*thaw-serializable-allowlist`*. See [#130] for details.
  - By default, `freeze` now **allows** the use of Java's Serializable for **any** class. `thaw` continues to be restrictive by default, and will quarantine any objects not on the class allowlist. See [#130] for details.

### New

  - [#122] Option to disable freezing and/or thawing of metadata.
  - `freeze` and `thaw` now support opts: `:serializable-allowlist`, `:incl-metadata?`.
  - New `read-quarantined-serializable-object-unsafe!` util to read quarantined Serializable objects. See [API docs](http://taoensso.github.io/nippy/taoensso.nippy.html#var-read-quarantined-serializable-object-unsafe.21) and/or [#130] for details.
  - Add `allow-and-record-any-serializable-class-unsafe` util. See [API docs](http://taoensso.github.io/nippy/taoensso.nippy.html#var-allow-and-record-any-serializable-class-unsafe) and/or [#130] for details.


## Upgrading from `v2.14` (may be BREAKING)

Likely breaking. Please see [#130] for **detailed upgrade instructions**.

### Changes

  - **[BREAKING]** Bumped minimum Clojure version from `v1.5` to `v1.7`.
  - **[BREAKING]** [#130] `thaw` will now quarantine Serializable objects whose class is not allowed by `*thaw-serializable-allowlist*`. See [#130] for details.
  - **[BREAKING]** `:nippy/unthawable` responses now have a standardized form: `{:nippy/unthawable {:type _ :cause _ ...}`. Most folks won't care about this change unless you have code specifically to deal with `:nippy/unthawable` responses.
  - [#101] Switch default encryptor from `AES-CBC` to `AES-GCM` (faster, includes integrity check)

### New

  - [#127] Add utils: `freeze-to-string`, `thaw-from-string` (@piotr-yuxuan)
  - [#113 #114] Add support for object arrays (@isaksky)
  - [#83 #112] Add support for deftype (@isaksky)
  - [#83 #113] Add support for URIs (@isaksky)
  - [#126] `extend-freeze`: include id collision odds in docstring

### Fixes

  - [#120] Update `freezable?` to cover `nil`

---

# Earlier releases

See [here](https://github.com/taoensso/nippy/releases) for earlier releases.

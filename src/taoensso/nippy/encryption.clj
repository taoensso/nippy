(ns taoensso.nippy.encryption
  "Simple no-nonsense crypto with reasonable defaults. Because your Clojure data
  deserves some privacy."
  {:author "Peter Taoussanis"}
  (:require [taoensso.encore :as encore]))

;;;; Interface

(def standard-header-ids "These'll support :auto thaw." #{:aes128-sha512})

(defprotocol IEncryptor
  (header-id      [encryptor])
  (encrypt ^bytes [encryptor pwd ba])
  (decrypt ^bytes [encryptor pwd ba]))

;;;; Default digests, ciphers, etc.

(def ^:private ^javax.crypto.Cipher aes128-cipher
  (javax.crypto.Cipher/getInstance "AES/CBC/PKCS5Padding"))
(def ^:private ^java.security.MessageDigest sha512-md
  (java.security.MessageDigest/getInstance "SHA-512"))
(def ^:private ^java.security.SecureRandom prng
  (java.security.SecureRandom/getInstance "SHA1PRNG"))

(def ^:private ^:const aes128-block-size (.getBlockSize aes128-cipher))
(def ^:private ^:const salt-size         aes128-block-size)

(defn- rand-bytes [size] (let [seed (byte-array size)] (.nextBytes prng seed) seed))

;;;; Default key-gen

(defn- sha512-key
  "SHA512-based key generator. Good JVM availability without extra dependencies
  (PBKDF2, bcrypt, scrypt, etc.). Decent security with multiple rounds."
  [salt-ba ^String pwd]
  (loop [^bytes ba (let [pwd-ba (.getBytes pwd "UTF-8")]
                     (if salt-ba (encore/ba-concat salt-ba pwd-ba) pwd-ba))
         n (* (int Short/MAX_VALUE) (if salt-ba 5 64))]
    (if-not (zero? n)
      (recur (.digest sha512-md ba) (dec n))
      (-> ba (java.util.Arrays/copyOf aes128-block-size)
             (javax.crypto.spec.SecretKeySpec. "AES")))))

(comment
  (time (sha512-key nil "hi" 1))   ; ~40ms per hash (fast)
  (time (sha512-key nil "hi" 5))   ; ~180ms  (default)
  (time (sha512-key nil "hi" 32))  ; ~1200ms (conservative)
  (time (sha512-key nil "hi" 128)) ; ~4500ms (paranoid)
  )

;;;; Default implementations

(defn- destructure-typed-pwd [typed-password]
  (let [throw-ex
        (fn [] (throw (ex-info
                      (str "Expected password form: "
                        "[<#{:salted :cached}> <password-string>].\n "
                        "See `default-aes128-encryptor` docstring for details!")
                      {:typed-password typed-password})))]
    (if-not (vector? typed-password) (throw-ex)
      (let [[type password] typed-password]
        (if-not (#{:salted :cached} type) (throw-ex)
          [type password])))))

(comment (destructure-typed-pwd [:salted "foo"]))

(defrecord AES128Encryptor [key-gen key-cache]
  IEncryptor
  (header-id [_] (if (= key-gen sha512-key) :aes128-sha512 :aes128-other))
  (encrypt   [_ typed-pwd data-ba]
    (let [[type pwd] (destructure-typed-pwd typed-pwd)
          salt?      (identical? type :salted)
          iv-ba      (rand-bytes aes128-block-size)
          salt-ba    (when salt? (rand-bytes salt-size))
          prefix-ba  (if-not salt? iv-ba (encore/ba-concat iv-ba salt-ba))
          key        (encore/memoized (when-not salt? key-cache)
                       key-gen salt-ba pwd)
          iv         (javax.crypto.spec.IvParameterSpec. iv-ba)]
      (.init aes128-cipher javax.crypto.Cipher/ENCRYPT_MODE
             ^javax.crypto.spec.SecretKeySpec key iv)
      (encore/ba-concat prefix-ba (.doFinal aes128-cipher data-ba))))

  (decrypt [_ typed-pwd ba]
    (let [[type pwd] (destructure-typed-pwd typed-pwd)
          salt?      (= type :salted)
          prefix-size (+ aes128-block-size (if salt? salt-size 0))
          [prefix-ba data-ba] (encore/ba-split ba prefix-size)
          [iv-ba salt-ba]     (if-not salt? [prefix-ba nil]
                                (encore/ba-split prefix-ba aes128-block-size))
          key (encore/memoized (when-not salt? key-cache)
                key-gen salt-ba pwd)
          iv  (javax.crypto.spec.IvParameterSpec. iv-ba)]
      (.init aes128-cipher javax.crypto.Cipher/DECRYPT_MODE
             ^javax.crypto.spec.SecretKeySpec key iv)
      (.doFinal aes128-cipher data-ba))))

(def aes128-encryptor
  "Default 128bit AES encryptor with multi-round SHA-512 key-gen.

  Password form [:salted \"my-password\"]
  ---------------------------------------
  USE CASE: You want more than a small, finite number of passwords (e.g. each
             item encrypted will use a unique user-provided password).

  IMPLEMENTATION: Uses a relatively cheap key hash, but automatically salts
                  every key.

  PROS: Each key is independent so would need to be attacked independently.
  CONS: Key caching impossible, so there's an inherent trade-off between
        encryption/decryption speed and the difficulty of attacking any
        particular key.

  Slower than `aes128-cached`, and easier to attack any particular key - but
  keys are independent.

  Password form [:cached \"my-password\"]
  ---------------------------------------
  USE CASE: You want only a small, finite number of passwords (e.g. a limited
            number of staff/admins, or you'll be using a single password to
            encrypt many items).

  IMPLEMENTATION: Uses a _very_ expensive (but cached) key hash, and no salt.

  PROS: Great amortized encryption/decryption speed. Expensive key hash makes
        attacking any particular key very difficult.
  CONS: Using a small number of keys for many encrypted items means that if any
        key _is_ somehow compromised, _all_ items encrypted with that key are
        compromised.

  Faster than `aes128-salted`, and harder to attack any particular key - but
  increased danger if a key is somehow compromised."
  (->AES128Encryptor sha512-key (atom {})))

;;;; Default implementation

(comment
  (def dae aes128-encryptor)
  (def secret-ba (.getBytes "Secret message" "UTF-8"))
  (encrypt dae "p" secret-ba) ; Malformed
  (time (encrypt dae [:salted "p"] secret-ba))
  (time (encrypt dae [:cached "p"] secret-ba))
  (time (->> secret-ba
             (encrypt dae [:salted "p"])
             (encrypt dae [:cached "p"])
             (decrypt dae [:cached "p"])
             (decrypt dae [:salted "p"])
             (String.))))

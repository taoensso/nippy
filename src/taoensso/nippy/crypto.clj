(ns taoensso.nippy.crypto
  "Alpha - subject to change.
  Simple no-nonsense crypto with reasonable defaults. Because your Clojure data
  deserves some privacy."
  {:author "Peter Taoussanis"}
  (:require [clojure.string       :as str]
            [taoensso.nippy.utils :as utils]))

;;;; Interface

(defprotocol IEncrypter
  (gen-key ^javax.crypto.spec.SecretKeySpec [encrypter salt-ba pwd])
  (encrypt ^bytes [encrypter pwd ba])
  (decrypt ^bytes [encrypter pwd ba]))

(defrecord AES128Encrypter [key-work-factor key-cache])

;;;; Digests, ciphers, etc.

;; 128bit keys have good JVM availability and are
;; entirely sufficient, Ref. http://goo.gl/2YRQG
(def ^:private ^:const aes128-block-size (int 16))
(def ^:private ^:const salt-size         (int 16))

(def ^:private ^javax.crypto.Cipher aes128-cipher
  (javax.crypto.Cipher/getInstance "AES/CBC/PKCS5Padding"))
(def ^:private ^java.security.MessageDigest sha512-md
  (java.security.MessageDigest/getInstance "SHA-512"))
(def ^:private ^java.security.SecureRandom prng
  (java.security.SecureRandom/getInstance "SHA1PRNG"))

(defn- rand-bytes [size] (let [seed (byte-array size)] (.nextBytes prng seed) seed))

;;;; Default keygen

(defn- sha512-key
  "SHA512-based key generator. Good JVM availability without extra dependencies
  (PBKDF2, bcrypt, scrypt, etc.). Decent security with multiple rounds."
  [salt-ba ^String pwd key-work-factor]
  (loop [^bytes ba (let [pwd-ba (.getBytes pwd "UTF-8")]
                     (if salt-ba (utils/ba-concat salt-ba pwd-ba) pwd-ba))
         n (* (int Short/MAX_VALUE) key-work-factor)]
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

;;;; Default implementation

(extend-type AES128Encrypter
  IEncrypter
  (gen-key [{:keys [key-work-factor key-cache]} salt-ba pwd]
    ;; Trade-off: salt-ba and key-cache mutually exclusive
    (utils/memoized key-cache sha512-key salt-ba pwd key-work-factor))

  (encrypt [{:keys [key-cache] :as this} pwd data-ba]
    (let [salt?     (not key-cache)
          iv-ba     (rand-bytes aes128-block-size)
          salt-ba   (when salt? (rand-bytes salt-size))
          prefix-ba (if-not salt? iv-ba (utils/ba-concat iv-ba salt-ba))
          key       (gen-key this salt-ba pwd)
          iv        (javax.crypto.spec.IvParameterSpec. iv-ba)]
      (.init aes128-cipher javax.crypto.Cipher/ENCRYPT_MODE key iv)
      (utils/ba-concat prefix-ba (.doFinal aes128-cipher data-ba))))

  (decrypt [{:keys [key-cache] :as this} pwd ba]
    (let [salt?       (not key-cache)
          prefix-size (+ aes128-block-size (if salt? salt-size 0))
          [prefix-ba data-ba] (utils/ba-split ba prefix-size)
          [iv-ba salt-ba]     (if-not salt? [prefix-ba nil]
                                (utils/ba-split prefix-ba aes128-block-size))
          key (gen-key this salt-ba pwd)
          iv  (javax.crypto.spec.IvParameterSpec. iv-ba)]
      (.init    aes128-cipher javax.crypto.Cipher/DECRYPT_MODE key iv)
      (.doFinal aes128-cipher data-ba))))

(def aes128-salted
  "USE CASE: You want more than a small, finite number of passwords (e.g. each
             item encrypted will use a unique user-provided password).

  IMPLEMENTATION: Uses a relatively cheap key hash, but automatically salts
                  every key.

  PROS: Each key is independent so would need to be attacked independently.
  CONS: Key caching impossible, so there's an inherent trade-off between
        encryption/decryption speed and the difficulty of attacking any
        particular key.

  Slower than `aes128-cached`, and easier to attack any particular key - but
  keys are independent."
  (AES128Encrypter. 5 nil))

(def aes128-cached
  "USE CASE: You want only a small, finite number of passwords (e.g. a limited
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
  (AES128Encrypter. 64 (atom {})))

(defn- destructure-typed-password
  "[<type> <password>] -> [Encrypter <password>]"
  [typed-password]
  (letfn [(throw-ex []
            (throw (Exception.
              (str "Expected password form: "
                   "[<#{:salted :cached}> <password-string>].\n "
                   "See `aes128-salted`, `aes128-cached` for details."))))]
    (if-not (vector? typed-password)
      (throw-ex)
      (let [[type password] typed-password]
        [(case type :salted aes128-salted :cached aes128-cached (throw-ex))
         password]))))

(defn encrypt-aes128 [typed-password ba]
  (let [[encrypter password] (destructure-typed-password typed-password)]
    (encrypt encrypter password ba)))

(defn decrypt-aes128 [typed-password ba]
  (let [[encrypter password] (destructure-typed-password typed-password)]
    (decrypt encrypter password ba)))

(comment
  (encrypt-aes128 "my-password" (.getBytes "Secret message")) ; Malformed
  (time (gen-key aes128-salted nil "my-password"))
  (time (gen-key aes128-cached nil "my-password"))
  (time (->> (.getBytes "Secret message" "UTF-8")
             (encrypt-aes128 [:salted "p"])
             (encrypt-aes128 [:cached "p"])
             (decrypt-aes128 [:cached "p"])
             (decrypt-aes128 [:salted "p"])
             (String.))))
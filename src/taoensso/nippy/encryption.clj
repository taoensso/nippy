(ns taoensso.nippy.encryption
  "Simple no-nonsense crypto with reasonable defaults"
  (:require [taoensso.encore :as enc]))

;; Note that AES128 may be preferable to AES256 due to known attack
;; vectors specific to AES256, Ref. https://www.schneier.com/blog/archives/2009/07/another_new_aes.html
;; Or, for a counter argument, Ref. https://blog.agilebits.com/2013/03/09/guess-why-were-moving-to-256-bit-aes-keys/

;;;; Interface

(def standard-header-ids "These'll support :auto thaw" #{:aes128-sha512})

(defprotocol IEncryptor
  (header-id      [encryptor])
  (encrypt ^bytes [encryptor pwd ba])
  (decrypt ^bytes [encryptor pwd ba]))

;;;; Default digests, ciphers, etc.

;; TODO Prefer GCM > CBC ("AES/GCM/NoPadding", Ref. https://goo.gl/jpZoj8
(def ^:private aes-cipher* (enc/thread-local-proxy (javax.crypto.Cipher/getInstance "AES/CBC/PKCS5Padding")))
(def ^:private sha512-md*  (enc/thread-local-proxy (java.security.MessageDigest/getInstance "SHA-512")))
(def ^:private prng*       (enc/thread-local-proxy (java.security.SecureRandom/getInstance "SHA1PRNG")))

(defn- aes-cipher ^javax.crypto.Cipher         [] (.get ^ThreadLocal aes-cipher*))
(defn- sha512-md  ^java.security.MessageDigest [] (.get ^ThreadLocal sha512-md*))
(defn- prng       ^java.security.SecureRandom  [] (.get ^ThreadLocal prng*))

(def ^:private ^:const aes-block-size (.getBlockSize (aes-cipher)))
(def ^:private ^:const aes-iv-size    aes-block-size #_12) ; 12 for GCM, Ref. https://goo.gl/c8mhqp
(def ^:private ^:const salt-size      16)

(defn- rand-bytes [size] (let [ba (byte-array size)] (.nextBytes (prng) ba) ba))

;;;; Default key derivation (salt+password -> key)

(defn- sha512-key
  "SHA512-based key generator. Good JVM availability without extra dependencies
  (PBKDF2, bcrypt, scrypt, etc.). Decent security when using many rounds."

  ([salt-ba pwd        ] (sha512-key salt-ba pwd (* Short/MAX_VALUE (if salt-ba 5 64))))
  ([salt-ba pwd ^long n]
   (let [md      (sha512-md)
         init-ba (let [pwd-ba (.getBytes ^String pwd "UTF-8")]
                   (if salt-ba (enc/ba-concat salt-ba pwd-ba) pwd-ba))
         ^bytes ba (enc/reduce-n (fn [acc in] (.digest md acc)) init-ba n)]

     (-> ba
       (java.util.Arrays/copyOf aes-block-size)
       (javax.crypto.spec.SecretKeySpec. "AES")))))

(comment
  (enc/qb 10
    (sha512-key nil "hi" (* Short/MAX_VALUE 1))   ; ~40ms per hash (fast)
    (sha512-key nil "hi" (* Short/MAX_VALUE 5))   ; ~180ms  (default)
    (sha512-key nil "hi" (* Short/MAX_VALUE 32))  ; ~1200ms (conservative)
    (sha512-key nil "hi" (* Short/MAX_VALUE 128)) ; ~4500ms (paranoid)
    ))

;;;; Default implementations

(defn- throw-destructure-ex [typed-password]
  (throw (ex-info
           (str "Expected password form: "
             "[<#{:salted :cached}> <password-string>].\n "
             "See `default-aes128-encryptor` docstring for details!")
           {:typed-password typed-password})))

(defn- destructure-typed-pwd [typed-password]
  (if (vector? typed-password)
    (let [[type password] typed-password]
      (if (#{:salted :cached} type)
        [type password]
        (throw-destructure-ex typed-password)))
    (throw-destructure-ex typed-password)))

(comment (destructure-typed-pwd [:salted "foo"]))

(deftype AES128Encryptor [header-id keyfn cached-keyfn]
  IEncryptor
  (header-id [_] header-id)
  (encrypt   [_ typed-pwd ba]
    (let [[type pwd] (destructure-typed-pwd typed-pwd)
          salt?      (identical? type :salted)
          iv-ba      (rand-bytes aes-iv-size)
          salt-ba    (when salt? (rand-bytes salt-size))
          prefix-ba  (if   salt? (enc/ba-concat iv-ba salt-ba) iv-ba)
          key-spec   (if   salt?
                       (keyfn        salt-ba pwd)
                       (cached-keyfn salt-ba pwd))
          ;; param-spec (javax.crypto.spec.GCMParameterSpec. 128 iv-ba)
          param-spec (javax.crypto.spec.IvParameterSpec. iv-ba)
          cipher     (aes-cipher)]

      (.init cipher javax.crypto.Cipher/ENCRYPT_MODE
        ^javax.crypto.spec.SecretKeySpec key-spec param-spec)
      (enc/ba-concat prefix-ba (.doFinal cipher ba))))

  (decrypt [_ typed-pwd ba]
    (let [[type pwd]          (destructure-typed-pwd typed-pwd)
          salt?               (identical? type :salted)
          prefix-size         (+ aes-iv-size (if salt? salt-size 0))
          [prefix-ba data-ba] (enc/ba-split ba prefix-size)
          [iv-ba salt-ba]     (if salt?
                                (enc/ba-split prefix-ba aes-iv-size)
                                [prefix-ba nil])
          key-spec (if salt?
                     (keyfn        salt-ba pwd)
                     (cached-keyfn salt-ba pwd))

          ;; param-spec (javax.crypto.spec.GCMParameterSpec. 128 iv-ba)
          param-spec (javax.crypto.spec.IvParameterSpec. iv-ba)
          cipher     (aes-cipher)]

      (.init cipher javax.crypto.Cipher/DECRYPT_MODE
        ^javax.crypto.spec.SecretKeySpec key-spec param-spec)
      (.doFinal cipher data-ba))))

(def aes128-encryptor
  "Default 128bit AES encryptor with many-round SHA-512 key-gen.

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

  (AES128Encryptor. :aes128-sha512 sha512-key (enc/memoize_ sha512-key)))

;;;; Default implementation

(comment
  (def dae aes128-encryptor)
  (def secret-ba (.getBytes "Secret message" "UTF-8"))
  (encrypt dae "p" secret-ba) ; Malformed
  (enc/qb 10
    (encrypt dae [:salted "p"] secret-ba)
    (encrypt dae [:cached "p"] secret-ba))

  (enc/qb 10
    (->> secret-ba
      (encrypt dae [:salted "p"])
      (encrypt dae [:cached "p"])
      (decrypt dae [:cached "p"])
      (decrypt dae [:salted "p"])
      (String.))))

(ns taoensso.nippy.encryption
  "Simple no-nonsense crypto with reasonable defaults"
  (:require
   [taoensso.encore       :as enc]
   [taoensso.nippy.crypto :as crypto]))

(def standard-header-ids "These'll support :auto thaw" #{:aes128-sha512})

(defprotocol IEncryptor
  (header-id      [encryptor])
  (encrypt ^bytes [encryptor pwd ba])
  (decrypt ^bytes [encryptor pwd ba]))

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

(deftype AES128Encryptor [header-id salted-key-fn cached-key-fn]
  IEncryptor
  (header-id [_] header-id)
  (encrypt   [_ typed-pwd plain-ba]
    (let [[type pwd] (destructure-typed-pwd typed-pwd)
          salt?      (identical? type :salted)
          ?salt-ba   (when salt? (crypto/rand-bytes 16))
          key-ba
          (crypto/take-ba 16 ; 128 bit AES
            (if-let [salt-ba ?salt-ba]
              (salted-key-fn salt-ba pwd)
              (cached-key-fn nil     pwd)))]

      (crypto/encrypt
        {:cipher-kit crypto/cipher-kit-aes-cbc
         :?salt-ba   ?salt-ba
         :key-ba     key-ba
         :plain-ba   plain-ba})))

  (decrypt [_ typed-pwd enc-ba]
    (let [[type pwd] (destructure-typed-pwd typed-pwd)
          salt?      (identical? type :salted)
          salt->key-fn
          (if salt?
            #(salted-key-fn % pwd)
            #(cached-key-fn % pwd))]

      (crypto/decrypt
        {:cipher-kit   crypto/cipher-kit-aes-cbc
         :salt-size    (if salt? 16 0)
         :salt->key-fn salt->key-fn
         :enc-ba       enc-ba}))))

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

  (AES128Encryptor. :aes128-sha512
    (do           (fn [ salt-ba pwd] (crypto/take-ba 16 (crypto/sha512-key-ba salt-ba pwd (* Short/MAX_VALUE 5)))))
    (enc/memoize_ (fn [_salt-ba pwd] (crypto/take-ba 16 (crypto/sha512-key-ba nil     pwd (* Short/MAX_VALUE 64)))))))

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

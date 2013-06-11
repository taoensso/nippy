(ns taoensso.nippy.crypto
  "Alpha - subject to change.
  Simple no-nonsense crypto with reasonable defaults. Because your Clojure data
  deserves some privacy."
  {:author "Peter Taoussanis"}
  (:require [taoensso.nippy.utils :as utils]))

(defprotocol ICrypto "Simple cryptography interface."
  (gen-key ^javax.crypto.spec.SecretKeySpec [crypto salt pwd]
    "Returns an appropriate SecretKeySpec.")
  (encrypt ^bytes [crypto salt pwd ba] "Returns encrypted bytes.")
  (decrypt ^bytes [crypto salt pwd ba] "Returns decrypted bytes."))

(defrecord CryptoAES [cipher-type default-salt key-gen-opts cache])

(def ^:private ^java.security.MessageDigest sha-md
  (java.security.MessageDigest/getInstance "SHA-512"))

(def ^:private ^:const aes128-block-size (int 16))

(defn- sha512-key
  "Default SHA512-based key generator. Good JVM availability without extra
  dependencies (PBKDF2, bcrypt, scrypt, etc.). Decent security with multiple
  rounds. VERY aggressive multiples (>64) possible+recommended when cached."
  [^String salted-pwd & [{:keys [rounds-multiple]
                          :or   {rounds-multiple 5}}]] ; Cacheable
  (loop [^bytes ba (.getBytes salted-pwd "UTF-8")
         n (* (int Short/MAX_VALUE) (or rounds-multiple 5))]
    (if-not (zero? n)
      (recur (.digest sha-md ba) (dec n))
      (-> ba
          ;; 128bit keys have good JVM availability and are
          ;; entirely sufficient, Ref. http://goo.gl/2YRQG
          (java.util.Arrays/copyOf aes128-block-size)
          (javax.crypto.spec.SecretKeySpec. "AES")))))

(comment
  (time (sha512-key "hi" {:rounds-multiple 1}))   ; ~40ms per hash (fast)
  (time (sha512-key "hi" {:rounds-multiple 5}))   ; ~180ms  (default)
  (time (sha512-key "hi" {:rounds-multiple 32}))  ; ~1200ms (conservative)
  (time (sha512-key "hi" {:rounds-multiple 128})) ; ~4500ms (paranoid)
  )

(def ^:private cipher* (memoize #(javax.crypto.Cipher/getInstance %)))
(defn- cipher ^javax.crypto.Cipher [cipher-type] (cipher* cipher-type))

(def ^:private ^java.security.SecureRandom rand-gen
  (java.security.SecureRandom/getInstance "SHA1PRNG"))
(defn- rand-bytes [size] (let [seed (make-array Byte/TYPE size)]
                           (.nextBytes rand-gen seed) seed))

(extend-type CryptoAES
  ICrypto
  (gen-key [{:keys [default-salt key-gen-opts cache]} salt pwd]
    (utils/apply-memoized cache
     sha512-key (str (or salt default-salt) pwd) key-gen-opts))

  (encrypt [{:keys [cipher-type cache] :as crypto} salt pwd ba]
    (let [cipher (cipher cipher-type)
          key    (gen-key crypto salt pwd)
          iv-ba  (rand-bytes aes128-block-size)
          iv     (javax.crypto.spec.IvParameterSpec. iv-ba)]
      (.init cipher javax.crypto.Cipher/ENCRYPT_MODE key iv)
      (.doFinal cipher (utils/ba-concat iv-ba ba))))

  (decrypt [{:keys [cipher-type cache] :as crypto} salt pwd ba]
    (let [cipher (cipher cipher-type)
          key    (gen-key crypto salt pwd)
          [iv-ba data-ba] (utils/ba-split ba aes128-block-size)
          iv     (javax.crypto.spec.IvParameterSpec. iv-ba)]
      (.init cipher javax.crypto.Cipher/DECRYPT_MODE key iv)
      (.doFinal cipher data-ba))))

(defn crypto-aes128
  "Returns a new CryptoAES object with options:
    :default-salt    - Shared fallback password salt when none is provided. If
                       the use case allows it, a unique random salt per
                       encrypted item is better.
    :cache-keys?     - IMPORTANT. DO enable this if and ONLY if your use case
                       involves only a small, finite number of unique secret
                       keys (salt+password)s. Dramatically improves `gen-key`
                       performance in those cases and (as a result) allows for
                       a *much* stronger `key-work-factor`.
    :key-work-factor - O(n) CPU time needed to generate keys. Larger factors
                       provide more protection against brute-force attacks but
                       make encryption+decryption slower if `:cache-keys?` is
                       not enabled.

                       Some sensible values (from fast to strong):
                         Without caching: 1, 5,  10
                            With caching: 5, 32, 64, 128

  See also `crypto-default` and `crypto-default-cached` for sensible ready-made
  CryptoAES objects."
  [& [{:keys [default-salt cache-keys? key-work-factor]
       :or   {default-salt "XA~I3(:]3'ck5!M[z\\m`l^0mltR~y/]Arq_d9+$`e#yJssN^8"
              key-work-factor 5}}]]
  (CryptoAES. "AES/CBC/PKCS5Padding"
              default-salt
              {:rounds-multiple (int key-work-factor)}
              (when cache-keys? (atom {}))))

(def crypto-default        (crypto-aes128))
(def crypto-default-cached (crypto-aes128 {:cache-keys?     true
                                           :key-work-factor 64}))

(comment
  (time (gen-key crypto-default        "my-salt" "my-password"))
  (time (gen-key crypto-default-cached "my-salt" "my-password"))
  (time (->> (.getBytes "Secret message" "UTF-8")
             (encrypt crypto-default "s" "p")
             (encrypt crypto-default "s" "p")
             (decrypt crypto-default "s" "p")
             (decrypt crypto-default "s" "p")
             (String.))))
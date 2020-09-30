(ns taoensso.nippy.crypto
  "Low-level crypto utils.
  Private & alpha, very likely to change!"
  (:refer-clojure :exclude [rand-nth])
  (:require [taoensso.min-encore :as enc]
            [taoensso.truss :as truss]))

;; Note that AES128 may be preferable to AES256 due to known attack
;; vectors specific to AES256, Ref. https://goo.gl/qU4CCV
;;  or for a counter argument, Ref. https://goo.gl/9LA9Yb

;;;; Randomness

(do
  (defn rand-nth            [coll]           (nth coll (int (* (.nextDouble   (enc/secure-rng)) (count coll)))))
  (defn rand-bytes  ^bytes  [size] (let [ba (byte-array size)] (.nextBytes    (enc/secure-rng) ba) ba))
  (defn rand-double ^double []                                 (.nextDouble   (enc/secure-rng)))
  (defn rand-gauss  ^double []                                 (.nextGaussian (enc/secure-rng)))
  (defn rand-bool           []                                 (.nextBoolean  (enc/secure-rng)))
  (defn rand-long
    (^long   [ ]                   (.nextLong   (enc/secure-rng)))
    (^long   [n] (long (* (long n) (.nextDouble (enc/secure-rng)))))))

(comment
  (seq (rand-bytes 16))
  (rand-nth [:a :b :c :d])
  (rand-long 100))

;;;; Hashing

(def ^:private sha256-md* (enc/thread-local-proxy (java.security.MessageDigest/getInstance "SHA-256")))
(def ^:private sha512-md* (enc/thread-local-proxy (java.security.MessageDigest/getInstance "SHA-512")))
(defn  sha256-md ^java.security.MessageDigest [] (.get ^ThreadLocal sha256-md*))
(defn  sha512-md ^java.security.MessageDigest [] (.get ^ThreadLocal sha512-md*))
(defn  sha256-ba ^bytes [ba] (.digest (sha256-md) ba))
(defn  sha512-ba ^bytes [ba] (.digest (sha512-md) ba))
(defn murmur3 [^String s] (clojure.lang.Murmur3/hashUnencodedChars s))

;;;; Key derivation (salt+password -> key / hash)
;; (fn [salt-ba utf8]) -> bytes

;; (defn  ba->hex [^bytes ba] (org.apache.commons.codec.binary.Hex/encodeHexString ba))
(defn   take-ba ^bytes [n ^bytes ba] (java.util.Arrays/copyOf ba ^int n)) ; Pads if ba too small
(defn  utf8->ba ^bytes [^String s] (.getBytes s "UTF-8"))
(defn- add-salt ^bytes [?salt-ba ba] (if ?salt-ba (enc/ba-concat ?salt-ba ba) ba))
(defn pwd-as-ba ^bytes [utf8-or-ba] (if (string? utf8-or-ba) (utf8->ba utf8-or-ba) (truss/have bytes? utf8-or-ba)))

(comment (seq (pwd-as-ba "foo")))

(defn sha512-key-ba
  "SHA512-based key generator. Good JVM availability without extra dependencies
  (PBKDF2, bcrypt, scrypt, etc.). Decent security when using many rounds."
  (^bytes [?salt-ba utf8-or-ba               ] (sha512-key-ba ?salt-ba utf8-or-ba 163835 #_(* Short/MAX_VALUE 5)))
  (^bytes [?salt-ba utf8-or-ba ^long n-rounds]
   (let [ba (add-salt ?salt-ba (pwd-as-ba utf8-or-ba))
         md (sha512-md)]
     (enc/reduce-n (fn [acc in] (.digest md acc)) ba n-rounds))))

(comment
  (count (seq (sha512-key-ba (utf8->ba "salt") "password" 1)))
  (count (seq (sha512-key-ba nil               "password" 1))))

;;;; Crypto

(defprotocol ICipherKit
  (get-cipher     ^javax.crypto.Cipher [_] "Returns a thread-safe `javax.crypto.Cipher` instance.")
  (get-iv-size                         [_] "Returns necessary iv-ba length.")
  (get-key-spec   ^javax.crypto.spec.SecretKeySpec           [_    ba] "Returns a `javax.crypto.spec.SecretKeySpec`.")
  (get-param-spec ^java.security.spec.AlgorithmParameterSpec [_ iv-ba] "Returns a `java.security.spec.AlgorithmParameters`."))

;; Prefer GCM > CBC, Ref. https://goo.gl/jpZoj8
(def ^:private gcm-cipher* (enc/thread-local-proxy (javax.crypto.Cipher/getInstance "AES/GCM/NoPadding")))
(def ^:private cbc-cipher* (enc/thread-local-proxy (javax.crypto.Cipher/getInstance "AES/CBC/PKCS5Padding")))

(defn gcm-cipher ^javax.crypto.Cipher [] (.get ^ThreadLocal gcm-cipher*))
(defn cbc-cipher ^javax.crypto.Cipher [] (.get ^ThreadLocal cbc-cipher*))
;
(deftype CipherKit-AES-GCM []
  ICipherKit
  (get-cipher     [_] (gcm-cipher))
  (get-iv-size    [_] 12)
  (get-key-spec   [_    ba] (javax.crypto.spec.SecretKeySpec. ba "AES"))
  (get-param-spec [_ iv-ba] (javax.crypto.spec.GCMParameterSpec. 128 iv-ba)))

(deftype CipherKit-AES-CBC []
  ICipherKit
  (get-cipher     [_] (cbc-cipher))
  (get-iv-size    [_] 16)
  (get-key-spec   [_    ba] (javax.crypto.spec.SecretKeySpec. ba "AES"))
  (get-param-spec [_ iv-ba] (javax.crypto.spec.IvParameterSpec. iv-ba)))

(def cipher-kit-aes-gcm "Default CipherKit for AES GCM" (CipherKit-AES-GCM.))
(def cipher-kit-aes-cbc "Default CipherKit for AES CBC" (CipherKit-AES-CBC.))

;;  Output bytes: [         <iv>            <?salt> <encrypted>]
;; Could also do: [<iv-len> <iv> <salt-len> <?salt> <encrypted>]
(defn encrypt
  [{:keys [cipher-kit ?salt-ba key-ba plain-ba rand-bytes-fn]
    :or   {cipher-kit    cipher-kit-aes-gcm
           rand-bytes-fn rand-bytes}}]

  (let [iv-size     (long (get-iv-size cipher-kit))
        iv-ba       (rand-bytes-fn iv-size)
        prefix-ba   (if ?salt-ba (enc/ba-concat iv-ba ?salt-ba) iv-ba)
        key-spec    (get-key-spec   cipher-kit key-ba)
        param-spec  (get-param-spec cipher-kit iv-ba)
        cipher      (get-cipher     cipher-kit)]

    (.init cipher javax.crypto.Cipher/ENCRYPT_MODE key-spec param-spec)
    (enc/ba-concat prefix-ba (.doFinal cipher plain-ba))))

(comment (encrypt {:?salt-ba nil :key-ba (take-ba 16 (sha512-key-ba nil "pwd")) :plain-ba (utf8->ba "data")}))

(defn decrypt
  [{:keys [cipher-kit salt-size salt->key-fn enc-ba]
    :or   {cipher-kit cipher-kit-aes-gcm}}]
  (let [salt-size          (long salt-size)
        iv-size            (long (get-iv-size cipher-kit))
        prefix-size        (+ iv-size salt-size)
        [prefix-ba enc-ba] (enc/ba-split enc-ba prefix-size)
        [iv-ba salt-ba]    (if (pos? salt-size)
                             (enc/ba-split prefix-ba iv-size)
                             [prefix-ba nil])

        key-ba     (salt->key-fn salt-ba)
        key-spec   (get-key-spec   cipher-kit key-ba)
        param-spec (get-param-spec cipher-kit iv-ba)
        cipher     (get-cipher     cipher-kit)]

    (.init cipher javax.crypto.Cipher/DECRYPT_MODE key-spec param-spec)
    (.doFinal cipher enc-ba)))

(comment
  (do
    (defn sha512-k16 [?salt-ba pwd] (take-ba 16 (sha512-key-ba ?salt-ba pwd)))
    (defn roundtrip [kit ?salt-ba key-ba key-fn]
      (let [salt-size (count ?salt-ba)
            encr (encrypt {:cipher-kit kit :?salt-ba ?salt-ba :key-ba key-ba :plain-ba (utf8->ba "data")})
            decr (decrypt {:cipher-kit kit :salt-size salt-size :salt->key-fn key-fn :enc-ba encr})]
        (String. ^bytes decr "UTF-8")))

    [(let [s (rand-bytes 16)] (roundtrip cipher-kit-aes-gcm s (sha512-k16 s "pwd") #(sha512-k16 % "pwd")))
     (let [s             nil] (roundtrip cipher-kit-aes-gcm s (sha512-k16 s "pwd") #(sha512-k16 % "pwd")))
     (let [s (rand-bytes 16)] (roundtrip cipher-kit-aes-cbc s (sha512-k16 s "pwd") #(sha512-k16 % "pwd")))
     (let [s             nil] (roundtrip cipher-kit-aes-cbc s (sha512-k16 s "pwd") #(sha512-k16 % "pwd")))])

  (enc/qb 10
    (let [s (rand-bytes 16)]
      (roundtrip cipher-kit-aes-gcm s (sha512-k16 s "pwd") #(sha512-k16 % "pwd"))))
  ;; 2394.89
  )

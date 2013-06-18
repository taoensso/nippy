(defproject com.taoensso/nippy "2.0.0-beta1"
  :description "Clojure serialization library"
  :url "https://github.com/ptaoussanis/nippy"
  :license {:name "Eclipse Public License"
            :url  "http://www.eclipse.org/legal/epl-v10.html"}
  :dependencies [[org.clojure/clojure    "1.4.0"]
                 [expectations           "1.4.43"]
                 [org.iq80.snappy/snappy "0.3"]
                 [cc.qbits/grease        "0.2.1"]]
  :profiles {:1.4   {:dependencies [[org.clojure/clojure "1.4.0"]]}
             :1.5   {:dependencies [[org.clojure/clojure "1.5.1"]]}
             :dev   {:dependencies []}
             :test  {:dependencies [[org.xerial.snappy/snappy-java "1.0.5-M3"]]}
             :bench {:dependencies []
                     :jvm-opts ["-server" "-XX:+UseCompressedOops"]}}
  :aliases {"test-all"    ["with-profile" "test,1.4:test,1.5" "expectations"]
            "test-auto"   ["with-profile" "test" "autoexpect"]
            "start-dev"   ["with-profile" "dev,test,bench" "repl" ":headless"]
            "start-bench" ["trampoline" "start-dev"]}
  :plugins [[lein-expectations "0.0.7"]
            [lein-autoexpect   "0.2.5"]
            [codox             "0.6.4"]]
  :min-lein-version "2.0.0"
  :warn-on-reflection true)

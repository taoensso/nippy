(defproject com.taoensso/nippy "0.10.2"
  :description "Simple, high-performance Clojure serialization library."
  :url "https://github.com/ptaoussanis/nippy"
  :license {:name "Eclipse Public License"}
  :dependencies [[org.clojure/clojure           "1.3.0"]
                 [org.xerial.snappy/snappy-java "1.0.5-M1"]]
  :profiles {:1.3  {:dependencies [[org.clojure/clojure "1.3.0"]]}
             :1.4  {:dependencies [[org.clojure/clojure "1.4.0"]]}
             :1.5  {:dependencies [[org.clojure/clojure "1.5.0-alpha3"]]}
             :dev  {:dependencies []}
             :test {:dependencies []}}
  :aliases {"test-all" ["with-profile" "test,1.3:test,1.4:test,1.5" "test"]}
  :min-lein-version "2.0.0"
  :warn-on-reflection true)
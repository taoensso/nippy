(defproject com.taoensso/nippy "1.3.0-alpha3"
  :description "Clojure serialization library"
  :url "https://github.com/ptaoussanis/nippy"
  :license {:name "Eclipse Public License"
            :url  "http://www.eclipse.org/legal/epl-v10.html"}
  :dependencies [[org.clojure/clojure    "1.3.0"]
                 [org.iq80.snappy/snappy "0.3"]]
  :profiles {:1.3  {:dependencies [[org.clojure/clojure "1.3.0"]]}
             :1.4  {:dependencies [[org.clojure/clojure "1.4.0"]]}
             :1.5  {:dependencies [[org.clojure/clojure "1.5.1"]]}
             :dev  {:dependencies []}
             :test {:dependencies [[org.xerial.snappy/snappy-java "1.0.5-M3"]]}}
  :aliases {"test-all" ["with-profile" "test,1.3:test,1.4:test,1.5" "test"]}
  :plugins [[codox "0.6.4"]]
  :min-lein-version "2.0.0"
  :warn-on-reflection true)
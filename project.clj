(defproject com.taoensso/nippy "0.9.0"
  :description "Simple, high-performance Clojure serialization library."
  :url "https://github.com/ptaoussanis/nippy"
  :license {:name "Eclipse Public License"}
  :dependencies [[org.clojure/clojure           "1.3.0"]
                 [org.xerial.snappy/snappy-java "1.0.4.1"]]
  :profiles {:1.3   {:dependencies [[org.clojure/clojure "1.3.0"]]}
             :1.4   {:dependencies [[org.clojure/clojure "1.4.0"]]}
             :1.5   {:dependencies [[org.clojure/clojure "1.5.0-master-SNAPSHOT"]]}
             :dev   {:dependencies []}}
  :repositories {"sonatype" {:url "http://oss.sonatype.org/content/repositories/releases"
                             :snapshots false
                             :releases {:checksum :fail :update :always}}
                 "sonatype-snapshots" {:url "http://oss.sonatype.org/content/repositories/snapshots"
                                       :snapshots true
                                       :releases {:checksum :fail :update :always}}}
  :aliases {"all" ["with-profile" "1.3:1.4:1.5"]}
  :min-lein-version "2.0.0"
  :warn-on-reflection true)
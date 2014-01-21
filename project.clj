(defproject com.taoensso/nippy "2.5.2"
  :description "Clojure serialization library"
  :url "https://github.com/ptaoussanis/nippy"
  :license {:name "Eclipse Public License"
            :url  "http://www.eclipse.org/legal/epl-v10.html"}
  :dependencies [[org.clojure/clojure      "1.4.0"]
                 [org.clojure/tools.reader "0.8.3"]
                 [org.iq80.snappy/snappy   "0.3"]
                 [org.tukaani/xz           "1.4"]]
  :profiles {:1.4   {:dependencies [[org.clojure/clojure "1.4.0"]]}
             :1.5   {:dependencies [[org.clojure/clojure "1.5.1"]]}
             :1.6   {:dependencies [[org.clojure/clojure "1.6.0-alpha3"]]}
             :dev   {:dependencies []}
             :test  {:dependencies [[expectations                  "1.4.56"]
                                    [org.xerial.snappy/snappy-java "1.1.1-M1"]
                                    [reiddraper/simple-check       "0.5.3"]]}
             :bench {:dependencies [] :jvm-opts ^:replace ["-server"]}}
  :aliases {"test-all"    ["with-profile" "+test,+1.4:+test,+1.5:+test,+1.6" "expectations"]
            "test-auto"   ["with-profile" "+test" "autoexpect"]
            "start-dev"   ["with-profile" "+dev,+test,+bench" "repl" ":headless"]
            "start-bench" ["trampoline" "start-dev"]
            "codox"       ["with-profile" "+test" "doc"]}
  :plugins [[lein-expectations "0.0.8"]
            [lein-autoexpect   "1.2.1"]
            [lein-ancient      "0.5.4"]
            [codox             "0.6.6"]]
  :min-lein-version "2.0.0"
  :global-vars {*warn-on-reflection* true}
  :repositories
  {"sonatype"
   {:url "http://oss.sonatype.org/content/repositories/releases"
    :snapshots false
    :releases {:checksum :fail}}
   "sonatype-snapshots"
   {:url "http://oss.sonatype.org/content/repositories/snapshots"
    :snapshots true
    :releases {:checksum :fail :update :always}}})

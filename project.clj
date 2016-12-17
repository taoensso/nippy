(defproject com.taoensso/nippy "2.13.0-RC1"
  :author "Peter Taoussanis <https://www.taoensso.com>"
  :description "High-performance serialization library for Clojure"
  :url "https://github.com/ptaoussanis/nippy"
  :license {:name "Eclipse Public License"
            :url  "http://www.eclipse.org/legal/epl-v10.html"
            :distribution :repo
            :comments "Same as Clojure"}
  :min-lein-version "2.3.3"
  :global-vars {*warn-on-reflection* true
                *assert*             true
                ;; *unchecked-math*  :warn-on-boxed
                }

  :dependencies
  [[org.clojure/clojure      "1.5.1"]
   [org.clojure/tools.reader "0.10.0"]
   [com.taoensso/encore      "2.88.0"]
   [org.iq80.snappy/snappy   "0.4"]
   [org.tukaani/xz           "1.6"]
   [net.jpountz.lz4/lz4      "1.3"]]

  :profiles
  {;; :default [:base :system :user :provided :dev]
   :server-jvm {:jvm-opts ^:replace ["-server" "-Xms1024m" "-Xmx2048m"]}
   :1.5  {:dependencies [[org.clojure/clojure "1.5.1"]]}
   :1.6  {:dependencies [[org.clojure/clojure "1.6.0"]]}
   :1.7  {:dependencies [[org.clojure/clojure "1.7.0"]]}
   :1.8  {:dependencies [[org.clojure/clojure "1.8.0"]]}
   :1.9  {:dependencies [[org.clojure/clojure "1.9.0-alpha10"]]}
   :test {:jvm-opts     ["-Xms1024m" "-Xmx2048m"]
          :dependencies [[org.clojure/test.check        "0.9.0"]
                         [org.clojure/data.fressian     "0.2.1"]
                         [org.xerial.snappy/snappy-java "1.1.2.6"]]}
   :dev [:1.9 :test :server-jvm
         {:plugins
          [[lein-pprint  "1.1.2"]
           [lein-ancient "0.6.10"]
           [lein-codox   "0.10.2"]]}]}

  :test-paths ["test" "src"]

  :codox
  {:language :clojure
   :source-uri "https://github.com/ptaoussanis/nippy/blob/master/{filepath}#L{line}"}

  :aliases
  {"test-all"   ["with-profile" "+1.9:+1.8:+1.7:+1.6:+1.5" "test"]
   "deploy-lib" ["do" "deploy" "clojars," "install"]
   "start-dev"  ["with-profile" "+dev" "repl" ":headless"]}

  :repositories {"sonatype-oss-public"
                 "https://oss.sonatype.org/content/groups/public/"})

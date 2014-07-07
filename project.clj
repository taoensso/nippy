(defproject com.taoensso/nippy "2.7.0-SNAPSHOT"
  :author "Peter Taoussanis <https://www.taoensso.com>"
  :description "Clojure serialization library"
  :url "https://github.com/ptaoussanis/nippy"
  :license {:name "Eclipse Public License"
            :url  "http://www.eclipse.org/legal/epl-v10.html"
            :distribution :repo
            :comments "Same as Clojure"}
  :min-lein-version "2.3.3"
  :global-vars {*warn-on-reflection* true
                *assert* true}

  :dependencies
  [[org.clojure/clojure      "1.4.0"]
   [org.clojure/tools.reader "0.8.5"]
   [com.taoensso/encore      "1.7.0"]
   [org.iq80.snappy/snappy   "0.3"]
   [org.tukaani/xz           "1.5"]
   [net.jpountz.lz4/lz4      "1.2.0"]]

  :profiles
  {;; :default [:base :system :user :provided :dev]
   :server-jvm {:jvm-opts ^:replace ["-server" "-Xms1024m" "-Xmx2048m"]}
   :1.5  {:dependencies [[org.clojure/clojure "1.5.1"]]}
   :1.6  {:dependencies [[org.clojure/clojure "1.6.0"]]}
   :test {:jvm-opts     ["-Xms1024m" "-Xmx2048m"]
          :dependencies [[expectations                  "1.4.56"]
                         [org.clojure/test.check        "0.5.8"]
                         ;; [com.cemerick/double-check  "0.5.7"]
                         [org.clojure/data.fressian     "0.2.0"]
                         [org.xerial.snappy/snappy-java "1.1.1"]]}
   :dev [:1.6 :test
         {:plugins
          [[lein-pprint       "1.1.1"]
           [lein-ancient      "0.5.5"]
           [lein-expectations "0.0.8"]
           [lein-autoexpect   "1.2.2"]
           [codox             "0.8.10"]]}]}

  :test-paths ["test" "src"]

  :aliases
  {"test-all"   ["with-profile" "default:+1.5:+1.6" "expectations"]
   ;; "test-all"   ["with-profile" "default:+1.6" "expectations"]
   "test-auto"  ["with-profile" "+test" "autoexpect"]
   "deploy-lib" ["do" "deploy" "clojars," "install"]
   "start-dev"  ["with-profile" "+server-jvm" "repl" ":headless"]}

  :repositories
  {"sonatype"
   {:url "http://oss.sonatype.org/content/repositories/releases"
    :snapshots false
    :releases {:checksum :fail}}
   "sonatype-snapshots"
   {:url "http://oss.sonatype.org/content/repositories/snapshots"
    :snapshots true
    :releases {:checksum :fail :update :always}}})

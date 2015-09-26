(defproject com.taoensso/nippy "2.10.0-beta1"
  :author "Peter Taoussanis <https://www.taoensso.com>"
  :description "Clojure serialization library"
  :url "https://github.com/ptaoussanis/nippy"
  :license {:name "Eclipse Public License"
            :url  "http://www.eclipse.org/legal/epl-v10.html"
            :distribution :repo
            :comments "Same as Clojure"}
  :min-lein-version "2.3.3"
  :global-vars {*warn-on-reflection* true
                *assert*             true
                *unchecked-math*     :warn-on-boxed}

  :dependencies
  [[org.clojure/clojure      "1.4.0"]
   [org.clojure/tools.reader "0.9.2"]
   [com.taoensso/encore      "1.38.0"] ; 2.x needs Clojure 1.5+
   [org.iq80.snappy/snappy   "0.4"]
   [org.tukaani/xz           "1.5"]
   [net.jpountz.lz4/lz4      "1.3"]]

  :profiles
  {;; :default [:base :system :user :provided :dev]
   :server-jvm {:jvm-opts ^:replace ["-server" "-Xms1024m" "-Xmx2048m"]}
   :1.5  {:dependencies [[org.clojure/clojure "1.5.1"]]}
   :1.6  {:dependencies [[org.clojure/clojure "1.6.0"]]}
   :1.7  {:dependencies [[org.clojure/clojure "1.7.0"]]}
   :test {:jvm-opts     ["-Xms1024m" "-Xmx2048m"]
          :dependencies [[expectations                  "2.1.1"]
                         [org.clojure/test.check        "0.8.2"]
                         ;; [com.cemerick/double-check  "0.6.1"]
                         [org.clojure/data.fressian     "0.2.1"]
                         [org.xerial.snappy/snappy-java "1.1.2"]]}
   :dev [:1.7 :test
         {:dependencies
          [[com.taoensso/encore "2.18.0"]]
          :plugins
          [[lein-pprint       "1.1.1"]
           [lein-ancient      "0.6.7"]
           [lein-expectations "0.0.8"]
           [lein-autoexpect   "1.2.2"]
           [codox             "0.8.10"]]}]}

  :test-paths ["test" "src"]

  :aliases
  {"test-all"   ["with-profile" "default:+1.5:+1.6:+1.7" "expectations"]
   ;; "test-all"   ["with-profile" "default:+1.6" "expectations"]
   "test-auto"  ["with-profile" "+test" "autoexpect"]
   "deploy-lib" ["do" "deploy" "clojars," "install"]
   "start-dev"  ["with-profile" "+server-jvm" "repl" ":headless"]}

  :repositories {"sonatype-oss-public"
                 "https://oss.sonatype.org/content/groups/public/"})

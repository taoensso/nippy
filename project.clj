(defproject com.taoensso/nippy "3.2.0"
  :author "Peter Taoussanis <https://www.taoensso.com>"
  :description "High-performance serialization library for Clojure"
  :url "https://github.com/ptaoussanis/nippy"
  :license {:name "Eclipse Public License"
            :url  "http://www.eclipse.org/legal/epl-v10.html"
            :distribution :repo
            :comments "Same as Clojure"}
  :min-lein-version "2.3.3"
  :global-vars
  {*warn-on-reflection* true
   *assert*             true
   *unchecked-math*     false #_:warn-on-boxed}

  :dependencies
  [[org.clojure/tools.reader "1.3.6"]
   [com.taoensso/encore      "3.31.0"]
   [org.iq80.snappy/snappy   "0.4"]
   [org.tukaani/xz           "1.9"]
   [org.lz4/lz4-java         "1.8.0"]]

  :plugins
  [[lein-pprint  "1.3.2"]
   [lein-ancient "0.7.0"]
   [lein-codox   "0.10.8"]]

  :profiles
  {;; :default [:base :system :user :provided :dev]
   :server-jvm {:jvm-opts ^:replace ["-server" "-Xms1024m" "-Xmx2048m"]}
   :provided {:dependencies [[org.clojure/clojure "1.11.1"]]}
   :c1.11    {:dependencies [[org.clojure/clojure "1.11.1"]]}
   :c1.10    {:dependencies [[org.clojure/clojure "1.10.1"]]}
   :c1.9     {:dependencies [[org.clojure/clojure "1.9.0"]]}

   :depr     {:jvm-opts ["-Dtaoensso.elide-deprecated=true"]}
   :dev      [:c1.11 :test :server-jvm :depr]
   :test
   {:jvm-opts
    ["-Xms1024m" "-Xmx2048m"
     "-Dtaoensso.nippy.thaw-serializable-allowlist-base=base.1, base.2"
     "-Dtaoensso.nippy.thaw-serializable-allowlist-add=add.1 , add.2"]
    :dependencies
    [[org.clojure/test.check        "1.1.1"]
     [org.clojure/data.fressian     "1.0.0"]
     [org.xerial.snappy/snappy-java "1.1.8.4"]]}}

  :test-paths ["test" #_"src"]

  :aliases
  {"start-dev"  ["with-profile" "+dev" "repl" ":headless"]
   "deploy-lib" ["do" #_["build-once"] ["deploy" "clojars"] ["install"]]

   "test-all"
   ["do" ["clean"]
    "with-profile" "+c1.11:+c1.10:+c1.9" "test"]}

  :repositories
  {"sonatype-oss-public"
   "https://oss.sonatype.org/content/groups/public/"})

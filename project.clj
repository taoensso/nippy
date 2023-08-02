(defproject com.taoensso/nippy "3.3.0-RC1"
  :author "Peter Taoussanis <https://www.taoensso.com>"
  :description "The fastest serialization library for Clojure"
  :url "https://github.com/taoensso/nippy"

  :license
  {:name "Eclipse Public License - v 1.0"
   :url  "https://www.eclipse.org/legal/epl-v10.html"}

  :dependencies
  [[org.clojure/tools.reader "1.3.6"]
   [com.taoensso/encore      "3.63.0"]
   [org.iq80.snappy/snappy   "0.4"]
   [org.tukaani/xz           "1.9"]
   [org.lz4/lz4-java         "1.8.0"]]

  :profiles
  {;; :default [:base :system :user :provided :dev]
   :provided {:dependencies [[org.clojure/clojure "1.11.1"]]}
   :c1.11    {:dependencies [[org.clojure/clojure "1.11.1"]]}
   :c1.10    {:dependencies [[org.clojure/clojure "1.10.1"]]}
   :c1.9     {:dependencies [[org.clojure/clojure "1.9.0"]]}

   :test
   {:jvm-opts
    ["-server"
     "-Xms1024m" "-Xmx2048m"
     "-Dtaoensso.elide-deprecated=true"
     "-Dtaoensso.nippy.thaw-serializable-allowlist-base=base.1, base.2"
     "-Dtaoensso.nippy.thaw-serializable-allowlist-add=add.1 , add.2"]

    :global-vars
    {*warn-on-reflection* true
     *assert*             true
     *unchecked-math*     false #_:warn-on-boxed}

    :dependencies
    [[org.clojure/test.check        "1.1.1"]
     [org.clojure/data.fressian     "1.0.0"]
     [org.xerial.snappy/snappy-java "1.1.10.3"]]}

   :graal-tests
   {:dependencies [[org.clojure/clojure "1.11.1"]
                   [com.github.clj-easy/graal-build-time "0.1.4"]]
    :main taoensso.graal-tests
    :aot [taoensso.graal-tests]
    :uberjar-name "graal-tests.jar"}

   :dev
   [:c1.11 :test
    {:plugins
     [[lein-pprint  "1.3.2"]
      [lein-ancient "0.7.0"]
      [com.taoensso.forks/lein-codox "0.10.10"]]

     :codox
     {:language #{:clojure #_:clojurescript}
      :base-language :clojure}}]}

  :test-paths ["test" #_"src"]

  :aliases
  {"start-dev"     ["with-profile" "+dev" "repl" ":headless"]
   ;; "build-once" ["do" ["clean"] ["cljsbuild" "once"]]
   "deploy-lib"    ["do" #_["build-once"] ["deploy" "clojars"] ["install"]]

   "test-clj"     ["with-profile" "+c1.11:+c1.10:+c1.9" "test"]
   ;; "test-cljs" ["with-profile" "+test" "cljsbuild"   "test"]
   "test-all"     ["do" ["clean"] ["test-clj"] #_["test-cljs"]]})

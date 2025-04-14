(defproject com.taoensso/nippy "3.5.0"
  :author "Peter Taoussanis <https://www.taoensso.com>"
  :description "The fastest serialization library for Clojure"
  :url "https://www.taoensso.com/nippy"

  :license
  {:name "Eclipse Public License - v 1.0"
   :url  "https://www.eclipse.org/legal/epl-v10.html"}

  :dependencies
  [[org.clojure/tools.reader "1.5.2"]
   [com.taoensso/encore      "3.142.0"]
   [org.tukaani/xz           "1.10"]
   [io.airlift/aircompressor "2.0.2"]]

  :test-paths ["test" #_"src"]

  :profiles
  {;; :default [:base :system :user :provided :dev]
   :provided {:dependencies [[org.clojure/clojure "1.11.4"]]}
   :c1.12    {:dependencies [[org.clojure/clojure "1.12.0"]]}
   :c1.11    {:dependencies [[org.clojure/clojure "1.11.4"]]}
   :c1.10    {:dependencies [[org.clojure/clojure "1.10.3"]]}

   :graal-tests
   {:source-paths ["test"]
    :main taoensso.graal-tests
    :aot [taoensso.graal-tests]
    :uberjar-name "graal-tests.jar"
    :dependencies
    [[org.clojure/clojure                  "1.11.3"]
     [com.github.clj-easy/graal-build-time "1.0.5"]]}

   :dev
   {:jvm-opts
    ["-server"
     "-Xms1024m" "-Xmx2048m"
     "-Dtaoensso.elide-deprecated=true"
     "-Dtaoensso.nippy.thaw-serializable-allowlist-base=base.1, base.2"
     "-Dtaoensso.nippy.thaw-serializable-allowlist-add=add.1 , add.2"
     #_"-Dtaoensso.nippy.target-release=320"
     #_"-Dtaoensso.nippy.target-release=350"]

    :global-vars
    {*warn-on-reflection* true
     *assert*             true
     *unchecked-math*     false #_:warn-on-boxed}

    :dependencies
    [[org.clojure/test.check    "1.1.1"]
     [org.clojure/data.fressian "1.1.0"]]

    :plugins
    [[lein-pprint  "1.3.2"]
     [lein-ancient "0.7.0"]]}}

  :aliases
  {"start-dev"     ["with-profile" "+dev" "repl" ":headless"]
   ;; "build-once" ["do" ["clean"] ["cljsbuild" "once"]]
   "deploy-lib"    ["do" #_["build-once"] ["deploy" "clojars"] ["install"]]

   "test-clj"     ["with-profile" "+c1.12:+c1.11:+c1.10" "test"]
   ;; "test-cljs" ["with-profile" "+c1.12" "cljsbuild"   "test"]
   "test-all"     ["do" ["clean"] ["test-clj"] #_["test-cljs"]]})

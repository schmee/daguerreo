(defproject schmee/daguerreo "0.1.1"
  :description "A DAG task execution engine for Clojure"
  :url "https://github.com/schmee/daguerreo"
  :license {:name "MIT"
            :url "https://opensource.org/licenses/MIT"}
  :dependencies [[aysylu/loom "1.0.2" :exclusions [tailrecursion/cljs-priority-map]]
                 [better-cond "2.1.0"]
                 [expound "0.7.2"]
                 [org.clojure/core.async "0.4.490"]]
  :repl-options {:init-ns dev
                 :init (do (require 'user :reload) (user/refresh))
                 :caught io.aviso.repl/pretty-pst}
  :source-paths ["src"]
  :test-paths ["test"]
  :profiles {:dev {:dependencies [[org.clojure/clojure "1.10.1"]
                                  [com.rpl/specter "1.1.2"]
                                  [com.taoensso/timbre "4.10.0"]
                                  [nubank/matcher-combinators "1.2.0"]
                                  [org.clojure/test.check "0.10.0"]
                                  [org.clojure/tools.namespace "0.2.11"]
                                  [org.clojure/tools.reader "1.3.2"]]
                   :source-paths ["dev"]}
             :kaocha {:dependencies [[lambdaisland/kaocha "0.0-529"]]}}
  :aliases {"kaocha" ["with-profile" "+kaocha" "run" "-m" "kaocha.runner"]})

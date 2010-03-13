(defproject cascading-clojure "1.1.0"
  :source-path "src/clj"

  :java-source-path "src/jvm"
  :java-fork "true"
  :dependencies [[org.clojure/clojure "1.1.0"]
                 [org.clojure/clojure-contrib "1.1.0"]
                 [cascading/cascading "1.0.17-SNAPSHOT"
                   :exclusions [javax.mail/mail janino/janino]]]
  :dev-dependencies [[lein-javac "0.0.2-SNAPSHOT"]
                     [swank-clojure "1.1.0"]]
  :namespaces [cascading.clojure.core
               cascading.clojure.io
               cascading.clojure.test
               cascading.clojure.core-test])

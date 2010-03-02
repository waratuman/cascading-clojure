(defproject cascading-clojure "1.0.0-SNAPSHOT"
  :source-path "src/clj"
  :java-source-path "src/jvm"
  :java-fork "true"
  :dependencies [[org.clojure/clojure "1.1.0"]
                 [org.clojure/clojure-contrib "1.1.0-master-SNAPSHOT"]
                 [cascading/cascading "1.0.17-SNAPSHOT"
                   :exclusions [javax.mail/mail janino/janino]]
                 [clj-json "0.2.0-SNAPSHOT"]]
  :dev-dependencies [[lein-javac "0.0.2-SNAPSHOT"]
                     [org.clojure/swank-clojure "1.0"]])

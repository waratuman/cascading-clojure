= cascading-clojure

A Clojure library for [Cascading](http://cascading.org).

== Hacking

Get [Leiningen](http://github.com/technomancy/leiningen) 1.1.0.

    $ lein deps
    $ lein compile-java
    $ lein compile
    $ lein test

Note that if you edit either `api.clj` or `testing.clj`, you should `lein compile` before running again.
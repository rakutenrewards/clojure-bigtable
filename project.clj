(defproject clojure-bigtable "0.1.0-SNAPSHOT"
  :description "Clojure interface to Java BigTable client"
  :url "https://github.com/RakutenReady/clojure-bigtable"
  :dependencies [[org.clojure/clojure "1.10.1"]
                 [com.google.cloud/google-cloud-bigtable "1.17.1"]
                 [org.clojure/core.async "1.3.610"]
                 [medley "1.3.0"]]
  :repl-options {:init-ns clojure-bigtable.core})

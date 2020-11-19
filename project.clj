(defproject clojure-bigtable "0.2.0-SNAPSHOT"
  :description "Clojure interface to Java BigTable client"
  :url "https://github.com/RakutenReady/clojure-bigtable"
  :dependencies [[org.clojure/clojure "1.10.1"]
                 [com.google.cloud/google-cloud-bigtable "1.17.1"]
                 [org.clojure/core.async "1.3.610"]
                 [medley "1.3.0"]]
  :deploy-repositories [["github" {:url "https://maven.pkg.github.com/RakutenReady/clojure-bigtable"
                                 :username :env/github_actor
                                 :password :env/github_token
                                 :sign-releases false}]]
  :release-tasks [["vcs" "assert-committed"]
                  ["change" "version" "leiningen.release/bump-version" "release"]
                  ["vcs" "commit"]
                  ["vcs" "tag" "v" "--no-sign"]
                  ["deploy" "github"]
                  ["change" "version" "leiningen.release/bump-version"]
                  ["vcs" "commit"]
                  ["vcs" "push"]]
  :repl-options {:init-ns clojure-bigtable.core})

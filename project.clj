(defproject jepsen.neo4j "0.1.0-SNAPSHOT"
  :description "A Jepsen test for neo4j"
  :license {:name "EPL-2.0 OR GPL-2.0-or-later WITH Classpath-exception-2.0"
            :url "https://www.eclipse.org/legal/epl-2.0/"}
  :main jepsen.neo4j
  :dependencies [[org.clojure/clojure "1.10.0"]
                 [jepsen "0.2.1-SNAPSHOT"]
                 [fullspectrum/neo4clj "1.0.0-ALPHA5"]]
  :repl-options {:init-ns jepsen.neo4j})

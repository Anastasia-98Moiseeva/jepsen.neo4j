(ns jepsen.neo4j
  (:require [clojure.tools.logging :refer :all]
            [clojure.string :as str]
            [jepsen [cli :as cli]
                    [control :as c]
                    [db :as db]
                    [tests :as tests]]
            [jepsen.control.util :as cu]
            [jepsen.os.debian :as debian]))
            
(def dir     "/opt/neo4j")
            
(defn db
  "Neo4i DB for a particular version."
  [version]
  (reify db/DB
    (setup! [_ test node]
      (info node "installing neo4j" version)
      (c/su
        (let [url (str "https://dl.dropboxusercontent.com/s/jnw606op0lzk3hq/neo4j-community-" version "-unix.tar.gz")]
          (cu/install-archive! url dir))))
        
    (teardown! [_ test node]
      (info node "tearing down neo4j"))))
         
(defn neo4j-test
  "Given an options map from the command line runner (e.g. :nodes, :ssh,
  :concurrency ...), constructs a test map."
  [opts]
  (merge tests/noop-test
         opts
         {:name "neo4j"
          :os   debian/os
          :db   (db "4.4.6")
          :pure-generators true}))

(defn -main
  "Handles command line arguments. Can either run a test, or a web server for
  browsing results."
  [& args]
  (cli/run! (merge (cli/single-test-cmd {:test-fn neo4j-test})
                   (cli/serve-cmd))
            args))

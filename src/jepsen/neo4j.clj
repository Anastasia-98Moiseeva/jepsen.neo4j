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
(def binary  (str dir "/bin/neo4j"))
(def logfile (str dir "/neo4j.log"))
(def pidfile (str dir "/neo4j.pid"))

(defn node-url
  "An HTTP url for connecting to a node on a particular port."
  [node port]
  (str "http://" node ":" port))

(defn peer-url
  "The HTTP url for other peers to talk to a node."
  [node]
  (node-url node 2380))

(defn client-url
  "The HTTP url clients use to talk to a node."
  [node]
  (node-url node 2379))

(defn initial-cluster
  "Constructs an initial cluster string for a test, like
  \"foo=foo:2380,bar=bar:2380,...\""
  [test]
  (->> (:nodes test)
       (map (fn [node]
              (str node "=" (peer-url node))))
       (str/join ",")))
            
(defn db
  "Neo4i DB for a particular version."
  [version]
  (reify db/DB
    (setup! [_ test node]
      (info node "installing neo4j" version)
      (c/su
        (let [url (str "https://dl.dropboxusercontent.com/s/jnw606op0lzk3hq/neo4j-community-" version "-unix.tar.gz")]
          (cu/install-archive! url dir))

        (cu/start-daemon!
          {:logfile logfile
           :pidfile pidfile
           :chdir   dir}
          binary
          :--log-output :stderr
          :--name (name node)
          :--listen-peer-urls (peer-url node)
          :--listen-client-urls (client-url node)
          :--advertise-client-urls (client-url node)
          :--initial-cluster-state :new
          :--initial-advertise-peer-urls (peer-url node)
          :--initial-cluster (initial-cluster test))

        (Thread/sleep 10000)))

    (teardown! [_ test node]
      (info node "tearing down neo4j")
      (cu/stop-daemon! binary pidfile)
      (c/su (c/exec :rm :-rf dir))
      )

    db/LogFiles
    (log-files [_ test node]
      [logfile])))
         
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

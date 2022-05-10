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
(def admin  (str dir "/bin/neo4j-admin"))
(def cypher-shell  (str dir "/bin/cypher-shell"))
(def conf  (str dir "/conf/neo4j.conf"))
(def logfile (str dir "/neo4j.log"))
(def pidfile (str dir "/neo4j.pid"))

(defn uncomment-conf-prop
  [property]
  (c/exec :sed :-i "'s/#" property "/" property "/'" conf))

(defn db
  "Neo4i DB for a particular version."
  [version]
  (reify db/DB
    (setup! [_ test node]
      (c/su
        (info node "installing jdk and jre")
        (c/exec :apt :install :-y "default-jre")
        (c/exec :apt :install :-y "default-jdk")
        (info node "installing neo4j" version)
        (let [url (str "https://dl.dropboxusercontent.com/s/4ta0bluariw1z2k/neo4j-enterprise-" version "-unix.tar.gz")]
          (cu/install-archive! url dir))
        (let [auth-disable-property "dbms.security.auth_enabled=false"]
          (uncomment-conf-prop auth-disable-property))
        (cu/start-daemon!
          {:logfile logfile
           :pidfile pidfile
           :chdir   dir}
          binary
          :start)

        (Thread/sleep 10000)))

    (teardown! [_ test node]
      (info node "tearing down neo4j")
      (cu/stop-daemon! binary pidfile)
      (c/su (c/exec :rm :-rf dir)))

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


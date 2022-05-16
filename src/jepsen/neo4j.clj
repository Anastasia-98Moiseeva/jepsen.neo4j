(ns jepsen.neo4j
  (:require [clojure.tools.logging :refer :all]
            [jepsen [cli :as cli]
             [control :as c]
             [db :as db]
             [generator :as gen]
             [tests :as tests]]
            [jepsen.control.util :as cu]
            [jepsen.os.debian :as debian]
            [jepsen.neoclient])
  (:import (jepsen.neoclient Client)))

(def dir "/opt/neo4j")
(def binary (str dir "/bin/neo4j"))
(def conf (str dir "/conf/neo4j.conf"))
(def logfile (str dir "logs/neo4j.log"))

(defn r [_ _] {:type :invoke, :f :read, :value nil})
(defn w [_ _] {:type :invoke, :f :write, :value (rand-int 10)})

(defn db
  "Neo4i DB for a particular version."
  [version]
  (reify db/DB
    (setup! [_ test node]
      (info node "installing neo4j" version)
      (c/su
        (let [url (str "https://dl.dropboxusercontent.com/s/p0uoprfma140p4c/neo4j-community-" version "-unix.tar.gz")]
          (cu/install-archive! url dir))
        (c/exec* (str "cd " dir " && sed -i 's/#dbms.default_listen_address/dbms.default_listen_address/g' " conf))
        (c/exec* (str "cd " dir " && sed -i 's/#dbms.security.auth_enabled=false/dbms.security.auth_enabled=false/g' " conf))
        (info (c/exec binary "start")))
      (Thread/sleep 50000))

    (teardown! [_ test node]
      (info node "tearing down neo4j")
      (info (c/su (c/exec* (str dir "/bin/neo4j stop || true"))))
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
         {:name            "neo4j"
          :os              debian/os
          :db              (db "4.2.16")
          :pure-generators true
          :client          (Client. nil)
          :generator       (->> (gen/mix [r w])
                                (gen/stagger 1/50)
                                (gen/time-limit (:time-limit opts)))}))

(defn -main
  "Handles command line arguments. Can either run a test, or a web server for
  browsing results."
  [& args]
  (cli/run! (merge (cli/single-test-cmd {:test-fn neo4j-test})
                   (cli/serve-cmd))
            args))

(ns jepsen.neo4j
  (:require [clojure.tools.logging :refer :all]
            [jepsen [cli :as cli]
             [checker :as checker]
             [control :as c]
             [db :as db]
             [generator :as gen]
             [tests :as tests]]
            [jepsen.control.util :as cu]
            [jepsen.os.debian :as debian]
            [knossos.model :as model]
            [jepsen.linearizability :as lin]
            [jepsen.checker.timeline :as timeline])
  (:import (jepsen.linearizability Client)))

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

(def workloads
  "A map of workload names to functions that can take opts and construct
  workloads."
  {:linear lin/workload})

(def cli-opts
  "Additional command line options."
  [["-w" "--workload NAME" "Test workload to run"
    :default :linear
    :parse-fn keyword
    :validate [workloads (cli/one-of workloads)]]])

(defn neo4j-test
  "Given an options map from the command line runner (e.g. :nodes, :ssh,
  :concurrency ...), constructs a test map."
  [opts]
  (let [workload ((get workloads (:workload opts)) opts)
        gen (->> (:generator workload))]
    (merge tests/noop-test
           opts
           {:name            "neo4j"
            :os              debian/os
            :db              (db "4.2.16")
            :pure-generators true
            :client          (:client workload)
            :checker         (checker/compose
                               {:workload (:checker workload)
                                :timeline (timeline/html)})
            :generator       gen})))

(defn -main
  "Handles command line arguments. Can either run a test, or a web server for
  browsing results."
  [& args]
  (cli/run! (merge (cli/single-test-cmd {:test-fn  neo4j-test
                                         :opt-spec cli-opts})
                   (cli/serve-cmd))
            args))

(ns jepsen.neo4j
  (:require [clojure.tools.logging :refer :all]
            [clojure.string :as str]
            [jepsen [cli :as cli]
             [checker :as checker]
             [control :as c]
             [db :as db]
             [generator :as gen]
             [tests :as tests]]
            [jepsen.control.util :as cu]
            [jepsen.os.debian :as debian]
            [jepsen.neo4j [linearizability :as lin]
             [set :as set]
             [append :as app]
             [nemesis :as nemesis]]))

(def dir "/opt/neo4j")
(def binary (str dir "/bin/neo4j"))
(def conf (str dir "/conf/neo4j.conf"))
(def logfile (str dir "logs/neo4j.log"))

(defn node-ip-port
  [node]
  (str node ":5000"))

(defn initial-discovery-members
  [test]
  (->> (:nodes test)
       (map (fn [node]
              (node-ip-port node)))
       (str/join ",")))

(defn db
  "Neo4i DB for a particular version."
  [version]
  (reify db/DB
    (setup! [_ test node]
      (info node "installing neo4j" version)
      (c/su
        (let [url (str "https://dl.dropboxusercontent.com/s/4ta0bluariw1z2k/neo4j-enterprise-" version "-unix.tar.gz")]
          (cu/install-archive! url dir))
        (c/exec* (str "cd " dir " && sed -i 's/#dbms.mode/dbms.mode/g' " conf))
        (c/exec* (str "cd " dir " && sed -i 's/#dbms.default_listen_address/dbms.default_listen_address/g' " conf))
        (c/exec* (str "cd " dir " && sed -i 's/#causal_clustering.initial_discovery_members=localhost:5000,localhost:5001,localhost:5002/causal_clustering.initial_discovery_members=" (initial-discovery-members test) "/g' " conf))
        (c/exec* (str "cd " dir " && sed -i 's/#dbms.default_advertised_address=localhost/dbms.default_advertised_address=" node "/g' " conf))

        (c/exec* (str "cd " dir " && sed -i 's/#dbms.security.auth_enabled=false/dbms.security.auth_enabled=false/g' " conf))

        (c/exec* (str "cd " dir " && sed -i 's/#dbms.routing.enabled=false/dbms.routing.enabled=true/g' " conf))
        (c/exec* (str "cd " dir " && sed -i 's/#dbms.routing.listen_address/dbms.routing.listen_address/g' " conf))
        (c/exec* (str "cd " dir " && sed -i 's/#dbms.routing.advertised_address=:7688/dbms.routing.advertised_address=" node ":7688/g' " conf))
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

(def special-nemeses
  "A map of special nemesis names to collections of faults"
  {:none []
   :all  [:pause :kill :partition :clock :member]})

(defn parse-nemesis-spec
  "Takes a comma-separated nemesis string and returns a collection of keyword
  faults."
  [spec]
  (->> (str/split spec #",")
       (map keyword)
       (mapcat #(get special-nemeses % [%]))))

(def workloads
  "A map of workload names to functions that can take opts and construct
  workloads."
  {:linear lin/workload
   :set    set/workload
   :append app/workload})

(def cli-opts
  "Additional command line options."
  [["-w" "--workload NAME" "Test workload to run"
    :default :linear
    :parse-fn keyword
    :validate [workloads (cli/one-of workloads)]]

   [nil "--nemesis FAULTS" "A comma-separated list of nemesis faults to enable"
    :parse-fn parse-nemesis-spec
    :validate [(partial every? #{:pause :kill :partition :clock :member})
               "Faults must be pause, kill, partition, clock, or member, or the special faults all or none."]]

   [nil "--nemesis-interval SECS" "Roughly how long between nemesis operations."
    :default 2
    :parse-fn read-string
    :validate [pos? "Must be a positive integer."]]]
  )

(defn neo4j-test
  "Given an options map from the command line runner (e.g. :nodes, :ssh,
  :concurrency ...), constructs a test map."
  [opts]
  (let [workload ((workloads (:workload opts)) opts)
        nemesis (nemesis/nemesis-package
                  {:db       db
                   :nodes    (:nodes opts)
                   :faults   (:nemesis opts)
                   :partition {:targets [:primaries]}
                   :pause    {:targets [nil :one :primaries :majority :all]}
                   :kill     {:targets [nil :one :primaries :majority :all]}
                   :interval (:nemesis-interval opts)})
        gen (->> (:generator workload)
                 (gen/nemesis (:generator nemesis))
                 (gen/time-limit (:time-limit opts)))
        gen (if (:final-generator workload)
              (gen/phases gen
                          (gen/clients (:final-generator workload)))
              gen)]

    (merge tests/noop-test
           opts
           (dissoc workload :final-generator)
           {:name            "neo4j"
            :os              debian/os
            :db              (db "4.2.16")
            :pure-generators true
            :checker         (checker/compose
                               {:perf       (checker/perf
                                              {:nemeses (:perf nemesis)})
                                :clock      (checker/clock-plot)
                                :stats      (checker/stats)
                                :exceptions (checker/unhandled-exceptions)
                                :workload   (:checker workload)})
            :client          (:client workload)
            :nemesis         (:nemesis nemesis)
            :generator       gen})))

(defn -main
  "Handles command line arguments. Can either run a test, or a web server for
  browsing results."
  [& args]
  (cli/run! (merge (cli/single-test-cmd {:test-fn  neo4j-test
                                         :opt-spec cli-opts})
                   (cli/serve-cmd))
            args))

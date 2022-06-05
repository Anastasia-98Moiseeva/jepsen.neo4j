(ns jepsen.neo4j.linearizability
  (:require [neo4clj.client :as nc]
            [jepsen [client :as client]
             [checker :as checker]
             [generator :as gen]]
            [knossos.model :as model]
            [clojure.tools.logging :refer :all]))

(defrecord Client [conn]
  client/Client
  (open! [this test node]
    (assoc this :conn (nc/connect (str "neo4j://" node ":7687") "neo4j" "pas")))

  (setup! [this test]
    (info (nc/create-node! conn {:ref-id "p"
                                 :labels [:person]
                                 :props  {:name "Alice"}})))

  (invoke! [_ test op]
    (case (:f op)
      :read (assoc op
              :type :ok,
              :value (get-in (nc/find-node conn {:ref-id "n"
                                                 :props  {:name "Alice"}}) [:props :rating]))
      :write (do
               (nc/update-props! conn {:props {:name "Alice"}} {:rating (:value op)})
               (assoc op :type :ok))
      )
    )

  (teardown! [this test])

  (close! [_ test]
    (nc/disconnect conn)))

(defn r [_ _] {:type :invoke, :f :read, :value nil})
(defn w [_ _] {:type :invoke, :f :write, :value (rand-int 10)})

(defn workload
  "Stuff you need to build a test!"
  [opts]
  {:client    (Client. nil)
   :generator (->> (gen/mix [r w])
                   (gen/stagger 1/50))
   :checker   (checker/linearizable
                {:model     (model/register)
                 :algorithm :linear})})

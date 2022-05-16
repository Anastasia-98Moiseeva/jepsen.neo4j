(ns jepsen.neoclient
  (:require [neo4clj.client :as nc]
            [jepsen [client :as client]]
            [clojure.tools.logging :refer :all]))

(defrecord Client [conn]
  client/Client
  (open! [this test node]
    (assoc this :conn (nc/connect (str "neo4j://192.168.0.103:7687") "neo4j" "pas")))

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

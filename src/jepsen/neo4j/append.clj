(ns jepsen.neo4j.append
  (:require [neo4clj.client :as nc]
            [jepsen [client :as client]]
            [jepsen.tests.cycle.append :as append]
            [clojure.tools.logging :refer :all]))

(defn mop!
  "Executes a transactional micro-op on a connection."
  [conn test transaction [f k v :as mop]]
  (info :mop mop)
  (case f
    :r (vec (get-in (nc/find-node transaction {:ref-id "n"
                                               :labels [:person]
                                               :props  {:name k}}) [:props :rating]))
    :append (let [node (nc/find-node transaction {:ref-id "p"
                                                  :labels [:person]
                                                  :props  {:name k}})
                  items (vec (get-in node [:props :rating]))]

              (info "append operation: " (if (= node nil)
                             (nc/create-node! transaction {:labels [:person] :props {:name k :rating (vec (conj [] v))}})
                             (nc/update-props! transaction {:ref-id "z"
                                                            :labels [:person]
                                                            :props  {:name k}} {:rating (vec (conj items v))}
                                               )))
              )
    ))

(defrecord Client [conn]
  client/Client
  (open! [this test node]
    (assoc this :conn (nc/connect (str "neo4j://" node ":7687") "neo4j" "pas")))

  (setup! [this test])

  (invoke! [_ test op]
    (let [txn (:value op)
          txn' (nc/with-transaction conn tx
                                    (mapv (partial mop! conn test tx) txn))]
      (info "txn': " txn')
      (info "txn: " txn)
      (assoc op :type :ok, :value (vec (for [i (range 0 (count txn))]
                                         (if (= (get (get txn i) 0) :r)
                                           (assoc (get txn i) 2 (get txn' i))
                                           (get txn i))))))
    )

  (teardown! [this test])

  (close! [_ test]
    (nc/disconnect conn)))


(defn workload
  "Stuff you need to build a test!"
  [opts]
  (-> (append/test {:key-count          3
                    :max-txn-length     3
                    :max-writes-per-key 3
                    })
      (assoc :client (Client. nil))
      ))

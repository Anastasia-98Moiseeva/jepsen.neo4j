(ns jepsen.append
  (:require [neo4clj.client :as nc]
            [jepsen [client :as client]
             [checker :as checker]
             [generator :as gen]]
    ;[knossos.model :as model]
            [jepsen.tests.cycle.append :as append]
            [clojure.tools.logging :refer :all]
            [clojure.set :as set]))

(defn mop!
  "Executes a transactional micro-op on a connection."
  [conn test transaction [f k v :as mop]]
  (info :mop mop)
  (case f
    :r (get-in (nc/find-node transaction {:ref-id "n"
                                          :id     k
                                          :props  {:name "Alice"}}) [:props :rating])
    ; (get-in (nc/find-node transaction {:ref-id "n" :id     1 :labels [:person]}) [:props :rating])
    :append (let [items (get-in (nc/find-node transaction {:ref-id "n"
                                                           :id     k
                                                           :props  {:name "Alice"}}) [:props :rating])]
              (info "iiiiiiiiii" (vec items))
              (nc/update-props! transaction {:id k :props {:name "Alice"}} {:rating (vec (conj (vec items) (str v)))}))
    ;(do (nc/update-props! transaction {:id     1 :labels [:person]} {:rating v}))
    ))

(defrecord Client [conn]
  client/Client
  (open! [this test node]
    (assoc this :conn (nc/connect (str "neo4j://192.168.1.140:7687") "neo4j" "pas")))

  (setup! [this test]
    (info (nc/create-node! conn {:ref-id "p"
                                 :labels [:person]
                                 :props  {:name "Alice"}}))
    ;(info (nc/create-node! conn {:ref-id "p" :id     1 :labels [:person]}))
    )

  (invoke! [_ test op]
    (info "tttttttttttt" op)
    (let [txn (:value op)
          txn' (nc/with-transaction conn tx
                                    (mapv (partial mop! conn test tx) txn))
          val (:value op)]
      (info "rrrrreeeeeessssss" (assoc op :type :ok, :value (vec (for [i (range 0 (count val))] (if (= (get (get val i) 0) :r) (assoc (get val i) 2 (get txn' i)) (get val i))))))
      (assoc op :type :ok, :value (vec (for [i (range 0 (count val))]
                                         (if (= (get (get val i) 0) :r)
                                           (assoc (get val i) 2 (get txn' i))
                                           (get val i))))))
    )

  (teardown! [this test])

  (close! [_ test]
    (nc/disconnect conn)))


(defn workload
  "Stuff you need to build a test!"
  [opts]
  (-> (append/test {:key-count          7
                    :max-txn-length     3
                    :max-writes-per-key 3
                    :anomalies          [:G0, :G1a, :G1b, :G1c, :G1 :G2]})
      (assoc :client (Client. nil))
      ))

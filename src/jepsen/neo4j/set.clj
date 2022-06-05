(ns jepsen.neo4j.set
  (:require [neo4clj.client :as nc]
            [jepsen [client :as client]
             [checker :as checker]
             [generator :as gen]]
            [clojure.tools.logging :refer :all]))


(defrecord Client [conn]
  client/Client
  (open! [this test node]
    (assoc this :conn (nc/connect (str "neo4j://" node ":7687") "neo4j" "pas")))

  (setup! [this test])

  (invoke! [_ test op]
    (case (:f op)
      :add (do
             (nc/create-node! conn {:ref-id "e"
                                    :labels [:person]
                                    :props  {:rating (:value op)}})
             (assoc op :type :ok))
      :read
      (let [all-nodes (nc/find-nodes conn {:ref-id "n" :labels [:person]})]
        (assoc op
          :type :ok,
          :value (for [i (apply vector all-nodes)] (get-in i [:props :rating])))))
    )

  (teardown! [this test])

  (close! [_ test]
    (nc/disconnect conn)))

(defn workload
  "Stuff you need to build a test!"
  [opts]
  {:client          (Client. nil)
   :checker         (checker/set)
   :generator       (->> (range)
                         (map (fn [i] {:type :invoke, :f :add, :value i}))
                         (gen/stagger 1/10))
   :final-generator (gen/each-thread {:type :invoke, :f :read})})

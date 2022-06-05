(ns jepsen.neo4j.nemesis
  (:require [jepsen.nemesis [combined :as nc]]))

(defn nemesis-package
  "Constructs a nemesis and generators."
  [opts]
  (let [opts (update opts :faults set)]
    (nc/nemesis-package opts)))

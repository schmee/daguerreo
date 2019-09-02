(ns ^:no-doc daguerreo.impl.graph
  (:require [clojure.set :as set]))

(defprotocol ITaskGraph
  (dependencies [graph node])
  (dependents [graph node])
  (nodes [graph])
  (find-cycle [graph]))

(defrecord Node [label path])

(defrecord TaskGraph [nodes in out]
  ITaskGraph
  (dependencies [graph node]
    (out node))

  (dependents [graph node]
    (in node))

  (nodes [_]
    nodes)

  (find-cycle [{:keys [nodes] :as graph}]
    (loop [frontier []
           unvisited nodes]
      (if (seq frontier)
        (let [{:keys [label path]} (first frontier)
              children (dependents graph label)
              on-path (set/intersection (set path) children)]
          (if (empty? on-path)
            (recur (into (rest frontier)
                         (eduction (map #(->Node % (conj path label)))
                                   (filter unvisited children)))
                   (disj unvisited label))
            (into (conj path label) on-path)))
        (if (empty? unvisited)
          nil
          (let [label (first unvisited)]
            (recur [(->Node label [])]
                   (disj unvisited label))))))))

(defn tasks->graph [tasks]
  (map->TaskGraph
    (reduce
      (fn [acc {:keys [name dependencies] :as task}]
        (cond-> (reduce
                  (fn [acc dep]
                    (update-in acc [:in dep] (fnil conj #{}) name))
                  acc
                  dependencies)
          dependencies (assoc-in [:out name] dependencies)
          true (update-in [:nodes] conj name)))
      {:in {}
       :out {}
       :nodes #{}}
      tasks)))

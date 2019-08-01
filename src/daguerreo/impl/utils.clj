(ns daguerreo.impl.utils
  (:require [loom.graph :as graph]))

(defn task->node [task]
  (let [{:keys [name :dependencies]} task]
    (if (seq dependencies)
      (map vector dependencies (repeat name))
      [name])))

(defn tasks->graph [tasks]
  (apply graph/digraph (mapcat task->node tasks)))

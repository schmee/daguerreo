(ns daguerreo.unit.graph-test
  (:require [clojure.test :refer :all]
            [daguerreo.impl.graph :as graph]))

(defn f [_ _])

(defn base-task [tasks]
  (mapv #(merge {:fn f} %) tasks))

(defn find-cycle [tasks]
  (-> tasks
      base-task
      graph/tasks->graph
      graph/find-cycle))

(deftest graph
  (let [graph (graph/tasks->graph [{:name :a}
                                   {:name :b :dependencies #{:a}}
                                   {:name :c :dependencies #{:b :d}}
                                   {:name :d :dependencies #{:b}}])]
    (is (= {:in  {:a #{:b} :b #{:c :d} :d #{:c}}
            :out {:b #{:a} :c #{:b :d} :d #{:b}}
            :nodes #{:a :b :c :d}}
           (into {} graph)))

    (is (= #{:b}
           (graph/dependencies graph :d)))

    (is (= #{:c}
           (graph/dependents graph :d)))))

(deftest cycles
  (is (nil? (find-cycle [{:name :a :dependencies #{:b}}
                         {:name :b :dependencies #{}}])))

  (is (= [:b :d :b]
         (find-cycle [{:name :a}
                      {:name :b :dependencies #{:a :d}}
                      {:name :c :dependencies #{:b}}
                      {:name :d :dependencies #{:b}}])))

  (is (= [:c :b :a :c]
         (find-cycle [{:name :a :dependencies #{:b}}
                      {:name :b :dependencies #{:c}}
                      {:name :c :dependencies #{:a}}])))

  (is (= [:e :c :b :a :e]
         (find-cycle [{:name :a :dependencies #{:b}}
                      {:name :b :dependencies #{:c}}
                      {:name :c :dependencies #{:e}}
                      {:name :d}
                      {:name :e :dependencies #{:a}}
                      {:name :f :dependencies #{:c :e}}
                      {:name :g}
                      {:name :x1}
                      {:name :x2 :dependencies #{:x1}}]))))

(comment
  (ns daguerreo.unit.graph-test)
  (graph)
  (cycles))

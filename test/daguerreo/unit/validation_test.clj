(ns daguerreo.unit.validation-test
  (:require [daguerreo.impl.validation :refer :all :as v]
            [daguerreo.impl.graph :as graph]
            [clojure.test :refer :all]))

(defn base-task [tasks]
  (mapv #(merge {:fn (fn [ctx _])} %) tasks))

(defn validate [& tasks]
  (let [mapped (base-task tasks)]
    (validate-tasks mapped (graph/tasks->graph mapped))))

(deftest validation
  (is (= [{:name :a :error ::v/missing-dependencies :missing #{:b}}]
         (validate {:name :a :dependencies #{:b}})))


  (is (= [{:error ::v/continue-on-failure-with-dependent-tasks :dependents #{:a} :name :b}]
         (validate {:name :a :dependencies #{:b}}
                   {:name :b :continue-on-failure? true})))

  (is (= [{:error ::v/dependency-cycle :cycle [:b :a :b]}]
         (validate {:name :a :dependencies #{:b}}
                   {:name :b :dependencies #{:a}})))

  (is (= [{:error ::v/dependency-cycle :cycle [:c :b :a :c]}]
         (validate {:name :a :dependencies #{:b}}
                   {:name :b :dependencies #{:c}}
                   {:name :c :dependencies #{:a}}))))

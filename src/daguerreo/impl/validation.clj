(ns ^:no-doc daguerreo.impl.validation
  (:require [clojure.spec.alpha :as s]
            [expound.alpha :as expound]
            [loom.alg :as alg]
            [loom.graph :as graph]

            [daguerreo.impl.utils :as utils]
            [daguerreo.impl.specs]))

(defn valid-spec? [task]
  (when-not (s/valid? :daguerreo/task task)
    (merge
      (s/explain-data :daguerreo/task task)
      {:name (:name task)
       :error ::spec-validation-failure})))

(defn contains-cycles? [tasks task-graph]
  (when-not (some? (alg/topsort task-graph))
    [{:error ::dependency-cycle}]))

(defn continue-on-failure-with-deps? [task task-graph]
  (let [dependents (graph/successors task-graph (:name task))]
    (when (and (:continue-on-failure? task) (seq dependents))
      {:error ::continue-on-failure-with-dependent-tasks
       :name (:name task)
       :dependents dependents})))

(defn deps-exists? [task tasks]
  (let [missing (clojure.set/difference (-> task :dependencies set)
                                        (->> tasks (map :name) set))]
    (when (seq missing)
      {:name (:name task)
       :error ::missing-dependencies
       :missing missing})))

(defn validate-tasks [tasks task-graph]
  (into []
        (comp cat (remove nil?))
        [(map valid-spec? tasks)
         (map #(continue-on-failure-with-deps? % task-graph) tasks)
         (map #(deps-exists? % tasks) tasks)
         (contains-cycles? tasks task-graph)]))

(defmulti format-error :error)

(defmethod format-error ::spec-validation-failure [error]
  (str "Task " (:name error) " -" (expound/expound-str :daguerreo/task error)))

(defmethod format-error ::missing-dependencies [error]
  (str "Task " (:name error) " - missing dependencies\nMissing: " (::missing error)))

(defmethod format-error ::continue-on-failure-with-dependent-tasks [error]
  (str "Task " (:name error) " - `continue-on-failure?` with dependent tasks\n"
       "Dependents: " (:dependents error)))

(defn validate-and-report
  ([tasks]
   (validate-and-report tasks (utils/tasks->graph tasks)))
  ([tasks task-graph]
   (let [errors (validate-tasks tasks task-graph)]
     (when (seq errors)
       (println (str (count errors) " error(s)"))
       (doseq [error errors]
        (println (format-error error))
        (println "------------------------------------------------------"))
       (throw (ex-info "Invalid tasks" {}))))))

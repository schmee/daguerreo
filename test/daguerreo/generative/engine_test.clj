(ns daguerreo.generative.engine-test
  (:require [clojure.core.async :as a]
            [clojure.spec.alpha :as s]
            [clojure.test.check :as tc]
            [clojure.test.check.generators :as gen]

            [daguerreo.core :as d]
            [daguerreo.helpers :as helpers]
            [daguerreo.impl.engine :as engine]
            [clojure.test :refer :all]))

(defn create-tap [mult]
  (as-> (a/chan 100) $
    (a/tap mult $)
    $))

(defn int->keyword [i]
  (-> i str keyword))

(defn tasks-with-valid-deps [n-tasks]
  (let [is (range n-tasks)]
    (for [i is :let [cands (vec (range (inc i) n-tasks))
                     deps (set (take (rand-int (min 6 (count cands)))
                                     (repeatedly #(int->keyword (rand-nth cands)))))]]
      (vector (int->keyword i) deps))))

(defn task-gen [n-tasks max-sleep]
  (gen/let [base (s/gen :daguerreo/task)
              max-retries (gen/choose 0 10)
              timeout (if (zero? max-sleep)
                        (gen/return 1000)
                        (gen/choose (max 10 (int (/ max-sleep 1.5)))
                                    (int (* max-sleep 2))))]
    (-> base
        (assoc :timeout timeout)
        (assoc :max-retries 3)
        (assoc :continue-on-failure? false))))

(defn has-dep-data? [ctx deps]
  (every? (partial contains? ctx) (map ctx deps)))

(defn task-set [n-tasks max-sleep ctx-check-chan]
  (gen/fmap
    (fn [tasks]
      (mapv
        (fn [task [name deps]]
          (let [task (assoc task :name name :dependencies deps)
                task-fn (fn [ctx _]
                          (when-not (has-dep-data? ctx deps)
                            (a/put! ctx-check-chan {:tag :deps-not-ready
                                                    :task task
                                                    :ctx ctx}))
                          (if (> (rand) (- 1 0.2))
                            (throw (Exception. "BOOM"))
                            (do
                              (when-not (zero? max-sleep)
                                (Thread/sleep (rand-int max-sleep)))
                              {name name})))]
            (assoc task :fn task-fn)))
        tasks
        (tasks-with-valid-deps n-tasks)))
    (gen/vector (task-gen n-tasks max-sleep) n-tasks)))

(def task-state-transitions
  {:task.state/exception #{:task.state/failed :task.state/ready}
   :task.state/ready #{:task.state/running}
   :task.state/running #{:task.state/cancelled :task.state/completed :task.state/exception :task.state/timed-out}
   :task.state/timed-out #{:task.state/failed :task.state/ready}
   :task.state/unscheduled #{:task.state/ready}})

(defn valid-state-transition? [event]
  (let [{:daguerreo.task/keys [old-state new-state]} event]
    (contains? (task-state-transitions old-state) new-state)))

(defn state-transition-validator [event-tap error-chan]
  (engine/go-loop-bcond []
    :when-let [{:daguerreo/keys [event-type]
                :daguerreo.task/keys [name]
                :as event} (a/<! event-tap)]

    (and (= event-type :task.event/state-transition)
         (not (valid-state-transition? event)))
    (do
      (a/>! error-chan (assoc event :tag :state-transition-validator))
      (recur))

    :else (recur)))

(defn one-time-state-validator [event-tap error-chan]
  (engine/go-loop-bcond [tasks {}]
    :when-let [{:daguerreo/keys [event-type]
                :daguerreo.task/keys [name new-state]
                :as event} (a/<! event-tap)]

    (or
      (engine/check-state-transition event :task.state/unscheduled nil)
      (engine/check-state-transition event nil :task.state/failed)
      (engine/check-state-transition event nil :task.state/completed))
    (if (contains? (tasks name) new-state)
      (do
        (a/>! error-chan (assoc event :tag :one-time-state-validator))
        (recur tasks))
      (recur (update tasks name (fnil conj #{}) new-state)))

    :else (recur tasks)))

(defn state-consistency-validator [event-tap error-chan]
  (engine/go-loop-bcond [last-state {}]
    :when-let [{:daguerreo/keys [event-type]
                :daguerreo.task/keys [name old-state new-state]
                :as event} (a/<! event-tap)]

    (and (last-state name) (not= (last-state name) old-state))
    (do
      (a/>! error-chan (assoc event :tag :state-consistency-validator
                                    :real-old (last-state name)))
      (recur (assoc last-state name new-state)))

    :else (recur (assoc last-state name new-state))))

(defn error-collector [errors error-chan ctx-check-chan job]
  (engine/go-loop-bcond []
    :let [[event c] (a/alts! [error-chan ctx-check-chan])]

    (and (= c error-chan) (nil? event))
    (comment "exit loop")

    :else (do
            (println "ERROR " event)
            (swap! errors conj event)
            (a/close! error-chan)
            (a/close! ctx-check-chan)
            (engine/cancel job))))

(defn generate-task-set [{:keys [n-tasks-sets n-tasks max-sleep ctx-check-chan]}]
  (gen/sample (task-set n-tasks max-sleep ctx-check-chan) n-tasks-sets))

(defn run-tasks-with-validation [tasks ctx-check-chan]
  (let [event-chan (a/chan 100)
        error-chan (a/chan 100)
        ec-mult (a/mult event-chan)
        errors (atom [])]
    (state-transition-validator (create-tap ec-mult) error-chan)
    (state-consistency-validator (create-tap ec-mult) error-chan)
    (one-time-state-validator (create-tap ec-mult) error-chan)
    (let [job (engine/run tasks {:event-chan (helpers/event-logger)})]
      (error-collector errors error-chan ctx-check-chan job)
      {:job job
       :errors errors})))

(def N-TASK-SETS 20)

(def task-sizes {:small 10
                 :medium 50
                 :large 500
                 :huge 1000})

(def task-sleeps {:instant 0
                  :hyper 100
                  :fast 500
                  :medium 1000
                  :slow 2000})

(defn run-task-set-with-validation [size sleep]
  (let [ctx-check-chan (a/chan 100)
        task-set (generate-task-set {:n-tasks-sets N-TASK-SETS
                                     :ctx-check-chan ctx-check-chan
                                     :max-sleep (task-sleeps sleep)
                                     :n-tasks (task-sizes size)})]
    (doseq [tasks task-set]
      (let [{:keys [job errors] :as job} (run-tasks-with-validation tasks ctx-check-chan)]
        (is (empty? @errors))
        (is (seq @job))))))

(defmacro defgen
 ([size sleep]
  `(defgen ~size ~sleep nil))
 ([size sleep tag]
  (let [test-name (symbol (clojure.string/join
                            "-"
                            ["check-invariants" (name size) (name sleep)]))]
    `(deftest ~(if tag (vary-meta test-name assoc tag true) test-name)
       (run-task-set-with-validation ~size ~sleep)))))

(defgen :huge :hyper)
(defgen :huge :instant)
(defgen :large :hyper)
(defgen :large :instant)
(defgen :medium :fast)
(defgen :medium :hyper)
(defgen :medium :instant)
(defgen :small :fast)
(defgen :small :hyper)
(defgen :small :instant)
(defgen :small :medium)
(defgen :small :slow)

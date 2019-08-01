(ns daguerreo.unit.core-test
  (:require [clojure.core.async :as a]
            [clojure.test :refer :all]
            [matcher-combinators.test]

            [daguerreo.core :as core]
            [daguerreo.helpers :as helpers])
  (:import [java.util.concurrent Executors]))

(defn run
  ([tasks]
   (run tasks {}))
  ([tasks {:keys [event-chan] :as opts}]
   (let [opts (if event-chan
                opts
                (assoc opts :event-chan (helpers/event-logger)))
         ec (:event-chan opts)]
     ;; Timeout to prevent the REPL from blocking if something goes wrong
     (a/go
       (a/<! (a/timeout 3000))
       (a/close! ec))
     (deref (core/run tasks opts) 3000 nil))))

(defn incer [ctx _]
  {:v (inc (:v ctx))})

(def the-ex (ex-info "BOOM" {:some "message"}))

(defn thrower [ctx _]
  (throw the-ex))

(deftest ctx
  (let [tasks [{:name :a :fn incer :dependencies #{:b}}
               {:name :b :fn incer :dependencies #{:c}}
               {:name :c :fn incer}]]
    (is (= {:daguerreo.job/state :job.state/completed :v 3}
           (run tasks {:ctx {:v 0}})))))

(deftest exception
  (let [tasks [{:name :a :fn incer}
               {:name :b :fn thrower}]
        ctx (run tasks {:ctx {:v 0}})]
    (is (match? {:daguerreo.job/state :job.state/failed
                 :daguerreo.job/failure-reason :task.state/exception
                 :daguerreo.task/name :b
                 :v 1}
                ctx))
    (is (identical? the-ex (:daguerreo.task/exception ctx)))))

(deftest max-retries
  (let [n-runs (atom 0)
        tasks [{:name :a
                :fn (fn [ctx _]
                      (swap! n-runs inc)
                      (thrower ctx nil))}
               {:name :b :fn incer}]
        max-retries 3
        ctx (run tasks {:ctx {:v 0} :max-retries max-retries})]
    (is (= @n-runs (inc max-retries)))
    (is (match? {:daguerreo.job/state :job.state/failed
                 :daguerreo.job/failure-reason :task.state/exception
                 :daguerreo.task/name :a
                 :v 1}
                ctx))
    (is (identical? the-ex (:daguerreo.task/exception ctx)))))

(deftest max-concurrency
  (let [event-chan (a/chan 1 (filter #(= (:daguerreo/event-type %)
                                         :task.event/state-transition)))
        events-chan (a/go-loop [events []]
                     (if-let [event (a/<! event-chan)]
                       (recur (conj events event))
                       events))]
    (let [tasks [{:name :a :fn incer}
                 {:name :b :fn incer}
                 {:name :c :fn incer}]
          ctx (run tasks {:ctx {:v 0} :max-concurrency 1 :event-chan event-chan})
          events (a/<!! events-chan)]
      (is (= {:daguerreo.job/state :job.state/completed :v 3} ctx))
      (is (match? [{:daguerreo.task/new-state :task.state/ready}
                   {:daguerreo.task/new-state :task.state/ready}
                   {:daguerreo.task/new-state :task.state/ready}
                   {:daguerreo.task/new-state :task.state/running}
                   {:daguerreo.task/new-state :task.state/completed}
                   {:daguerreo.task/new-state :task.state/running}
                   {:daguerreo.task/new-state :task.state/completed}
                   {:daguerreo.task/new-state :task.state/running}
                   {:daguerreo.task/new-state :task.state/completed}]
                  events)))))

(defn looper [ch]
  (fn [_ is-active?]
   (loop [i 0]
     (if (is-active?)
       (do
         (Thread/sleep 200)
         (when (= i 0)
           (a/put! ch :one-loop))
         (recur (inc i)))
       (a/close! ch)))))

(deftest cancellation
  (let [ch (a/chan)
        tasks [{:name :a :fn (looper ch)}
               {:name :b :fn incer}]
        job (core/run tasks {:ctx {:v 0}})]
    (is (= :one-loop (a/<!! ch)))
    (core/cancel job)
    (is (nil? (a/<!! ch)))
    (is (= {:daguerreo.job/state :job.state/cancelled
            :v 1}
           @job))))

(deftest invalid-result
  (let [ch (a/chan)
        tasks [{:name :a :fn (fn [_ _] :not-a-map)}]]
    (is (match? {:daguerreo.job/state :job.state/failed
                 :daguerreo.job/failure-reason :task.state/invalid-result
                 :daguerreo.task/name :a
                 :daguerreo.task/result :not-a-map}
                (run tasks)))))

(deftest timeout
  (let [ch (a/chan)
        timeout 50
        tasks [{:name :a
                :fn (fn [_ _] (Thread/sleep (* timeout 2)) {:a 1})
                :timeout timeout}]]
    (is (match? {:daguerreo.job/state :job.state/failed
                 :daguerreo.job/failure-reason :task.state/timed-out
                 :daguerreo.task/name :a}
                (run tasks {:max-retries 3})))))

(deftest custom-executor
  (let [ch (a/chan)
        executor (Executors/newSingleThreadExecutor)
        tasks [{:name :a :fn (fn [_ _] {:a 1})}
               {:name :b :fn (fn [_ _] {:b 2})}]]

    (is (match? {:daguerreo.job/state :job.state/completed
                 :a 1
                 :b 2}
                (run tasks {:ctx {:v 0 :executor executor}})))))

(deftest global-timeout
  (let [timeout 500
        tasks [{:name :a :fn (fn [_ _] {:a 1})}
               {:name :b :fn (fn [_ _] (Thread/sleep (* timeout 2)) {:b 1})}]]
    (is (= {:daguerreo.job/state :job.state/timed-out
            :a 1}
           (run tasks {:timeout timeout})))))

(comment
  (ns daguerreo.unit.core-test)
  (run-tests))

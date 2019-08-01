(ns daguerreo.impl.specs
  (:require [clojure.spec.alpha :as s]
            [clojure.spec.gen.alpha :as gen]
            [clojure.test.check.generators :as tcgen])
  (:import [java.util.concurrent Executor]))

(s/def :daguerreo.task/fn (s/with-gen fn? (fn [] (gen/return (fn [ctx is-active?])))))
(s/def :daguerreo.task/name keyword?)
(s/def :daguerreo.task/continue-on-failure? boolean?)
(s/def :daguerreo.task/dependencies (s/coll-of :daguerreo.task/name :kind set?))
(s/def :daguerreo.task/max-retries nat-int?)
(s/def :daguerreo.task/timeout (s/and pos-int? #(>= % 50)))

(s/def :daguerreo/task
  (s/keys :req-un [:daguerreo.task/fn
                   :daguerreo.task/name]
          :opt-un [:daguerreo.task/continue-on-failure?
                   :daguerreo.task/dependencies
                   :daguerreo.task/max-retries
                   :daguerreo.task/timeout]))

(s/def :daguerreo/tasks
  (s/+ :daguerreo/task))

(s/def :daguerreo.task/state
  #{:task.state/completed
    :task.state/exception
    :task.state/failed
    :task.state/invalid-result
    :task.state/ready
    :task.state/running
    :task.state/timed-out
    :task.state/unscheduled})

(s/def :daguerreo.task/old-state :daguerreo.task/state)
(s/def :daguerreo.task/new-state :daguerreo.task/state)
(s/def :daguerreo.task/result map?)
(s/def :daguerreo.task/exception (s/with-gen #(instance? Exception %) #(s/gen #{(Exception.)})))

(s/def :daguerreo.job/job-state
  #{:job.state/idle
    :job.state/cancelled
    :job.state/completed
    :job.state/failed
    :job.state/running
    :job.state/timed-out})

(s/def :daguerreo.job/old-state :daguerreo.job/job-state)
(s/def :daguerreo.job/new-state :daguerreo.job/job-state)

(s/def :daguerreo/event-type
  #{:task.event/state-transition
    :job.event/state-transition})

(s/def :daguerreo/base-event
  (s/keys :req [:daguerreo/event-type]))

(defmulti event-type :daguerreo/event-type)

(defmethod event-type :job.event/state-transition [_]
  (s/merge :daguerreo/base-event
           (s/keys :req [:daguerreo.job/old-state
                         :daguerreo.job/new-state])))

(defmethod event-type :task.event/state-transition [_]
  (s/merge :daguerreo/base-event
           (s/keys :req [:daguerreo.task/old-state
                         :daguerreo.task/new-state]
                   :opt [:daguerreo.task/result
                         :daguerreo.task/exception])))

(s/def :daguerreo/event (s/multi-spec event-type :daguerreo/event-type))

(s/def :daguerreo.core/event-chan #(instance? clojure.core.async.impl.channels.ManyToManyChannel %))
(s/def :daguerreo.core/max-concurrency pos-int?)
(s/def :daguerreo.core/task-thread-pool #(instance? Executor %))
(s/def :daguerreo.core/max-retries nat-int?)
(s/def :daguerreo.core/timeout (s/and pos-int? #(>= % 50)))
(s/def :daguerreo.core/run-opts
  (s/keys :opt-un [:daguerreo.core/event-chan
                   :daguerreo.core/max-concurrency
                   :daguerreo.core/max-retries
                   :daguerreo.core/executor
                   :daguerreo.core/timeout]))

(ns daguerreo.core
  (:require [clojure.spec.alpha :as s]
            [daguerreo.impl.engine :as engine]
            [daguerreo.impl.validation :as validation]
            [daguerreo.impl.specs]))

(s/fdef validate-tasks
  :args (s/cat :tasks :daguerreo/tasks))

(defn validate-tasks
  "Validates the integrity of a sequence of tasks, to ensure that there are no cycles, that all dependencies are present etc. If any errors are found, raises an exception and prints an error
   message explaining the cause of the error(s)."
  [tasks]
  (validation/validate-and-report tasks))

(defn cancel
  "Cancels a job. When cancelled, the job is immediately realized with the context of all tasks that were completed and with a `:daugerreo.job/status` of `:job.status/cancelled`.
   Any running tasks will be cancelled and any unscheduled tasks will be skipped."
  [job]
  (engine/cancel job))

(s/fdef run
  :args (s/alt :tasks
               (s/cat :tasks :daguerreo/tasks)

               :tasks+opts
               (s/cat :tasks :daguerreo/tasks
                      :opts :daguerreo.core/run-opts)))

(defn run
  "Runs a set of tasks and returns a \"job\", a derefferable that when dereffed returns the resulting map after the job has reached a terminal state.

  The resulting map will always contain `:daguerreo.job/status` (see specs for the set of possible values of this key).

  Validates the tasks before running them via `validate-tasks`.

  `opts` is a map that can contain the following keys:
  - `:event-chan` - a core.async channel that will receive all the events sent from the scheduler. Will be closed when the job reaches a terminal state. NOTE: if this channel blocks, it will block the entire scheduler. Make sure that you are reading of the channel, or use a dropping/sliding buffer to handle back-pressure.
  - `:executor` - the `java.util.Executor` that is used to run tasks.
  - `:max-concurrency` - the maximum number of tasks that will run concurrently.
  - `:max-retries`: the maximum number of times a tasks is restarted after a timeout or exception. This does not include the original attempt, so with N max retries a task will be run at most (N + 1) times. `:max-retries` can also be set on a task, which will override this value on a per-task basis.
  - `:timeout` - the job timeout in milliseconds. After this time has passed, the job will be cancelled and the result available when dereffing."
  ([tasks]
   (engine/run tasks {}))
  ([tasks opts]
   (engine/run tasks opts)))

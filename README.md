# Daguerreo [![CircleCI](https://circleci.com/gh/schmee/daguerreo.svg?style=svg)](https://circleci.com/gh/schmee/daguerreo) [![cljdoc badge](https://cljdoc.org/badge/schmee/daguerreo)](https://cljdoc.org/d/schmee/daguerreo/CURRENT)

Daguerreo is a library to create workflows using tasks. It takes care of dependency resolution, parallellism, retries and timeouts and let's you focus on your business logic. It shares the basic structure of other task execution libraries such as [Airflow](https://airflow.apache.org/) or [Luigi](https://github.com/spotify/luigi), but unlike those it is meant to be embedded in your application rather than run as a standalone service.

## Installation

[![Current Version](https://clojars.org/schmee/daguerreo/latest-version.svg)](https://clojars.org/schmee/daguerreo)

## Documentation

- [API documentation](https://cljdoc.org/d/schmee/daguerreo/CURRENT/api/daguerreo)

- [Specs](https://github.com/schmee/daguerreo/blob/master/src/daguerreo/impl/specs.clj)

## Example

Let's make a smoothie! We need to dice the fruit, pour in the coconut milk and water, blend it and put garnish on it. However, we can do without the garnish (if we for some reason fail to complete that task).

![Example 1](/images/example1.png)

Let's model this with Daguerreo:

```clj
(defn dice-fruit
  "Chop it up"
  [fruit]
  (apply str (shuffle (seq fruit))))

(defn blend
  "Smoosh it together"
  [& ingredients]
  (apply str (shuffle (mapcat seq ingredients))))

(defn garnish
  "Sugar on top"
  [smoothie]
  (str "sugar + " smoothie))

(def tasks
  [{:name :dice-banana
    :fn (fn [ctx _]
          {:diced-banana (dice-fruit (get-in ctx [:fruit "banana"]))})}
   {:name :dice-mango
    :fn (fn [ctx _]
          {:diced-mango (dice-fruit (get-in ctx [:fruit "mango"]))})}
   {:name :blend
    :fn (fn [ctx _]
          {:smoothie (blend (get ctx :diced-banana)
                            (get ctx :diced-mango)
                            (get-in ctx [:liquids "coconut-milk"])
                            (get-in ctx [:liquids "water"]))})
    :dependencies #{:dice-banana :dice-mango}}
   {:name :garnish
    :fn (fn [ctx _]
          {:smoothie (garnish (:smoothie ctx))})
    :continue-on-failure? true
    :dependencies #{:blend}}])

(def ctx {:fruit #{"banana" "mango"}
          :liquids #{"water" "coconut-milk"}})

(def job (daguerreo.core/run tasks {:ctx ctx :timeout 3000}))

dev=> @job
{:daguerreo.job/state :job.state/completed}
 :diced-banana "ananba"
 :diced-mango "mgona"
 :fruit #{"banana" "mango"}
 :liquids #{"coconut-milk" "water"}
 :smoothie "sugar + cotiaanmkuooatmecnbnagna-lwr"
```

A task function must return a map, which will be merged into the job context. Each task function receives the context which contains the initial context (specified in `run`) and the return values of all its dependencies. When the job is completed, the entire context is returned, and it contains the `:smoothie` as promised!

We can make a simple modification to the `run` command to gain more insight into what Daguerreo is doing:

```clj
user=> @(daguerreo.core/run tasks {:ctx ctx :timeout 3000 :event-chan (helpers/event-logger)})
JOB > idle -> running
:dice-banana > unscheduled -> ready
:dice-mango > unscheduled -> ready
:dice-banana > ready -> running
:dice-mango > ready -> running
:dice-mango > running -> completed
:dice-banana > running -> completed
:blend > unscheduled -> ready
:blend > ready -> running
:blend > running -> completed
:garnish > unscheduled -> ready
:garnish > ready -> running
:garnish > running -> completed
JOB > running -> completed
{:daguerreo.job/state :job.state/completed}
 :diced-banana "banaan"
 :diced-mango "agomn"
 :fruit #{"banana" "mango"}
 :liquids #{"coconut-milk" "water"}
 :smoothie "sugar + etnurmogbnmaaawtknonc-iaalco"
```

As we can see, the first two tasks can be done in parallell, while the last two tasks are run sequentially, just as we would expect from looking at the task graph!

Now, most of you are probably not making smoothies. Instead, imagine some complicated business logic that involves fetching data from multiple APIs, combining and transforming the recieved data, and then writing the result to a queue and a database:

![Example 2](/images/example2.png)

This sort of structure is very common, and Daguerreo let's you solve problems like this quickly and efficiently.

## Overview

 A **task** is a simple map with two mandatory keys: a `:name` that is used to refer to the task in various contexts, and `:fn`, which is a function that does the actual work. Importantly, you can also specify dependencies between tasks. If Task B depends on Task A, Daguerreo guarantees that the result of Task A will be ready before task be is scheduled.

The `run` function takes a collection of tasks to be performed and returns a **job**. The job is an opaque object similar to a promise that is derefferable and also [cancellable](#early-termination-cancellation-and-timeouts).

The way to communicate results between tasks in Daguerreo is the **job context**. The job context is a map that contains the result of all the tasks that have been completed so far (and any initial value passed to `run`). The `:fn` function in a task takes the context as the first argument and must return a map which will be merged into the context that is passed in to the next tasks. specifying dependencies, you can ensure that all the data needed for a task is ready and contained in the context before the task is scheduled to run.

 Behind the scenes, Duagerreo creates a [DAG](https://en.wikipedia.org/wiki/Directed_acyclic_graph) that models the flow of information between tasks. Tasks are scheduled so that independent tasks are run in parallell whenever possible, and if a tasks throws an exception it will be automatically re-scheduled and tried again. The maximum number of running task and the maximum number of retries are both configurable per task and job.

## Early termination: cancellation and timeouts

It is possible to specify a timeout both per-task and for the whole job:

```clj
(def tasks
  [{:name :some-task
    :timeout 5000 ;; the maximum time this task is allowed to run
    :max-retries 3 ;; the number of times the job will be retried before failing the task
    :fn some-fn}}])

(def job (daguerreo.core/run tasks {:timeout 10000})) ;; the maximum runtime of the whole job
```

When a task times out, it will be re-scheduled and eligible to be run again, subject to the `:max-retries` paramter.

It is also possible to manually cancel a job with `daguerreo.core/cancel`.

### Making tasks handle early termination

Since it is not possbile in general to forcibly preempt a running thread on the JVM, cancelltion in Daguerreo is *cooperative*, similar to other task schedulers such as [Kotlin coroutines](https://kotlinlang.org/docs/reference/coroutines/cancellation-and-timeouts.html) or [Python's Trio](https://trio.readthedocs.io/en/latest/reference-core.html#cancellation-and-timeouts). In most cases this doesn't require you to do anything, but in some cases you will need to give Daguerreo some help.

- **Blocking IO**: since Daguerreo cannot preempt a blocked thread, make sure you set the appropriate timeouts when you are doing blocking IO (such as network requests).

- **Long-running loops**: below is an example of a task that doesn't respond to early termination:

  ```clj
  (fn [ctx _]
    (loop []
      (Thread/sleep 1000) ;; simulate some work
      (recur)))
  ```

  Since a thread cannot be preempted, there is no way for Daguerreo to cancel this task. However, we can make it handle early termination with a simple modification:


  ```clj
  (fn [ctx is-active?]
    (loop []
      (when (is-active?) ;; am I still supposed to run?
        (Thread/sleep 1000)
        (recur)))
  ```

This is where the second argument to the task function comes into play. `is-active?` is a function that returns a boolean indicating whether the task is supposed to be running. If you have long-running tasks that you want to cancel or timeout, make sure to call `is-active?` intermittently.

## Listening to scheduler events

The scheduler in Daguerreo is entirely message-driven. These events contain information about state transitions of tasks which is then acted on by various internal workers. By passing a channel in the `:event-chan` option to `run`, you can get access to these events as well! This can be used to build all sorts of functionality such as detailed logging and per-task metrics.

The channel will contain events of type `:daguerreo/event`, that will look something like this:

```clj
{:daguerreo/event-type :task.event/state-transition
 :daguerreo.task/name ::some-task
 :daguerreo.task/old-state :task.state/running
 :daguerreo.task/new-state :task.state/completed}
```

For more details on what an event can contain, check out [the specs.](https://github.com/schmee/daguerreo/blob/master/src/daguerreo/impl/specs.clj)

## Tasks options

- `:timeout` - the task timeout in milliseconds. After this time has passed, the task will be cancelled and made eligible for scheduling.
- `:max-retries` - the maximum number of times the tasks is restarted after a timeout or exception. This does not include the original attempt, so a task with _N_ max retries will be run _(N + 1)_ times.
- `:continue-on-failure?` - when a task has failed `:max-retries` number of times, it is moved to the `failed` state. Normally, this causes the entire job to fail, but `:continue-on-failure?` is set to true the job will continue anyway. This can be useful for tasks that do logging, metrics or other things non-critical to the outcome of the job.

## Inspiration

- [Airflow](https://airflow.apache.org/)
- [Luigi](https://github.com/spotify/luigi)
- [Kotlin coroutines](https://kotlinlang.org/docs/reference/coroutines/coroutines-guide.html)
- [Trio](https://github.com/python-trio/trio)
- [core.async](https://github.com/clojure/core.async)
- [Notes on structured concurrency, or: Go statement considered harmful](https://vorpus.org/blog/notes-on-structured-concurrency-or-go-statement-considered-harmful/#a-surprise-benefit-removing-go-statements-enables-new-features)
- [Timeouts and cancellation for humans](https://vorpus.org/blog/timeouts-and-cancellation-for-humans/)

## License

Copyright © 2019 John Schmidt

Released under the MIT License: http://www.opensource.org/licenses/mit-license.php

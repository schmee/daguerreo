(ns daguerreo.helpers
  (:require [clojure.core.async :as a]))

(defmulti log-event (fn [_ event] (:daguerreo/event-type event)))

(defmethod log-event :default [log-fn event]
  (log-fn event))

(defmethod log-event :job.event/state-transition [log-fn event]
  (let [{:daguerreo.job/keys [old-state new-state]} event]
    (log-fn (str "JOB > " (name old-state) " -> " (name new-state)))))

(defmethod log-event :task.event/state-transition [log-fn event]
  (let [{:daguerreo.task/keys [old-state new-state]} event]
    (log-fn (str (:daguerreo.task/name event) " > " (name old-state) " -> " (name new-state)))))

(defn event-logger
  "Returns a channel, meant to be passed as the `:event-chan` argument to `run`, and starts a go-loop that will read and print the events using `print-fn` (defaults to `println`) in a human-readable (and unspecified) format."
  ([]
   (event-logger println))
  ([log-fn]
   (let [c (a/chan 100)]
    (a/go-loop []
      (when-let [event (a/<! c)]
        (log-event log-fn event)
        (recur)))
    c)))

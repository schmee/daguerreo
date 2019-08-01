(ns daguerreo.impl.engine
  (:require [better-cond.core :as b]
            [clojure.core.async :as a]
            [loom.graph :as graph]

            [daguerreo.impl.validation :as v]
            [daguerreo.impl.utils :as utils])
  (:import [java.util.concurrent Executor Executors ExecutorService ThreadFactory]))

(set! *warn-on-reflection* true)

(defmacro go-loop-bcond [bindings & body]
  `(a/go-loop ~bindings
     (b/cond ~@body)))

(defn check-state-transition [event expected-old expected-new]
  (let [{:daguerreo/keys [event-type]
         :daguerreo.task/keys [old-state new-state]} event]
    (and (= event-type :task.event/state-transition)
         (if expected-old (= old-state expected-old) true)
         (if expected-new (= new-state expected-new) true))))

(defn set-task-state! [task-state event-chan name task-state-or-run-state]
   (let [run-state (-> task-state name :run-state)
         old-run-state @run-state
         new-run-state (if (keyword? task-state-or-run-state)
                         {:state task-state-or-run-state :id (:id old-run-state)}
                         task-state-or-run-state)
         event {:daguerreo/event-type :task.event/state-transition
                :daguerreo.task/name name
                :daguerreo.task/old-state (:state old-run-state)
                :daguerreo.task/new-state (:state new-run-state)
                ::run-state new-run-state}]
     (reset! run-state new-run-state)
     (a/put! event-chan event)))

(defn cas-task-state!
  [task-state event-chan name old-run-state new-run-state opt-data]
  (let [run-state (-> task-state name :run-state)
        did-swap (compare-and-set! run-state old-run-state new-run-state)]
    (when did-swap
      (a/put! event-chan (merge {:daguerreo/event-type :task.event/state-transition
                                 :daguerreo.task/name name
                                 :daguerreo.task/old-state (:state old-run-state)
                                 :daguerreo.task/new-state (:state new-run-state)
                                 ::run-state new-run-state}
                                opt-data)))
    did-swap))

(defn completed? [task]
  (= (::state task) :task.state/completed))

(defn running? [task]
  (= (::state task) :task.state/running))

(defn unscheduled? [task]
  (= (::state task) :task.state/unscheduled))

(defn get-deps [task-map-with-state task]
  (map task-map-with-state (:dependencies task)))

(defn can-transition-to-ready? [task-map-with-state task]
  (and (unscheduled? task)
       (every? completed? (get-deps task-map-with-state task))))

(defn find-unscheduled-with-deps-satisfied
  ([task-map task-state]
   (find-unscheduled-with-deps-satisfied task-map task-state (keys task-state)))
  ([task-map task-state candidates]
   (let [dereffed-state (into {}
                          (for [[task-name state] task-state]
                            [task-name {::state (-> state :run-state deref :state)}]))
         task-map-with-state (merge-with merge task-map dereffed-state)]
     (->> candidates
          (map task-map-with-state)
          (filter (partial can-transition-to-ready? task-map-with-state))
          (sort-by (comp - count :dependents))))))

(def n-cores
  (.availableProcessors (Runtime/getRuntime)))

(defn counted-thread-factory []
  (let [counter (atom 0)]
    (reify ThreadFactory
      (newThread [this runnable]
        (doto (Thread. runnable)
          (.setName (str "daguerreo-" (swap! counter inc)))
          (.setDaemon true))))))

(def default-executor
  (delay (Executors/newCachedThreadPool (counted-thread-factory))))

(defn create-tap [mult buffer-count]
  (as-> (a/chan buffer-count) $
    (a/tap mult $)
    $))

(defn is-active-fn [event-tap task-name]
  (let [active? (atom true)]
    (go-loop-bcond []
      :let [{:daguerreo.job/keys [new-state]
             :daguerreo.task/keys [name] :as event} (a/<! event-tap)]

      (or (nil? event)
          (= new-state :job.state/cancelled)
          (and (= name task-name)
               (check-state-transition event :task.state/running nil)))
      (do
        (reset! active? false)
        (a/close! event-tap))

      :else (recur))
    (fn [] @active?)))

(defn run-async-with-ex-handler [^Executor pool f ctx is-active-fn]
  (let [result-chan (a/chan)]
    (.execute
      pool
      ^Runnable
      (fn []
        (let [result (try
                       (f @ctx is-active-fn)
                       (catch Exception e
                         e))]
          (when (some? result)
            (a/put! result-chan result))
          (a/close! result-chan))))
    result-chan))

(defn run-task! [executor task-id task-state ctx event-chan create-event-tap task]
  (let [{name :name f :fn} task
        id (swap! task-id inc)
        run-state {:state :task.state/running
                   :id id}]
    (set-task-state! task-state event-chan name run-state)
    (a/go
      (let [iafn (is-active-fn (create-event-tap) name)
            result (a/<! (run-async-with-ex-handler executor f ctx iafn))]
        (cond
          (instance? Exception result)
          (do
            (reset! (-> task-state name :exception) result)
            (cas-task-state! task-state event-chan name run-state {:state :task.state/exception :id id} {:daguerreo.task/exception result}))

          (map? result)
          (do
            (swap! ctx merge result)
            (cas-task-state! task-state event-chan name run-state {:state :task.state/completed :id id} {:daguerreo.task/result result}))

          :else
          (cas-task-state! task-state event-chan name run-state {:state :task.state/invalid-result :id id} {:daguerreo.task/result result}))))))


(defn create-runner [deps event-tap]
  (let [{:keys [task-state task-map task-graph ctx event-chan create-event-tap opts]} deps
        {:keys [max-concurrency executor]} opts
        pool (or executor @default-executor)
        task-id (atom 0)]
    (go-loop-bcond [n-running 0
                    task-queue (clojure.lang.PersistentQueue/EMPTY)]
      :when-let [{:daguerreo.task/keys [name] :as event} (a/<! event-tap)]

      (check-state-transition event nil :task.state/ready)
      (if (and max-concurrency (>= n-running max-concurrency))
        (recur n-running (conj task-queue name))
        (do
          (run-task! pool task-id task-state ctx event-chan create-event-tap (get task-map name))
          (recur (inc n-running) task-queue)))

      (check-state-transition event nil :task.state/completed)
      (if-let [next-task-name (peek task-queue)]
        (do
          (run-task! pool task-id task-state ctx event-chan create-event-tap (get task-map next-task-name))
          (recur n-running (pop task-queue)))
        (recur (dec n-running) task-queue))

      (check-state-transition event :task.state/running nil)
      (recur (dec n-running) task-queue)

      :else (recur n-running task-queue))))

(defn create-scheduler [deps event-tap]
  (let [{:keys [task-state task-map task-graph ctx event-chan opts]} deps]
    (go-loop-bcond []
      :when-let [{:daguerreo.task/keys [name] :as event} (a/<! event-tap)]

      (check-state-transition event nil :task.state/completed)
      (let [dependents (-> task-map name :dependents)
            next-tasks (find-unscheduled-with-deps-satisfied task-map task-state dependents)]
        (doseq [task next-tasks]
          (set-task-state! task-state event-chan (:name task) :task.state/ready))
        (recur))

      :else (recur))))

(defn create-retryer [deps event-tap]
  (let [{:keys [task-state task-map task-graph ctx event-chan opts]} deps]
    (go-loop-bcond [retries-per-tasks (zipmap (keys task-map) (repeat 0))]
      :when-let [{:daguerreo.task/keys [name] :as event} (a/<! event-tap)]

      (or (check-state-transition event nil :task.state/exception)
          (check-state-transition event nil :task.state/timed-out))
      (let [max-retries (or (-> task-map name :max-retries)
                            (-> opts :max-retries))]
        (if (and max-retries (< (get retries-per-tasks name) max-retries))
          (do
            (set-task-state! task-state event-chan name :task.state/ready)
            (recur (update retries-per-tasks name inc)))
          (do
            (set-task-state! task-state event-chan name :task.state/failed)
            (recur retries-per-tasks))))

      :else (recur retries-per-tasks))))

(defn create-timeouter [deps event-tap]
  (let [{:keys [task-state task-map task-graph ctx event-chan opts]} deps]
    (go-loop-bcond [timeouts {}]
      :let [[{:daguerreo/keys [event-type]
              :daguerreo.task/keys [name]
              :as event} c] (a/alts! (into [event-tap] (keys timeouts)))]

      (check-state-transition event nil :task.state/running)
      (if-let [timeout (-> task-map name :timeout)]
        (recur (assoc timeouts (a/timeout timeout) {:name name
                                                    :run-state (::run-state event)}))
        (recur timeouts))

      (check-state-transition event :task.state/running nil)
      (recur (dissoc timeouts {:name name
                               :run-state (::run-state event)}))

      (and (not= c event-tap) (timeouts c))
      (let [{:keys [name run-state]} (timeouts c)
            new-run-state {:state :task.state/timed-out
                           :id (:id run-state)}]
        (cas-task-state! task-state event-chan name run-state new-run-state {})
        (recur (dissoc timeouts c)))

      (and (nil? event) (= c event-tap))
      (comment "exit loop")

      :else (recur timeouts))))

(defn set-job-state! [job-state event-chan new-state]
  (let [old-state @job-state]
    (reset! job-state new-state)
    (a/put! event-chan {:daguerreo/event-type :job.event/state-transition
                        :daguerreo.job/old-state old-state
                        :daguerreo.job/new-state new-state})))

(def task-state->job-state
  {:task.state/timed-out :job.state/timed-out
   :task.state/exception :job.state/exception
   :task.state/illegal-state :job.state/failed})

(defn create-supervisor [deps event-tap control-chan job-state]
  (let [{:keys [task-state task-map task-graph ctx event-chan opts]} deps
        {:keys [timeout]} opts
        timeout-chan (when timeout (a/timeout timeout))
        chans (cond-> [control-chan event-tap]
                timeout (into [timeout-chan]))]
    (go-loop-bcond [tasks-remaining (-> task-map keys set)]
      :let [[{:daguerreo.task/keys [name old-state new-state result] :as event} c] (a/alts! chans :priority true)]

      (= c timeout-chan)
      (do
        (set-job-state! job-state event-chan :job.state/timed-out)
        (swap! ctx merge {:daguerreo.job/state :job.state/timed-out})
        (a/close! event-chan))

      (= c control-chan)
      (do
        (set-job-state! job-state event-chan :job.state/cancelled)
        (swap! ctx merge {:daguerreo.job/state :job.state/cancelled})
        (a/close! event-chan))

      (check-state-transition event nil :task.state/completed)
      (let [new-tasks-remaining (disj tasks-remaining name)]
        (if (seq new-tasks-remaining)
          (recur new-tasks-remaining)
          (do
            (set-job-state! job-state event-chan :job.state/completed)
            (swap! ctx assoc :daguerreo.job/state :job.state/completed)
            (a/close! event-chan))))

      (check-state-transition event nil :task.state/invalid-result)
      (do
        (swap! ctx merge {:daguerreo.job/state :job.state/failed
                          :daguerreo.job/failure-reason new-state
                          :daguerreo.task/name name
                          :daguerreo.task/result result})
        (set-job-state! job-state event-chan :job.state/failed)
        (a/close! event-chan))

      (and (check-state-transition event nil :task.state/failed)
           (-> task-map name :continue-on-failure? not))
      (let [final-job-state (task-state->job-state old-state)]
        (swap! ctx (fn [ctx]
                    (let [e (-> task-state name :exception deref)]
                      (merge ctx
                             (cond-> {:daguerreo.job/state :job.state/failed
                                      :daguerreo.job/failure-reason old-state
                                      :daguerreo.task/name name}
                               e (assoc :daguerreo.task/exception e))))))
        (set-job-state! job-state event-chan final-job-state)
        (a/close! event-chan))

      :else (recur tasks-remaining))))

(def this-ns
  (str (ns-name *ns*)))

(defn create-impl-key-remover [deps event-out-chan event-tap]
  (a/go-loop []
    (if-let [event (a/<! event-tap)]
      (let [no-impl-keys (reduce-kv (fn [m k v]
                                      (if-not (= (namespace k) this-ns)
                                        (assoc m k v)
                                        m))
                                    {}
                                    event)]
        (a/>! event-out-chan event)
        (recur))
      (a/close! event-out-chan))))

(defprotocol Cancelable
  (cancel [this]))

(defprotocol Inspectable
  (inspect [this]))

(defrecord Dependencies [task-state task-map task-graph ctx event-chan create-event-tap opts])

(defn reify-job [deps ctx event-tap control-chan]
  (let [result (promise)]
    (go-loop-bcond []
      :when-let [{:daguerreo.job/keys [old-state new-state]} (a/<! event-tap)]
      (= old-state :job.state/running) (deliver result @ctx)
      :else (recur))

    (reify
      clojure.lang.IDeref
      (deref [_]
        (deref result))

      clojure.lang.IBlockingDeref
      (deref [_ timeout-ms timeout-val]
        (deref result timeout-ms timeout-val))

      clojure.lang.IPending
      (isRealized [_]
        (realized? result))

      Cancelable
      (cancel [this]
        (a/close! control-chan))

      Inspectable
      (inspect [_] deps))))

(defrecord Dependencies [task-state task-map task-graph ctx event-chan create-event-tap opts job-state])

(def default-opts
  {:max-retries 5
   :max-concurrency n-cores
   :ctx {}})

(defn create-task-map [tasks task-graph]
  (into {}
    (for [task tasks :let [name (:name task)]]
      [name
       (assoc task :dependents (or (graph/successors task-graph name) #{}))])))

(defn run
  ([tasks]
   (run tasks {}))
  ([tasks user-opts]
   (let [task-graph (utils/tasks->graph tasks)]
     (v/validate-and-report tasks task-graph)
     (let [opts (merge default-opts user-opts)
           event-out-chan (:event-chan opts)
           ctx-atom (atom (:ctx opts))
           task-map (create-task-map tasks task-graph)
           buffer-count (* (count tasks) 3) ;; no real reason for this count...
           event-chan (a/chan buffer-count)
           event-mult (a/mult event-chan)
           create-event-tap #(create-tap event-mult buffer-count)
           ;; map of atoms instead of single map atom because CAS is needed for the state
           task-state (zipmap (map :name tasks)
                              (repeatedly #(hash-map :run-state (atom {:state :task.state/unscheduled
                                                                       :id nil})
                                                     :exception (atom nil))))
           job-state (atom :job.state/idle)
           deps (->Dependencies task-state task-map task-graph ctx-atom event-chan create-event-tap opts job-state)
           control-chan (a/chan 1)
           worker-fns (cond-> [#(create-supervisor deps % control-chan job-state)
                               #(create-runner deps %)
                               #(create-retryer deps %)
                               #(create-scheduler deps %)
                               #(create-timeouter deps %)]
                        event-out-chan (conj #(create-impl-key-remover deps event-out-chan %)))
           ;; Create all event taps before starting workers to prevent races / lost messages
           event-taps (vec (take (count worker-fns) (repeatedly create-event-tap)))
           job-tap (create-event-tap)]
       (set-job-state! job-state event-chan :job.state/running)

       (doseq [[worker-fn tap] (map vector worker-fns event-taps)]
         (worker-fn tap))

       (doseq [task (find-unscheduled-with-deps-satisfied task-map task-state)]
         (set-task-state! task-state event-chan (:name task) :task.state/ready))

       (reify-job deps ctx-atom job-tap control-chan)))))

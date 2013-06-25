(ns flatland.laminate
  (:require [flatland.useful.utils :refer [returning]]
            [lamina.time :as t]
            [lamina.core :as lamina :refer [channel enqueue receive-all enqueue-and-close]]
            [lamina.connections :as connection]
            [lamina.core.operators :as op]
            [lamina.query.operators :as q]
            [lamina.query.core :refer [def-query-operator def-query-comparator query-comparator]]))

(defn within-window-op [trigger {:keys [id action window task-queue period]
                                 :or {id :id, action :action,
                                      window (t/hours 1) period (t/period)
                                      task-queue (t/task-queue)}}
                        ch]
  (let [result (channel)
        watch-list (ref {})
        expiries (ref (sorted-map))
        conj (fnil conj [])
        now #(t/now task-queue)
        [get-id get-action] (map q/getter [id action])
        trigger (name trigger)]
    (lamina/concat*
     (op/bridge-accumulate ch result "within-window"
       {:accumulator (fn [x]
                       (let [[this-id this-action] ((juxt get-id get-action) x)]
                         (dosync
                          (let [present? (contains? @watch-list this-id)
                                trigger? (= trigger (name this-action))]
                            (when (or present? trigger?)
                              (alter watch-list update-in [this-id]
                                     conj this-action))
                            (when (and trigger? (not present?))
                              (alter expiries update-in [(+ (now) window)]
                                     conj this-id))))))
        :emitter (fn []
                   (dosync
                    (let [watches @watch-list
                          expired (subseq @expiries <= (now))
                          ids (mapcat val expired)]
                      (alter expiries #(apply dissoc % (map key expired)))
                      (alter watch-list #(apply dissoc % ids))
                      (for [id ids]
                        {:id id, :actions (get watches id)}))))
        :period period
        :task-queue task-queue}))))

(def-query-operator within-window
  :periodic? true
  :distribute? false
  :transform (fn [{:keys [options]} ch]
               (within-window-op (get options 0) (dissoc options 0) ch)))

(defn after-op [filters {:keys [window period task-queue]
                         :or {window (t/hours 1) period (t/period)
                              task-queue (t/task-queue)}}
                ch]
  (let [start? (fn [x] (every? #(% x) filters))
        result (lamina/channel)
        now #(t/now task-queue)
        queued (ref [])
        expiry (ref nil)]
    (lamina/concat*
     (op/bridge-accumulate ch result "after"
      {:accumulator (fn [x]
                      (dosync
                       (let [include? (or @expiry (start? x))
                             new? (and include? (not @expiry))]
                         (when include?
                           (alter queued conj x))
                         (when new?
                           (ref-set expiry (+ (now) window))))))
       :emitter (fn []
                  (dosync
                   (when (and @expiry (<= @expiry (now)))
                     (returning [@queued]
                       (ref-set expiry nil)
                       (ref-set queued [])))))
       :period period
       :task-queue task-queue}))))

(def-query-operator after
  :periodic? true
  :distribute? false
  :transform (fn [{:keys [options]} ch]
               (after-op (for [[k v] options
                               :when (number? k)]
                           (query-comparator v))
                         options
                         ch)))

(def-query-comparator contains
  (fn [field value]
    (let [f (comp (partial map q/normalize-for-comparison)
                  (q/getter field))
          pred #{value}]
      #(some pred (f %)))))


(defn reduce-and-emit [name f emit]
  (fn [{:keys [options]}
       ch]
    (let [{:keys [period task-queue] :or {period (t/period), task-queue (t/task-queue)}}
          options]
      (let [empty (Object.)
            acc (ref empty)
            f (fn [acc x]
                (if (identical? acc empty)
                  (f x)
                  (f acc x)))]
        (lamina/concat*
         (op/bridge-accumulate ch (channel) name
                               {:accumulator (fn [x]
                                               (dosync (alter acc f x)))
                                :emitter (fn []
                                           (dosync (let [value @acc]
                                                     (ref-set acc empty)
                                                     (when-not (identical? value empty)
                                                       [(emit value)]))))
                                :period period, }))))))

(def-query-operator max
  :periodic? true
  :distribute? true
  :transform (reduce-and-emit "max" max identity))

(def-query-operator min
  :periodic? true
  :distribute? true
  :transform (reduce-and-emit "min" min identity))

(def-query-operator mean
  :periodic? true
  :distribute? false
  :transform (reduce-and-emit "mean" (fn
                                       ([x]
                                          {:count 1 :sum x})
                                       ([{:keys [count sum]} y]
                                          {:count (inc count) :sum (+ sum y)}))
                              (fn [{:keys [count sum]}]
                                (/ sum count))))

(letfn [(mapper [f]
          (fn [{:keys [options]} ch]
            (let [arg (get options 0)]
              (lamina/map* #(f arg %) ch))))]
  (def-query-operator scale
    :periodic? false
    :distribute? true
    :transform (mapper *))
  (def-query-operator add
    :periodic? false
    :distribute? true
    :transform (mapper +))
  (def-query-operator format
    :periodic? false
    :distribute? true
    :transform (mapper format)))

(def-query-operator map
  :periodic? false
  :distribute? true
  :transform (fn [{:keys [options]} ch]
               (let [f (q/getter (get options 0))]
                 (lamina/map* (partial map f) ch))))

(def-query-operator meta
  :periodic? false
  :distribute? true
  :transform (fn [options ch]
               (lamina/map* meta ch)))

(def-query-operator top
  :periodic? false
  :distribute? false
  :transform (fn [{:keys [options] :as x} ch]
               (let [n (or (get options 0)
                           (throw (IllegalArgumentException.
                                   "`top` operator needs numeric first argument")))
                     f (if-let [operator (some options [:by 1])]
                         (q/getter operator)
                         identity)]
                 (->> ch
                      (lamina/map* (fn [m]
                                     (into {}
                                           (take n
                                                 (sort-by (comp f val)
                                                          m)))))))))

(defn persistent-stream
  "Given a way to connect a channel to a server, and a \"sink\" channel to read from, creates an
  automatically-reconnecting channel to the server, and siphons all messages from the sink to the
  server. Returns a function which will close the persistent stream. All messages from the server
  are ignored: this is intended to be a one-way, fire-and-forget stream."
  [sink channel-fn]
  (let [server (connection/persistent-connection channel-fn
                                                 {:on-connected (fn [ch]
                                                                  (lamina/ground ch)
                                                                  (lamina/siphon sink ch))})]
    (server)
    (fn [] (connection/close-connection server))))

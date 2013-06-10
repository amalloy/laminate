(ns flatland.laminate.render
  (:require lamina.query.struct
            [lamina.query :as query]
            [flatland.useful.map :refer [keyed]]
            [flatland.laminate.time :as time]))

(defn parse-interval
  ([^String s]
     (let [[sign s] (if (.startsWith s "-")
                        [- (subs s 1)]
                        [+ s])]
         (long (sign (lamina.query.struct/parse-time-interval s)))))
  ([^String s default]
     (if (seq s)
       (parse-interval s)
       default)))

(defn parse-render-opts [{:keys [now timezone from until shift period align]}]
  (let [offset (parse-interval shift 0)
        timezone (parse-interval timezone 0)
        period (parse-interval period nil)
        now (+ offset now)
        [align post-offset] (case align
                              (nil "") [1 0]
                              ("true" "end") [period 0]
                              ("start") [period period]
                              [(parse-interval align 1) 0])
        [from until] (for [[timespec default] [[from (time/subtract-day now)]
                                               [until now]]]
                       (time/ms->s
                        (-> (if (seq timespec)
                              (+ now (parse-interval timespec))
                              default)
                            (time/align-to align timezone))))
        offset (+ offset post-offset)]
    (keyed [offset from until period])))

(defn points [targets offset query-opts]
  (for [[target datapoints]
        ,,(query/query-seqs (zipmap (map (partial str "&") targets)
                                    (repeat nil))
                            query-opts)]
    {:target (subs target 1)
     :datapoints (for [{:keys [timestamp value]} datapoints]
                   [value (-> timestamp ;; render API expects [value time] tuples
                              (- offset)
                              (time/ms->s))])}))

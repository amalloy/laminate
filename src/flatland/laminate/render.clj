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
        align (parse-interval align 1)
        timezone (parse-interval timezone 0)
        period (parse-interval period nil)
        now (+ offset now)
        [from until] (for [[timespec default] [[from (time/subtract-day now)]
                                               [until now]]]
                       (time/ms->s
                        (-> (if (seq timespec)
                              (+ now (parse-interval timespec))
                              default)
                            (time/align-to align timezone))))]
    (keyed [offset from until period])))

(defn points [targets offset query-opts]
  (for [target targets]
    {:target target
     :datapoints (for [{:keys [timestamp value]}
                       ,,(val (first (query/query-seqs
                                      {(str "&" target) nil} query-opts)))]
                   [value (-> timestamp ;; render API expects [value time] tuples
                              (- offset)
                              (time/ms->s))])}))
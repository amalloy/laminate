(ns flatland.laminate.render
  (:require lamina.query.struct
            [lamina.query :as query]
            [flatland.useful.map :refer [keyed]]
            [flatland.laminate.time :as time])
  (:import java.text.SimpleDateFormat))

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

(defn parse-time* [s now]
  (let [parser (java.text.SimpleDateFormat. "yyyy-MM-dd")]
    (when (seq s)
      (try
        [:absolute (.getTime (.parse parser s))]
        (catch Exception e
          (try
            (let [interval (parse-interval s)]
              (if (pos? interval)
                [:relative interval]
                [:absolute (+ now interval)]))
            (catch Exception e
              [:absolute (Long/parseLong s)])))))))

(defn parse-time [s now]
  (when-let [[style n] (parse-time* s now)]
    (if (= :absolute n)
      n
      (+ now n))))

(defn parse-render-opts [{:keys [target now timezone from until shift period align]}]
  (let [targets (if (coll? target)   ; if there's only one target it's a string, but if multiple are
                  target             ; specified then compojure will make a list of them
                  [target])
        offset (parse-interval shift 0)
        timezone (parse-interval timezone 0)
        period (parse-interval period nil)
        now (+ offset now)
        [align post-offset] (case align
                              (nil "" "false") [1 0]
                              ("true" "end") [period 0]
                              ("start") [period period]
                              [(parse-interval align 1) 0])
        [until-style until-value] (if (seq until)
                                    (parse-time* until now)
                                    [:absolute now])
        [from-style from-value] (parse-time* (or (not-empty from)
                                                "24h")
                                             now)
        [from until] (cond (every? #{:relative} [from-style until-style])
                           ,,(throw (IllegalArgumentException.
                                     "from and until can't both be relative"))
                           (= :relative from-style)
                           ,,[(- until-value from-value) until-value]
                           (= :relative until-style)
                           ,,[from-value (+ from-value until-value)]
                           :else [from-value until-value])
        [from until] (for [t [from until]]
                       (-> t
                           (time/align-to align timezone)
                           (time/ms->s)))
        offset (+ offset post-offset)]
    (keyed [targets offset from until period])))

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

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

(defn parse-time [s now]
  (let [parser (java.text.SimpleDateFormat. "yyyy-MM-dd")]
    (when (seq s)
      (try
        (.getTime (.parse parser s))
        (catch Exception e
          (try
            (+ now (parse-interval s))
            (catch Exception e
              (Long/parseLong s))))))))

(defn parse-render-opts [{:keys [target now timezone span from until shift period align]}]
  (when (every? seq [span from])
    (throw (IllegalArgumentException. "Can't specify both from and span")))
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
        until (if (seq until)
                (parse-time until now)
                now)
        from (if (seq from)
               (parse-time from now)
               (- until (parse-interval (or (not-empty span)
                                            "24h"))))
        [from until] (for [t [from until]]
                       (-> (time/ms->s t)
                           (time/align-to align timezone)))
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

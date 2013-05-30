(ns flatland.laminate.time)

(defn ms->s [ms]
  (-> ms (quot 1000)))

(defn s->ms [s]
  (* s 1000))

(let [ms-per-day (* 1000 60 60 24)]
  (defn subtract-day [ms]
    (- ms ms-per-day)))

(defn align-to
  ([i alignment]
     (align-to i alignment 0))
  ([i alignment tz-offset]
     (-> (* alignment
            (quot (+ i tz-offset (dec alignment)) ;; round *up* to nearest [alignment]
                  alignment))
         (- tz-offset))))
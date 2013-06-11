(ns flatland.laminate.render-test
  (:use clojure.test
        flatland.laminate.render
        flatland.laminate.time)
  (:import (java.util Date Calendar TimeZone)))

(def utc (TimeZone/getTimeZone "UTC"))
(def la (TimeZone/getTimeZone "America/Los_Angeles"))

(defn calendar [^TimeZone tz y m d]
  (doto (Calendar/getInstance tz)
    (.set Calendar/YEAR y)
    (.set Calendar/MONTH (dec m))
    (.set Calendar/DAY_OF_MONTH d)
    (.set Calendar/HOUR 7) ;; set arbitrarily so that it's not exactly on-the hour
    (.set Calendar/MINUTE 31)
    (.set Calendar/SECOND 22)
    (.set Calendar/MILLISECOND 0)))

(def seconds-per-hour (* 60 60))
(defn divides? [m n]
  (zero? (rem n m)))

(deftest test-parsing
  (let [now (calendar la 2013 6 10) ;; we want a UTC offset of -7, not -8, to test align code
        ms (-> now (.getTime) (.getTime))
        default-opts {:now ms :timezone (str (.getOffset la ms) "ms")}]
    (is (= (ms->s ms) (:until (parse-render-opts default-opts))))
    (is (not (divides? seconds-per-hour
                       (:from (parse-render-opts (assoc default-opts :period "2h"))))))
    (let [aligned-start (:from (parse-render-opts (assoc default-opts :period "2h" :align "true")))]
      (is (divides? seconds-per-hour aligned-start))
      (is (not (divides? (* 2 seconds-per-hour) ;; because it's in UTC, but aligned to PST
                         aligned-start))))
    (is (divides? (* 2 seconds-per-hour) ;; here we align in UTC, so it should divide
                  (:from (parse-render-opts (-> default-opts
                                                (assoc :period "2h" :align "true")
                                                (dissoc :timezone))))))
    (is (= (* 2000 seconds-per-hour)
           (:offset (parse-render-opts (assoc default-opts :period "2h" :align "start")))))))

(ns flatland.laminate-test
  (:use clojure.test
        flatland.laminate
        lamina.core))


(deftest test-stream
  (let [results (channel)
        current-channel (atom nil)
        connect (fn []
                  (reset! current-channel
                          (doto (channel)
                            (siphon results))))
        inputs (permanent-channel)
        stream (persistent-stream inputs connect)]
    (enqueue inputs 1)
    (is (= 1 @(read-channel results)))
    (let [active @current-channel]
      (close @current-channel)
      (enqueue inputs 2)
      (is (not= @current-channel active))
      (is (= 2 @(read-channel results))))))

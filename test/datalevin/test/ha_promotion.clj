(ns datalevin.test.ha-promotion
  (:require
   [clojure.test :refer [deftest is]]
   [datalevin.ha.promotion :as promo]))

(defn- lag-input
  [leader-watermark]
  (#'promo/pre-cas-lag-input
   {:fresh-ha-promotion-local-last-applied-lsn-fn
    (fn [_m _lease] 8)
    :fetch-leader-watermark-lsn-fn
    (fn [_db-name _m _lease] leader-watermark)
    :ha-member-watermarks-fn
    (fn [_db-name _m _endpoints] {})
    :highest-reachable-ha-member-watermark-fn
    (fn [_db-name _m _watermarks]
      {:last-applied-lsn 8})
    :normalize-leader-watermark-result-fn
    (fn [_lease result] result)}
   "db"
   {:ha-clock-skew-budget-ms 0}
   {:leader-endpoint "127.0.0.1:19001"
    :leader-last-applied-lsn 21
    :lease-until-ms 1000}
   1001
   leader-watermark))

(deftest pre-cas-lag-input-preserves-authority-lsn-test
  (let [reachable (lag-input {:reachable? true
                              :last-applied-lsn 8})
        unreachable (lag-input {:reachable? false
                                :reason :leader-watermark-fetch-failed})]
    (is (= 21 (get-in reachable
                      [:effective-lease :leader-last-applied-lsn])))
    (is (= 21 (get-in unreachable
                      [:effective-lease :leader-last-applied-lsn])))
    (is (= 21 (:authority-last-applied-lsn reachable)))
    (is (= 8 (:leader-watermark-last-applied-lsn reachable)))
    (is (= 8 (:local-last-applied-lsn reachable)))))

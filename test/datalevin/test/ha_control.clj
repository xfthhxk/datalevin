(ns datalevin.test.ha-control
  (:require
   [clojure.test :refer [deftest is]]
   [datalevin.ha :as ha]
   [datalevin.ha.control :as ctrl]
   [datalevin.ha.lease :as lease]))

(deftest renew-transition-clamps-leader-last-applied-lsn-monotonically-test
  (let [db-identity "ha-control-renew-clamp"
        lease-entry {:lease (lease/new-lease-record
                             {:db-identity db-identity
                              :leader-node-id 1
                              :leader-endpoint "127.0.0.1:19033"
                              :term 7
                              :lease-renew-ms 1000
                              :lease-timeout-ms 5000
                              :now-ms 1000
                              :leader-last-applied-lsn 11})
                     :version 3}
        state {:leases {db-identity lease-entry}}
        request {:db-identity db-identity
                 :leader-node-id 1
                 :leader-endpoint "127.0.0.1:19033"
                 :term 7
                 :lease-renew-ms 1000
                 :lease-timeout-ms 5000
                 :leader-last-applied-lsn 10
                 :now-ms 2000}
        {:keys [result state]}
        (#'ctrl/apply-renew-transition state request 2000 1000)]
    (is (:ok? result))
    (is (= 11
           (get-in result [:lease :leader-last-applied-lsn])))
    (is (= 11
           (get-in state
                   [:leases db-identity :lease :leader-last-applied-lsn])))))

(deftest acquire-transition-rejects-leader-last-applied-lsn-regression-test
  (let [db-identity "ha-control-acquire-regression"
        old-lease (lease/new-lease-record
                   {:db-identity db-identity
                    :leader-node-id 1
                    :leader-endpoint "127.0.0.1:19033"
                    :term 7
                    :lease-renew-ms 1000
                    :lease-timeout-ms 5000
                    :now-ms 1000
                    :leader-last-applied-lsn 11})
        state {:leases {db-identity {:lease old-lease
                                     :version 3}}}
        request {:db-identity db-identity
                 :leader-node-id 2
                 :leader-endpoint "127.0.0.1:19034"
                 :lease-renew-ms 1000
                 :lease-timeout-ms 5000
                 :leader-last-applied-lsn 10
                 :observed-version 3
                 :observed-lease old-lease
                 :now-ms 7000}
        {:keys [result state]}
        (#'ctrl/apply-try-acquire-transition state request 7000 1000)]
    (is (false? (:ok? result)))
    (is (= :leader-last-applied-lsn-regressed (:reason result)))
    (is (= 11
           (get-in state
                   [:leases db-identity :lease :leader-last-applied-lsn])))))

(deftest write-admission-rejects-stale-clock-skew-check-test
  (with-redefs [ha/ha-now-ms (constantly 5000)
                ha/ha-now-nanos (constantly 5000000)]
    (let [error (ha/ha-write-admission-error
                 {"db" {:ha-authority ::authority
                        :ha-role :leader
                        :ha-node-id 1
                        :ha-authority-owner-node-id 1
                        :ha-members [{:node-id 1
                                      :endpoint "127.0.0.1:19001"}]
                        :ha-authority-lease {:leader-endpoint
                                             "127.0.0.1:19001"}
                        :ha-authority-read-ok? true
                        :ha-last-authority-refresh-ms 5000
                        :ha-lease-timeout-ms 3000
                        :ha-lease-local-deadline-ms 9000
                        :ha-lease-local-deadline-nanos 9000000
                        :ha-leader-term 1
                        :ha-authority-term 1
                        :ha-lease-renew-ms 1000
                        :ha-clock-skew-hook {:cmd ["clock-skew"]}
                        :ha-clock-skew-last-check-ms 3000
                        :ha-clock-skew-refresh-pending? true
                        :ha-clock-skew-last-result {:ok? false
                                                    :pending? true}}}
                 {:type :tx-data
                  :args ["db"]})]
      (is (= :ha/write-rejected (:error error)))
      (is (= :clock-skew-check-pending (:reason error)))
      (is (= :clock-skew-check-pending
             (get-in error [:ha-clock-skew-error :reason]))))))

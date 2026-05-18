(ns datalevin.test.ha-control
  (:require
   [clojure.test :refer [deftest is]]
   [datalevin.ha :as ha]
   [datalevin.ha.authority :as auth]
   [datalevin.ha.control :as ctrl]
   [datalevin.ha.lease :as lease]
   [datalevin.server.ha :as sha]))

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

(deftest release-transition-preserves-authority-high-watermarks-test
  (let [db-identity "ha-control-release-retains-high-water"
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
        release-res (#'ctrl/apply-release-transition
                     state
                     {:db-identity db-identity
                      :leader-node-id 1
                      :term 7})
        released-state (:state release-res)
        acquire-low (#'ctrl/apply-try-acquire-transition
                     released-state
                     {:db-identity db-identity
                      :leader-node-id 2
                      :leader-endpoint "127.0.0.1:19034"
                      :lease-renew-ms 1000
                      :lease-timeout-ms 5000
                      :leader-last-applied-lsn 10
                      :observed-version 4
                      :now-ms 7000}
                     7000
                     1000)
        acquire-ok (#'ctrl/apply-try-acquire-transition
                    released-state
                    {:db-identity db-identity
                     :leader-node-id 2
                     :leader-endpoint "127.0.0.1:19034"
                     :lease-renew-ms 1000
                     :lease-timeout-ms 5000
                     :leader-last-applied-lsn 11
                     :observed-version 4
                     :now-ms 7000}
                    7000
                    1000)]
    (is (true? (get-in release-res [:result :ok?])))
    (is (nil? (get-in released-state [:leases db-identity :lease])))
    (is (= 7 (get-in released-state [:leases db-identity :term])))
    (is (= 11
           (get-in released-state
                   [:leases db-identity :leader-last-applied-lsn])))
    (is (= :leader-last-applied-lsn-regressed
           (get-in acquire-low [:result :reason])))
    (is (true? (get-in acquire-ok [:result :ok?])))
    (is (= 8 (get-in acquire-ok [:result :term])))))

(deftest read-index-observation-does-not-extend-lease-deadline-test
  (let [lease {:leader-node-id 1
               :leader-endpoint "127.0.0.1:19033"
               :term 3
               :lease-until-ms 10000}
        m {:ha-authority-lease lease
           :ha-lease-local-deadline-ms 5000
           :ha-lease-local-deadline-nanos 5000000}
        same-lease (auth/apply-authority-observation
                    m
                    {:lease lease
                     :version 4
                     :authority-now-ms nil
                     :lease-local-deadline-ms nil
                     :lease-local-deadline-nanos nil}
                    2000)
        changed-lease (auth/apply-authority-observation
                       m
                       {:lease (assoc lease :lease-until-ms 11000)
                        :version 5
                        :authority-now-ms nil
                        :lease-local-deadline-ms nil
                        :lease-local-deadline-nanos nil}
                       2000)]
    (is (= 5000 (:ha-lease-local-deadline-ms same-lease)))
    (is (= 5000000 (:ha-lease-local-deadline-nanos same-lease)))
    (is (nil? (:ha-lease-local-deadline-ms changed-lease)))
    (is (nil? (:ha-lease-local-deadline-nanos changed-lease)))))

(deftest write-admission-rejects-unknown-local-lease-deadline-test
  (with-redefs [ha/ha-now-ms (constantly 5000)
                ha/ha-now-nanos (constantly 5000000)]
    (let [error (ha/ha-write-admission-error
                 {"db" {:ha-authority ::authority
                        :ha-role :leader
                        :ha-node-id 1
                        :ha-authority-owner-node-id 1
                        :ha-members [{:node-id 1
                                      :endpoint "127.0.0.1:19001"}]
                        :ha-authority-lease {:leader-node-id 1
                                             :leader-endpoint "127.0.0.1:19001"
                                             :term 1
                                             :lease-until-ms 9000}
                        :ha-authority-read-ok? true
                        :ha-last-authority-refresh-ms 5000
                        :ha-lease-timeout-ms 3000
                        :ha-lease-local-deadline-ms nil
                        :ha-lease-local-deadline-nanos nil
                        :ha-leader-term 1
                        :ha-authority-term 1
                        :ha-lease-renew-ms 1000}}
                 {:type :tx-data
                  :args ["db"]})]
      (is (= :ha/write-rejected (:error error)))
      (is (= :lease-deadline-unknown (:reason error))))))

(deftest follower-side-effect-patch-carries-replay-invariants-test
  (let [expected {:ha-role :follower}
        next (assoc expected
                    :ha-follower-last-applied-term 3
                    :ha-follower-source-last-applied-lsn-known? true
                    :ha-follower-source-last-applied-lsn 22
                    :ha-follower-source-order-dynamic? true
                    :ha-follower-source-order-authority-version 9)
        patch (sha/ha-follower-side-effect-patch expected next)]
    (is (= 3 (:ha-follower-last-applied-term patch)))
    (is (true? (:ha-follower-source-last-applied-lsn-known? patch)))
    (is (= 22 (:ha-follower-source-last-applied-lsn patch)))
    (is (true? (:ha-follower-source-order-dynamic? patch)))
    (is (= 9 (:ha-follower-source-order-authority-version patch)))))

(deftest follower-sync-loop-sleep-throttles-caught-up-or-tiny-batches-test
  (let [base {:ha-role :follower
              :ha-lease-renew-ms 1000}]
    (is (= 250 (sha/ha-follower-loop-sleep-ms
                (assoc base :ha-follower-last-batch-size 0)
                1000)))
    (is (= 0 (sha/ha-follower-loop-sleep-ms
              (assoc base
                     :ha-follower-last-batch-size 8
                     :ha-follower-requested-batch-records 8)
              1000)))
    (is (= 1 (sha/ha-follower-loop-sleep-ms
              (assoc base
                     :ha-follower-last-batch-size 1
                     :ha-follower-requested-batch-records 1)
              1000)))
    (is (= 250 (sha/ha-follower-loop-sleep-ms
                (assoc base
                       :ha-follower-last-batch-size 1
                       :ha-follower-requested-batch-records 8)
                1000)))
    (is (= 250 (sha/ha-follower-loop-sleep-ms
                (assoc base
                       :ha-follower-last-batch-size 8
                       :ha-follower-requested-batch-records 8
                       :ha-follower-source-last-applied-lsn-known? true
                       :ha-follower-source-last-applied-lsn 17
                       :ha-follower-next-lsn 18)
                1000)))
    (is (= 1200 (sha/ha-follower-loop-sleep-ms
                 (assoc base
                        :ha-follower-last-batch-size 8
                        :ha-follower-requested-batch-records 8
                        :ha-follower-next-sync-not-before-ms 2200)
                 1000)))))

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

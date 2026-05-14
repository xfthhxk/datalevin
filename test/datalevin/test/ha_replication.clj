(ns datalevin.test.ha-replication
  (:require
   [clojure.test :refer [deftest is]]
   [datalevin.constants :as c]
   [datalevin.core :as d]
   [datalevin.ha.replication :as drep]
   [datalevin.ha.replication.bootstrap :as boot]
   [datalevin.ha.replication.store :as store]
   [datalevin.interface :as i]
   [datalevin.kv :as kv]
   [datalevin.util :as u])
  (:import
   [java.util UUID]))

(deftest next-ha-follower-sync-lsn-clamps-to-local-floor-test
  (is (= 15 (#'drep/next-ha-follower-sync-lsn 15 23)))
  (is (= 10 (#'drep/next-ha-follower-sync-lsn 15 10)))
  (is (= 15 (#'drep/next-ha-follower-sync-lsn 15 nil))))

(deftest explicit-local-kv-store-bypasses-dynamic-current-state-test
  (let [old-store (reify i/ILMDB
                    (closed-kv? [_] false))
        installed-store (reify i/ILMDB
                          (closed-kv? [_] false))]
    (binding [store/*ha-current-state-fn* (fn [] {:store old-store})]
      (is (identical? old-store
                      (store/raw-local-kv-store {:store installed-store})))
      (is (identical? installed-store
                      (store/explicit-raw-local-kv-store
                       {:store installed-store}))))))

(deftest gap-bootstrap-next-lsn-uses-first-retained-source-lsn-test
  (is (= 24 (#'drep/ha-gap-bootstrap-next-lsn
             20
             {:error :ha/txlog-gap-unresolved
              :gap-errors [{:data {:error :ha/txlog-gap
                                   :expected-lsn 20
                                   :actual-lsn 24}}
                           {:data {:error :ha/txlog-gap
                                   :expected-lsn 20
                                   :actual-lsn 27}}]})))
  (is (= 20 (#'drep/ha-gap-bootstrap-next-lsn
             20
             {:error :ha/txlog-source-behind
              :source-last-applied-lsn 18}))))

(deftest empty-follower-batch-advances-from-local-floor-test
  (let [reported-floor (atom nil)
        fetched-range  (atom nil)
        m              {:ha-node-id 2
                        :ha-local-last-applied-lsn 14
                        :ha-follower-next-lsn 23
                        :ha-follower-max-batch-records 8}
        lease          {:leader-endpoint "127.0.0.1:19001"
                        :leader-last-applied-lsn 22}]
    (with-redefs-fn
      {#'drep/reopen-ha-local-store-if-needed identity
       #'drep/fetch-ha-follower-records-with-gap-fallback
       (fn [_db-name _m _lease next-lsn upto-lsn]
         (reset! fetched-range [next-lsn upto-lsn])
         {:records []
          :source-endpoint "127.0.0.1:19001"
          :source-order ["127.0.0.1:19001"]
          :source-order-dynamic? false
          :source-last-applied-lsn-known? true
          :source-last-applied-lsn 22})
       #'drep/report-ha-replica-floor!
       (fn [_db-name _m leader-endpoint applied-lsn]
         (reset! reported-floor [leader-endpoint applied-lsn]))
       #'drep/refresh-ha-local-dt-db identity}
      (fn []
        (let [{:keys [applied-lsn state]}
              (#'drep/sync-ha-follower-batch "db" m lease 23 1000)]
          (is (= [23 30] @fetched-range))
          (is (= ["127.0.0.1:19001" 14] @reported-floor))
          (is (= 14 applied-lsn))
          (is (= 14 (:ha-local-last-applied-lsn state)))
          (is (= 15 (:ha-follower-next-lsn state)))
          (is (true? (:ha-follower-source-last-applied-lsn-known? state)))
          (is (= 22 (:ha-follower-source-last-applied-lsn state))))))))

(deftest empty-follower-batch-uses-fresh-materialized-floor-test
  (let [reported-floor (atom nil)
        m              {:ha-node-id 2
                        :ha-local-last-applied-lsn 29
                        :ha-follower-next-lsn 30
                        :ha-follower-max-batch-records 8}
        lease          {:leader-endpoint "127.0.0.1:19001"
                        :leader-last-applied-lsn 29}]
    (with-redefs-fn
      {#'drep/reopen-ha-local-store-if-needed identity
       #'drep/fetch-ha-follower-records-with-gap-fallback
       (fn [_db-name _m _lease _next-lsn _upto-lsn]
         {:records []
          :source-endpoint "127.0.0.1:19001"
          :source-order ["127.0.0.1:19001"]
          :source-order-dynamic? false
          :source-last-applied-lsn-known? true
          :source-last-applied-lsn 29})
       #'drep/read-ha-local-last-applied-lsn
       (constantly 21)
       #'drep/report-ha-replica-floor!
       (fn [_db-name _m leader-endpoint applied-lsn]
         (reset! reported-floor [leader-endpoint applied-lsn]))
       #'drep/refresh-ha-local-dt-db identity}
      (fn []
        (let [{:keys [applied-lsn state]}
              (#'drep/sync-ha-follower-batch "db" m lease 30 1000)]
          (is (= ["127.0.0.1:19001" 21] @reported-floor))
          (is (= 21 applied-lsn))
          (is (= 21 (:ha-local-last-applied-lsn state)))
          (is (= 22 (:ha-follower-next-lsn state))))))))

(deftest empty-follower-batch-preserves-bootstrap-snapshot-floor-test
  (let [reported-floor (atom nil)
        m              {:ha-node-id 2
                        :ha-local-last-applied-lsn 27
                        :ha-follower-next-lsn 28
                        :ha-follower-max-batch-records 8
                        :ha-follower-last-bootstrap-ms 1000
                        :ha-follower-bootstrap-source-endpoint
                        "127.0.0.1:19001"
                        :ha-follower-bootstrap-snapshot-last-applied-lsn
                        27}
        lease          {:leader-endpoint "127.0.0.1:19001"
                        :leader-last-applied-lsn 27}]
    (with-redefs-fn
      {#'drep/reopen-ha-local-store-if-needed identity
       #'drep/fetch-ha-follower-records-with-gap-fallback
       (fn [_db-name _m _lease _next-lsn _upto-lsn]
         {:records []
          :source-endpoint "127.0.0.1:19001"
          :source-order ["127.0.0.1:19001"]
          :source-order-dynamic? false
          :source-last-applied-lsn-known? true
          :source-last-applied-lsn 27})
       #'drep/read-ha-local-last-applied-lsn
       (constantly 21)
       #'drep/report-ha-replica-floor!
       (fn [_db-name _m leader-endpoint applied-lsn]
         (reset! reported-floor [leader-endpoint applied-lsn]))
       #'drep/refresh-ha-local-dt-db identity}
      (fn []
        (let [{:keys [applied-lsn state]}
              (#'drep/sync-ha-follower-batch "db" m lease 28 1250)]
          (is (= ["127.0.0.1:19001" 27] @reported-floor))
          (is (= 27 applied-lsn))
          (is (= 27 (:ha-local-last-applied-lsn state)))
          (is (= 28 (:ha-follower-next-lsn state))))))))

(deftest bootstrap-tail-clamps-to-contiguous-materialized-prefix-test
  (let [dir (u/tmp-dir (str "ha-bootstrap-tail-test-" (UUID/randomUUID)))]
    (try
      (let [db (d/open-kv dir {:wal? true})]
        (try
          (d/open-dbi db "a")
          (dotimes [i 10]
            (is (= :transacted
                   (d/transact-kv db [[:put "a" i i]]))))
          ;; Simulate a copied follower store with a persisted HA floor ahead
          ;; of the materialized WAL tail. The bootstrap verifier must clamp
          ;; to the last contiguous record instead of trusting the floor.
          (i/transact-kv (kv/raw-lmdb db)
                         c/kv-info
                         [[:put c/ha-local-applied-lsn 16]]
                         :keyword
                         :data)
          (let [result (#'boot/inspect-ha-local-bootstrap-tail
                        {:store db}
                        8
                        16)]
            (is (= 16 (:candidate-floor-lsn result)))
            (is (= 10 (:tail-last-lsn result)))
            (is (false? (:tail-complete? result)))
            (is (= 10 (:verified-floor-lsn result))))
          (finally
            (d/close-kv db))))
      (finally
        (u/delete-files dir)))))

(deftest reconcile-bootstrap-tail-replays-contiguous-records-test
  (let [dir (u/tmp-dir (str "ha-bootstrap-tail-replay-test-"
                            (UUID/randomUUID)))]
    (try
      (let [db (d/open-kv dir {:wal? true})]
        (try
          (d/open-dbi db "a")
          (dotimes [i 10]
            (is (= :transacted
                   (d/transact-kv db [[:put "a" i i]]))))
          ;; The copied LMDB can contain a local WAL tail past the copied
          ;; payload marker. Bootstrap must replay that tail before publishing
          ;; the follower floor.
          (i/transact-kv (kv/raw-lmdb db)
                         c/kv-info
                         [[:put c/wal-local-payload-lsn 8]]
                         :keyword
                         :data)
          (let [applied-lsns (atom [])
                result (#'boot/reconcile-ha-installed-snapshot-state
                        {:ha-role :follower
                         :store db}
                        8
                        10
                        (fn [state record]
                          (swap! applied-lsns conj (long (:lsn record)))
                          state))]
            (is (= [9 10] @applied-lsns))
            (is (= 10 (:installed-lsn result)))
            (is (= 10 (get-in result [:state :ha-local-last-applied-lsn])))
            (is (= 11 (get-in result [:state :ha-follower-next-lsn]))))
          (finally
            (d/close-kv db))))
      (finally
        (u/delete-files dir)))))

(deftest bootstrap-tail-trusts-copied-payload-floor-test
  (let [dir (u/tmp-dir (str "ha-bootstrap-payload-test-" (UUID/randomUUID)))]
    (try
      (let [db (d/open-kv dir)]
        (try
          (i/transact-kv db
                         c/kv-info
                         [[:put c/wal-local-payload-lsn 16]]
                         :keyword
                         :data)
          (let [result (#'boot/inspect-ha-local-bootstrap-tail
                        {:store db}
                        8
                        16)]
            (is (= 16 (:candidate-floor-lsn result)))
            (is (= 16 (:tail-last-lsn result)))
            (is (true? (:tail-complete? result)))
            (is (= 16 (:verified-floor-lsn result))))
          (finally
            (d/close-kv db))))
      (finally
        (u/delete-files dir)))))

(deftest follower-local-applied-lsn-clamps-to-payload-floor-test
  (let [dir (u/tmp-dir (str "ha-follower-payload-floor-test-"
                            (UUID/randomUUID)))]
    (try
      (let [db (d/open-kv dir {:wal? true})]
        (try
          (d/open-dbi db "a")
          (dotimes [i 10]
            (is (= :transacted
                   (d/transact-kv db [[:put "a" i i]]))))
          ;; Simulate a follower that durably appended/recovered WAL through
          ;; LSN 10, but only materialized payload rows through LSN 7.
          (i/transact-kv (kv/raw-lmdb db)
                         c/kv-info
                         [[:put c/wal-local-payload-lsn 7]]
                         :keyword
                         :data)
          (let [m {:ha-role :follower
                   :store db
                   :ha-local-last-applied-lsn 10
                   :ha-follower-next-lsn 11}]
            (is (= 7 (store/read-ha-local-last-applied-lsn m)))
            (is (= 7 (store/ha-local-last-applied-lsn m))))
        (finally
          (d/close-kv db))))
      (finally
        (u/delete-files dir)))))

(deftest follower-local-applied-lsn-trusts-copied-payload-floor-test
  (let [dir (u/tmp-dir (str "ha-follower-copied-payload-floor-test-"
                            (UUID/randomUUID)))]
    (try
      (let [db (d/open-kv dir {:wal? true})]
        (try
          (d/open-dbi db "a")
          (dotimes [i 10]
            (is (= :transacted
                   (d/transact-kv db [[:put "a" i i]]))))
          ;; A snapshot-installed follower can copy payload state beyond the
          ;; local txlog watermark. The copied payload marker must remain the
          ;; follower floor instead of being clamped back to the old watermark.
          (i/transact-kv (kv/raw-lmdb db)
                         c/kv-info
                         [[:put c/ha-local-applied-lsn 16]
                          [:put c/wal-local-payload-lsn 16]]
                         :keyword
                         :data)
          (let [m {:ha-role :follower
                   :store db
                   :ha-local-last-applied-lsn 16
                   :ha-follower-next-lsn 17}]
            (is (= 16 (store/read-ha-local-last-applied-lsn m)))
            (is (= 16 (store/ha-local-last-applied-lsn m))))
        (finally
          (d/close-kv db))))
      (finally
        (u/delete-files dir)))))

(deftest replication-reconcile-wrapper-accepts-apply-function-test
  (let [result (#'drep/reconcile-ha-installed-snapshot-state
                {:store :s}
                8
                10
                (fn [state _record] state))]
    (is (= 8 (:installed-lsn result)))
    (is (= {:store :s} (:state result)))
    (is (= 10 (:trusted-max-lsn result)))))

(deftest bootstrap-uses-installed-local-floor-not-manifest-materialized-floor-test
  (let [seen-reconcile (atom nil)
        result
        (boot/bootstrap-ha-follower-from-snapshot*
         {:normalize-ha-bootstrap-retry-state
          (fn [candidate-m _fallback-m _reopen-info]
            candidate-m)
          :ha-local-store-reopen-info
          (constantly nil)
          :fetch-ha-endpoint-snapshot-copy!
          (fn [_db-name _m _source-endpoint _snapshot-dir]
            {:copy-meta {:snapshot-last-applied-lsn 16
                         :payload-last-applied-lsn 16
                         :txlog-last-applied-lsn 23}})
          :validate-ha-snapshot-copy!
          (fn [_db-name _m _source-endpoint _snapshot-dir copy-meta _required-lsn]
            copy-meta)
          :install-ha-local-snapshot!
          (fn [m _snapshot-dir]
            {:ok? true
             :state (assoc m :installed? true)})
          :raw-local-kv-store
          (constantly ::kv)
          :read-ha-local-snapshot-current-lsn
          (constantly 8)
          :reconcile-ha-installed-snapshot-state
          (fn [state snapshot-lsn trusted-install-lsn]
            (reset! seen-reconcile [snapshot-lsn trusted-install-lsn])
            {:state state
             :installed-lsn snapshot-lsn})
          :persist-ha-local-applied-lsn!
          (fn [_state installed-lsn]
            installed-lsn)
          :note-ha-bootstrap-installed-state
          (fn [state installed-lsn source-endpoint snapshot-lsn now-ms
               persisted-installed-lsn]
            (assoc state
                   :noted [installed-lsn
                           source-endpoint
                           snapshot-lsn
                           now-ms
                           persisted-installed-lsn]))
          :sync-ha-follower-batch
          (fn [_db-name state _lease next-lsn _now-ms]
            {:state (assoc state :resume-next-lsn next-lsn)})}
         "db"
         {:initial? true}
         {:leader-endpoint "127.0.0.1:19001"}
         ["127.0.0.1:19001"]
         9
         1234)]
    (is (true? (:ok? result)))
    (is (= [8 23] @seen-reconcile))
    (is (= 9 (get-in result [:state :resume-next-lsn])))
    (is (= [8 "127.0.0.1:19001" 8 1234 8]
           (get-in result [:state :noted])))))

(deftest bootstrap-preserves-replayed-tail-term-test
  (let [seen-apply-fn? (atom false)
        result
        (boot/bootstrap-ha-follower-from-snapshot*
         {:normalize-ha-bootstrap-retry-state
          (fn [candidate-m _fallback-m _reopen-info]
            candidate-m)
          :ha-local-store-reopen-info
          (constantly nil)
          :fetch-ha-endpoint-snapshot-copy!
          (fn [_db-name _m _source-endpoint _snapshot-dir]
            {:copy-meta {:snapshot-last-applied-lsn 8
                         :payload-last-applied-lsn 8
                         :txlog-last-applied-lsn 10}})
          :validate-ha-snapshot-copy!
          (fn [_db-name _m _source-endpoint _snapshot-dir copy-meta _required-lsn]
            copy-meta)
          :install-ha-local-snapshot!
          (fn [m _snapshot-dir]
            {:ok? true
             :state (assoc m :installed? true)})
          :raw-local-kv-store
          (constantly ::kv)
          :read-ha-local-snapshot-current-lsn
          (constantly 8)
          :reconcile-ha-installed-snapshot-state
          (fn [state snapshot-lsn trusted-install-lsn apply-record-fn]
            (reset! seen-apply-fn? (fn? apply-record-fn))
            {:state (assoc state
                           :snapshot-lsn snapshot-lsn
                           :trusted-install-lsn trusted-install-lsn
                           :ha-follower-last-applied-term 3)
             :installed-lsn 10
             :replayed-last-term 3})
          :persist-ha-local-applied-lsn!
          (fn [_state installed-lsn]
            installed-lsn)
          :note-ha-bootstrap-installed-state
          (fn [state installed-lsn source-endpoint snapshot-lsn now-ms
               persisted-installed-lsn]
            (assoc state
                   :ha-follower-last-applied-term nil
                   :noted [installed-lsn
                           source-endpoint
                           snapshot-lsn
                           now-ms
                           persisted-installed-lsn]))
          :apply-ha-follower-record!
          (fn [state _record] state)
          :sync-ha-follower-batch
          (fn [_db-name state _lease next-lsn _now-ms]
            {:state (assoc state :resume-next-lsn next-lsn)})}
         "db"
         {:initial? true}
         {:leader-endpoint "127.0.0.1:19001"}
         ["127.0.0.1:19001"]
         9
         1234)]
    (is (true? (:ok? result)))
    (is (true? @seen-apply-fn?))
    (is (= 3 (get-in result [:state :ha-follower-last-applied-term])))
    (is (= 11 (get-in result [:state :resume-next-lsn])))
    (is (= [10 "127.0.0.1:19001" 10 1234 10]
           (get-in result [:state :noted])))))

(deftest bootstrap-rejects-installed-copy-below-required-floor-test
  (let [sync-called? (atom false)
        result
        (boot/bootstrap-ha-follower-from-snapshot*
         {:normalize-ha-bootstrap-retry-state
          (fn [candidate-m _fallback-m _reopen-info]
            candidate-m)
          :ha-local-store-reopen-info
          (constantly nil)
          :fetch-ha-endpoint-snapshot-copy!
          (fn [_db-name _m _source-endpoint _snapshot-dir]
            {:copy-meta {:snapshot-last-applied-lsn 16
                         :payload-last-applied-lsn 16
                         :txlog-last-applied-lsn 16}})
          :validate-ha-snapshot-copy!
          (fn [_db-name _m _source-endpoint _snapshot-dir copy-meta _required-lsn]
            copy-meta)
          :install-ha-local-snapshot!
          (fn [m _snapshot-dir]
            {:ok? true
             :state (assoc m :installed? true)})
          :raw-local-kv-store
          (constantly ::kv)
          :read-ha-local-snapshot-current-lsn
          (constantly 8)
          :reconcile-ha-installed-snapshot-state
          (fn [state snapshot-lsn trusted-install-lsn]
            {:state (assoc state
                           :snapshot-lsn snapshot-lsn
                           :trusted-install-lsn trusted-install-lsn)
             :installed-lsn 8
             :verified-floor-lsn 8})
          :persist-ha-local-applied-lsn!
          (fn [_state installed-lsn]
            installed-lsn)
          :note-ha-bootstrap-installed-state
          (fn [state installed-lsn source-endpoint snapshot-lsn now-ms
               persisted-installed-lsn]
            (assoc state
                   :noted [installed-lsn
                           source-endpoint
                           snapshot-lsn
                           now-ms
                           persisted-installed-lsn]))
          :sync-ha-follower-batch
          (fn [& _args]
            (reset! sync-called? true)
            {:state {}})}
         "db"
         {:initial? true}
         {:leader-endpoint "127.0.0.1:19001"}
         ["127.0.0.1:19001"]
         12
         1234)]
    (is (false? (:ok? result)))
    (is (false? @sync-called?))
    (is (= :ha/follower-snapshot-installed-too-stale
           (get-in result [:errors 0 :error])))
    (is (= 11 (get-in result [:errors 0 :data :required-lsn])))
    (is (= 16 (get-in result [:errors 0 :data :snapshot-last-applied-lsn])))
    (is (= 8 (get-in result
                     [:errors 0 :data :local-snapshot-last-applied-lsn])))
    (is (= 8 (get-in result [:errors 0 :data :installed-last-applied-lsn])))))

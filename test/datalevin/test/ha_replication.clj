(ns datalevin.test.ha-replication
  (:require
   [clojure.test :refer [deftest is]]
   [datalevin.constants :as c]
   [datalevin.core :as d]
   [datalevin.db :as db]
   [datalevin.ha.replication :as drep]
   [datalevin.ha.replication.bootstrap :as boot]
   [datalevin.ha.replication.store :as store]
   [datalevin.interface :as i]
   [datalevin.kv :as kv]
   [datalevin.storage :as st]
   [datalevin.util :as u])
  (:import
   [datalevin.db DB]
   [datalevin.storage Store]
   [java.util UUID]))

(defn- conn-store
  [conn]
  (.-store ^DB @conn))

(defn- fake-info-kv-store
  [values]
  (reify i/ILMDB
    (closed-kv? [_] false)
    (get-value [_ dbi-name k]
      (get values [dbi-name k]))
    (get-value [_ dbi-name k _k-type]
      (get values [dbi-name k]))
    (get-value [_ dbi-name k _k-type _v-type]
      (get values [dbi-name k]))
    (get-value [_ dbi-name k _k-type _v-type _ignore-key?]
      (get values [dbi-name k]))))

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

(deftest follower-lsn-cap-respects-authority-confirmed-lsn-test
  (let [cap #'store/cap-follower-lsn-to-authority]
    (is (= 17 (cap {:ha-role :follower
                    :ha-authority-lease {:leader-last-applied-lsn 17}}
                   18)))
    (is (= 18 (cap {:ha-role :leader
                    :ha-authority-lease {:leader-last-applied-lsn 17}}
                   18)))
    (is (= 18 (cap {:ha-role :follower}
                   18)))))

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

(deftest failed-gap-bootstrap-backs-off-follower-sync-test
  (let [now-ms 10000
        m      {:ha-role :follower
                :ha-node-id 2
                :ha-local-last-applied-lsn 12
                :ha-follower-next-lsn 13
                :ha-follower-last-batch-size 8
                :ha-follower-sync-backoff-ms 500
                :ha-lease-renew-ms 1000
                :ha-authority-version 42}
        details {:message "Follower txlog gap unresolved and snapshot bootstrap failed"
                 :data {:error :ha/follower-snapshot-bootstrap-failed}}]
    (let [state (#'drep/assoc-ha-follower-gap-bootstrap-failure
                 m
                 now-ms
                 ["127.0.0.1:19001"]
                 false
                 (:ha-authority-version m)
                 details)]
      (is (= :sync-failed (:ha-follower-last-error state)))
      (is (true? (:ha-follower-degraded? state)))
      (is (= :wal-gap (:ha-follower-degraded-reason state)))
      (is (= 0 (:ha-follower-last-batch-size state)))
      (is (nil? (:ha-follower-last-batch-records state)))
      (is (= 1000 (:ha-follower-sync-backoff-ms state)))
      (is (= 11000 (:ha-follower-next-sync-not-before-ms state)))
      (is (= 42 (:ha-follower-source-order-authority-version state))))))

(deftest empty-follower-batch-accepts-fresh-bootstrap-floor-test
  (let [source-endpoint "127.0.0.1:19001"
        m               {:ha-node-id 2
                         :ha-local-last-applied-lsn 18
                         :ha-follower-next-lsn 19
                         :ha-follower-last-bootstrap-ms 1000
                         :ha-follower-bootstrap-source-endpoint
                         source-endpoint
                         :ha-follower-bootstrap-snapshot-last-applied-lsn
                         18}
        lease           {:leader-endpoint source-endpoint
                         :leader-last-applied-lsn 17}]
    (is (true? (#'drep/bootstrap-floor-covers-source-behind?
                m source-endpoint 19 17)))
    (is (false? (#'drep/bootstrap-floor-covers-source-behind?
                 (assoc m :ha-follower-bootstrap-source-endpoint "other")
                 source-endpoint
                 19
                 17)))
    (with-redefs-fn
      {#'drep/ha-follower-source-endpoints
       (fn [_m _lease] [source-endpoint])
       #'drep/ha-leader-endpoint
       (fn [_m _lease] source-endpoint)
       #'drep/fetch-leader-watermark-lsn
       (fn [& _] {:reachable? true :last-applied-lsn 17})
       #'drep/fetch-ha-leader-txlog-batch
       (fn [& _] [])
       #'drep/ha-source-advertised-last-applied-lsn
       (fn [& _] {:known? true :last-applied-lsn 17})}
      (fn []
        (let [result (#'drep/fetch-ha-follower-records-with-gap-fallback
                      "db" m lease 19 26)]
          (is (= [] (:records result)))
          (is (= source-endpoint (:source-endpoint result)))
          (is (= [source-endpoint] (:source-order result)))
          (is (true? (:source-last-applied-lsn-known? result)))
          (is (= 17 (:source-last-applied-lsn result))))))))

(deftest raw-open-opts-persist-without-consuming-txlog-lsn-test
  (let [dir (u/tmp-dir (str "ha-raw-open-opts-test-" (UUID/randomUUID)))]
    (try
      (let [store (st/open dir nil {:db-name "ha-raw-open"
                                    :wal? true
                                    :cache-limit 512})]
        (try
          (let [lmdb (.-lmdb ^Store store)
                before-lsn (long (or (:last-applied-lsn
                                      (kv/txlog-watermarks lmdb))
                                     0))]
            (i/close store)
            (let [reopened (st/open
                            dir
                            nil
                            {:db-name "ha-raw-open"
                             :wal? true
                             :cache-limit 1024
                             :datalevin.storage/raw-persist-open-opts?
                             true})]
              (try
                (let [reopened-lmdb (.-lmdb ^Store reopened)
                      after-lsn (long (or (:last-applied-lsn
                                           (kv/txlog-watermarks
                                            reopened-lmdb))
                                          0))]
                  (is (= before-lsn after-lsn))
                  (is (= 1024 (:cache-limit (i/opts reopened)))))
                (finally
                  (i/close reopened)))))
          (finally
            (when-not (i/closed? store)
              (i/close store)))))
      (finally
        (u/delete-files dir)))))

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

(deftest follower-local-applied-lsn-ignores-snapshot-retention-floor-test
  (let [dir (u/tmp-dir (str "ha-follower-snapshot-retention-floor-test-"
                            (UUID/randomUUID)))]
    (try
      (let [db (d/open-kv dir {:wal? true})]
        (try
          ;; The snapshot marker can advance as a txlog retention floor. It
          ;; must not make a follower report unapplied datalog rows as local.
          (i/transact-kv (kv/raw-lmdb db)
                         c/kv-info
                         [[:put c/ha-local-applied-lsn 16]
                          [:put c/wal-snapshot-current-lsn 16]
                          [:put c/wal-local-payload-lsn 8]]
                         :keyword
                         :data)
          (let [m {:ha-role :follower
                   :store db
                   :ha-local-last-applied-lsn 16
                   :ha-follower-next-lsn 17}]
            (is (= 8 (store/read-ha-local-last-applied-lsn m)))
            (is (= 8 (store/ha-local-last-applied-lsn m))))
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

(deftest follower-replay-cleans-divergent-cardinality-one-values-test
  (let [source-dir   (u/tmp-dir (str "ha-follower-replay-source-"
                                     (UUID/randomUUID)))
        follower-dir (u/tmp-dir (str "ha-follower-replay-follower-"
                                     (UUID/randomUUID)))
        schema       {:register/key   {:db/valueType :db.type/long
                                       :db/unique :db.unique/identity}
                      :register/value {:db/valueType :db.type/long}}]
    (try
      (let [source   (d/create-conn source-dir schema {:wal? true})
            follower (d/create-conn follower-dir schema {:wal? true})]
        (try
          (d/transact! source [{:db/id 1
                                :register/key 0
                                :register/value 1}])
          (d/transact! follower [{:db/id 1
                                  :register/key 0
                                  :register/value 3}])
          (d/transact! source [{:db/id 1
                                :register/key 0
                                :register/value 1004}])
          (let [source-store   (conn-store source)
                follower-store (conn-store follower)
                source-kv      (store/raw-local-kv-store
                                {:store source-store})
                record         (last (kv/open-tx-log-rows source-kv 1 20))]
            (is (some? record))
            (when record
              (#'drep/apply-ha-follower-txlog-record!
               {:store follower-store
                :ha-node-id 2
                :ha-authority-term 1}
               record)
              (let [fresh-db (db/new-db follower-store)]
                (is (= #{[0 1004]}
                       (set
                        (d/q '[:find ?k ?v
                               :where
                               [?e :register/key ?k]
                               [?e :register/value ?v]]
                             fresh-db)))))))
          (finally
            (d/close source)
            (d/close follower))))
      (finally
        (u/delete-files source-dir)
        (u/delete-files follower-dir)))))

(deftest replication-reconcile-wrapper-accepts-apply-function-test
  (let [result (#'drep/reconcile-ha-installed-snapshot-state
                {:store :s}
                8
                10
                (fn [state _record] state))]
    (is (= 8 (:installed-lsn result)))
    (is (= {:store :s} (:state result)))
    (is (= 10 (:trusted-max-lsn result)))))

(deftest bootstrap-validation-does-not-use-snapshot-retention-floor-test
  (let [err (try
              (boot/validate-ha-snapshot-copy!
               "db"
               {:ha-db-identity "identity"}
               "127.0.0.1:19001"
               "/unused"
               {:db-name "db"
                :db-identity "identity"
                :snapshot-last-applied-lsn 16
                :payload-last-applied-lsn 8
                :txlog-last-applied-lsn 8}
               12)
              nil
              (catch clojure.lang.ExceptionInfo e
                (ex-data e)))]
    (is (= :ha/follower-snapshot-too-stale (:error err)))
    (is (= 12 (:required-lsn err)))
    (is (= 16 (:snapshot-last-applied-lsn err)))
    (is (= 8 (:payload-last-applied-lsn err)))
    (is (= 8 (:txlog-last-applied-lsn err)))))

(deftest bootstrap-snapshot-uses-only-current-leader-source-test
  (let [attempted-sources (atom [])
        noted-source (atom nil)
        result
        (boot/bootstrap-ha-follower-from-snapshot*
         {:normalize-ha-bootstrap-retry-state
          (fn [candidate-m _fallback-m _reopen-info]
            candidate-m)
          :ha-local-store-reopen-info
          (constantly nil)
          :fetch-ha-endpoint-snapshot-copy!
          (fn [_db-name _m source-endpoint _snapshot-dir]
            (swap! attempted-sources conj source-endpoint)
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
          (constantly (fake-info-kv-store
                       {[c/kv-info c/wal-local-payload-lsn] 8}))
          :read-ha-local-snapshot-current-lsn
          (constantly 8)
          :reconcile-ha-installed-snapshot-state
          (fn [state snapshot-lsn trusted-install-lsn]
            {:state (assoc state
                           :snapshot-lsn snapshot-lsn
                           :trusted-install-lsn trusted-install-lsn)
             :installed-lsn 10})
          :persist-ha-local-applied-lsn!
          (fn [_state installed-lsn]
            installed-lsn)
          :note-ha-bootstrap-installed-state
          (fn [state installed-lsn source-endpoint snapshot-lsn now-ms
               persisted-installed-lsn]
            (reset! noted-source source-endpoint)
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
         {:leader-endpoint "leader"}
         ["follower" "leader"]
         9
         1234)]
    (is (true? (:ok? result)))
    (is (= ["leader"] @attempted-sources))
    (is (= "leader" @noted-source))
    (is (= "leader"
           (get-in result [:state :ha-follower-bootstrap-source-endpoint])))
    (is (= 11 (get-in result [:state :resume-next-lsn])))))

(deftest bootstrap-snapshot-rejects-source-order-without-leader-test
  (let [attempted-sources (atom [])
        result
        (boot/bootstrap-ha-follower-from-snapshot*
         {:normalize-ha-bootstrap-retry-state
          (fn [candidate-m _fallback-m _reopen-info]
            candidate-m)
          :ha-local-store-reopen-info
          (constantly nil)
          :fetch-ha-endpoint-snapshot-copy!
          (fn [_db-name _m source-endpoint _snapshot-dir]
            (swap! attempted-sources conj source-endpoint)
            {:copy-meta {:snapshot-last-applied-lsn 8
                         :payload-last-applied-lsn 8
                         :txlog-last-applied-lsn 10}})
          :validate-ha-snapshot-copy!
          (fn [_db-name _m _source-endpoint _snapshot-dir copy-meta _required-lsn]
            copy-meta)
          :install-ha-local-snapshot!
          (fn [m _snapshot-dir]
            {:ok? true
             :state m})
          :raw-local-kv-store
          (constantly (fake-info-kv-store
                       {[c/kv-info c/wal-local-payload-lsn] 8}))
          :read-ha-local-snapshot-current-lsn
          (constantly 8)
          :reconcile-ha-installed-snapshot-state
          (fn [state _snapshot-lsn _trusted-install-lsn]
            {:state state
             :installed-lsn 10})
          :persist-ha-local-applied-lsn!
          (fn [_state installed-lsn]
            installed-lsn)
          :note-ha-bootstrap-installed-state
          (fn [state _installed-lsn _source-endpoint _snapshot-lsn _now-ms
               _persisted-installed-lsn]
            state)
          :sync-ha-follower-batch
          (fn [_db-name state _lease _next-lsn _now-ms]
            {:state state})}
         "db"
         {:initial? true}
         {:leader-endpoint "leader"}
         ["follower"]
         9
         1234)]
    (is (false? (:ok? result)))
    (is (empty? @attempted-sources))
    (is (= [] (:source-order result)))
    (is (= ["follower"] (:candidate-source-order result)))
    (is (= :ha/follower-snapshot-leader-source-unavailable
           (get-in result [:errors 0 :error])))
    (is (= "leader"
           (get-in result [:errors 0 :data :leader-endpoint])))))

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
          (constantly (fake-info-kv-store
                       {[c/kv-info c/wal-local-payload-lsn] 8}))
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

(deftest bootstrap-uses-local-payload-floor-not-snapshot-retention-floor-test
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
                         :payload-last-applied-lsn 8
                         :txlog-last-applied-lsn 16}})
          :validate-ha-snapshot-copy!
          (fn [_db-name _m _source-endpoint _snapshot-dir copy-meta _required-lsn]
            copy-meta)
          :install-ha-local-snapshot!
          (fn [m _snapshot-dir]
            {:ok? true
             :state (assoc m :installed? true)})
          :raw-local-kv-store
          (constantly (fake-info-kv-store
                       {[c/kv-info c/wal-local-payload-lsn] 8}))
          :read-ha-local-snapshot-current-lsn
          (constantly 16)
          :reconcile-ha-installed-snapshot-state
          (fn [state materialized-lsn trusted-install-lsn]
            (reset! seen-reconcile [materialized-lsn trusted-install-lsn])
            {:state state
             :installed-lsn 16})
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
    (is (= [8 16] @seen-reconcile))
    (is (= 17 (get-in result [:state :resume-next-lsn])))
    (is (= [16 "127.0.0.1:19001" 16 1234 16]
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
          (constantly (fake-info-kv-store
                       {[c/kv-info c/wal-local-payload-lsn] 8}))
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

(deftest bootstrap-caps-required-floor-to-authority-lsn-test
  (let [trusted-reconcile (atom nil)
        resume-next-lsn (atom nil)
        source-endpoint "127.0.0.1:19001"
        result
        (boot/bootstrap-ha-follower-from-snapshot*
         {:normalize-ha-bootstrap-retry-state
          (fn [candidate-m _fallback-m _reopen-info]
            candidate-m)
          :ha-local-store-reopen-info
          (constantly nil)
          :fetch-ha-endpoint-snapshot-copy!
          (fn [_db-name _m _source-endpoint _snapshot-dir]
            {:copy-meta {:db-name "db"
                         :db-identity "db-id"
                         :snapshot-last-applied-lsn 16
                         :payload-last-applied-lsn 17
                         :txlog-last-applied-lsn 17}})
          :validate-ha-snapshot-copy!
          boot/validate-ha-snapshot-copy!
          :install-ha-local-snapshot!
          (fn [m _snapshot-dir]
            {:ok? true
             :state (assoc m :installed? true)})
          :raw-local-kv-store
          (constantly (fake-info-kv-store
                       {[c/kv-info c/wal-local-payload-lsn] 17}))
          :read-ha-local-snapshot-current-lsn
          (constantly 16)
          :reconcile-ha-installed-snapshot-state
          (fn [state materialized-lsn trusted-install-lsn _apply-record-fn]
            (reset! trusted-reconcile [materialized-lsn trusted-install-lsn])
            {:state state
             :installed-lsn trusted-install-lsn})
          :persist-ha-local-applied-lsn!
          (fn [_state installed-lsn]
            installed-lsn)
          :note-ha-bootstrap-installed-state
          (fn [state installed-lsn source snapshot-lsn now-ms persisted-lsn]
            (assoc state
                   :noted [installed-lsn source snapshot-lsn now-ms
                           persisted-lsn]))
          :apply-ha-follower-record!
          (fn [state _record] state)
          :sync-ha-follower-batch
          (fn [_db-name state _lease next-lsn _now-ms]
            (reset! resume-next-lsn next-lsn)
            {:state (assoc state :resume-next-lsn next-lsn)})}
         "db"
         {:ha-db-identity "db-id"}
         {:leader-endpoint source-endpoint
          :leader-last-applied-lsn 17}
         [source-endpoint]
         19
         1234)]
    (is (true? (:ok? result)))
    (is (= [17 17] @trusted-reconcile))
    (is (= 18 @resume-next-lsn))
    (is (= [17 source-endpoint 17 1234 17]
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
          (constantly (fake-info-kv-store
                       {[c/kv-info c/wal-local-payload-lsn] 8}))
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

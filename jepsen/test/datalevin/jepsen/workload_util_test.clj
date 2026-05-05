(ns datalevin.jepsen.workload-util-test
  (:require
   [clojure.test :refer [deftest is testing]]
   [datalevin.jepsen.workload.identity-upsert :as identity-upsert]
   [datalevin.jepsen.workload.index-consistency :as index-consistency]
   [datalevin.jepsen.workload.internal :as internal]
   [datalevin.jepsen.workload.util :as workload.util]
   [jepsen.checker :as checker]
   [jepsen.history :as history]))

(deftest assoc-exception-op-classifies-indeterminate-errors-test
  (testing "transport and timeout failures are reported as info"
    (let [op     {:type :invoke :f :write}
          error  (ex-info "Timeout in making request" {:phase :open-conn})
          result (workload.util/assoc-exception-op op error :timeout)]
      (is (= :info (:type result)))
      (is (= :timeout (:error result)))))

  (testing "definite application errors remain fail"
    (let [op     {:type :invoke :f :write}
          error  (ex-info "boom" {:error :unexpected})
          result (workload.util/assoc-exception-op op error :unexpected)]
      (is (= :fail (:type result)))
      (is (= :unexpected (:error result)))))

  (testing "explicit indeterminate HA confirmation errors are reported as info"
    (let [op     {:type :invoke :f :write}
          error  (ex-info "HA write commit confirmation failed"
                          {:error :ha/write-indeterminate
                           :indeterminate? true})
          result (workload.util/assoc-exception-op
                  op
                  error
                  :ha/write-indeterminate)]
      (is (= :info (:type result)))
      (is (= :ha/write-indeterminate (:error result))))))

(deftest exception-detail-sanitizes-history-unsafe-values-test
  (let [opaque (Object.)
        error  (ex-info "boom"
                        {:opaque opaque
                         :nested {:values [1 opaque]}})
        detail (workload.util/exception-detail error)]
    (is (= "boom" (:message detail)))
    (is (= (str opaque) (:opaque detail)))
    (is (= [1 (str opaque)] (get-in detail [:nested :values])))))

(deftest history-safe-bounds-unbounded-collections-test
  (let [result (workload.util/history-safe {:values (range)})
        values (:values result)]
    (is (= (vec (range 64)) (subvec values 0 64)))
    (is (true? (:datalevin.jepsen/truncated? (peek values))))
    (is (= :collection-limit (:datalevin.jepsen/reason (peek values))))))

(deftest history-safe-does-not-stringify-depth-limited-collections-test
  (let [result  (workload.util/history-safe {:outer [{:inner (range)}]} 1)
        summary (get-in result [:outer 0])]
    (is (true? (:datalevin.jepsen/truncated? summary)))
    (is (= :max-depth (:datalevin.jepsen/reason summary)))))

(deftest append-graph-ignores-terminal-micro-op-transactions-test
  (testing "read-only terminal transactions are uninformative for append graphs"
    (is (true? (workload.util/append-graph-ignorable-micro-op-txn?
                {:type :info
                 :f :txn
                 :error "Timeout in making request"
                 :value [[:r 3 nil]]}))))

  (testing "append-only terminal transactions are uninformative without reads"
    (is (true? (workload.util/append-graph-ignorable-micro-op-txn?
                {:type :fail
                 :f :txn
                 :error :cas-failed
                 :value [[:append 2 1]]}))))

  (testing "mixed transactions still carry graph information"
    (is (false? (workload.util/append-graph-ignorable-micro-op-txn?
                 {:type :info
                  :f :txn
                  :error "Timeout in making request"
                  :value [[:r 3 nil]
                          [:append 2 1]]})))))

(defn- exact-state-checker-result
  [checker-f expected-value-f op]
  (checker/check
    (checker-f)
    {}
    (history/history
      [(assoc op
              :process 0
              :type :ok
              :value (expected-value-f op))
       (assoc op
              :process 1
              :type :info
              :error "Timeout in making request")])
    nil))

(deftest internal-checker-ignores-indeterminate-ops-test
  (let [op     {:f :lookup-ref-same
                :internal/case-id 1}
        result (exact-state-checker-result
                 #'internal/internal-checker
                 (fn [op]
                   (:value (#'internal/expected-outcome op)))
                 op)]
    (is (true? (:valid? result)) (pr-str result))
    (is (= 0 (:mismatch-count result)))
    (is (= 0 (:failure-count result)))
    (is (= 1 (:indeterminate-count result)))))

(deftest identity-upsert-checker-ignores-indeterminate-ops-test
  (let [op     {:f :upsert-same-tempid
                :identity/case-id 1}
        result (exact-state-checker-result
                 #'identity-upsert/identity-upsert-checker
                 #'identity-upsert/expected-states
                 op)]
    (is (true? (:valid? result)) (pr-str result))
    (is (= 0 (:mismatch-count result)))
    (is (= 0 (:failure-count result)))
    (is (= 1 (:indeterminate-count result)))))

(deftest identity-upsert-checker-ignores-node-kill-admission-rejections-test
  (let [op     {:f :lookup-ref-intermediate
                :identity/case-id 2}
        result (checker/check
                 (#'identity-upsert/identity-upsert-checker)
                 {:datalevin/nemesis-faults [:node-kill]}
                 (history/history
                  [(assoc op
                          :process 0
                          :type :ok
                          :value (#'identity-upsert/expected-states op))
                   (assoc op
                          :process 1
                          :type :fail
                          :error "Request to Datalevin server failed: \"HA write admission rejected\"")])
                 nil)]
    (is (true? (:valid? result)) (pr-str result))
    (is (= 0 (:mismatch-count result)))
    (is (= 0 (:failure-count result)))
    (is (= 1 (:disruption-failure-count result)))))

(deftest identity-upsert-checker-allows-transient-read-back-mismatch-test
  (let [op      {:f :string-tempid-upsert-ref
                 :identity/case-id 8}
        result  (checker/check
                 (#'identity-upsert/identity-upsert-checker)
                 {}
                 (history/history
                  [(assoc op
                          :process 0
                          :type :ok
                          :value [])
                   {:process 0
                    :type :ok
                    :f :probe
                    :value {8 (#'identity-upsert/expected-final-state op)}}])
                 nil)]
    (is (true? (:valid? result)) (pr-str result))
    (is (= 0 (:mismatch-count result)))
    (is (= 1 (:transient-mismatch-count result)))
    (is (= 0 (:probe-mismatch-count result)))))

(deftest index-consistency-checker-ignores-indeterminate-ops-test
  (let [op     {:f :ref-create
                :index/case-id 1}
        result (exact-state-checker-result
                 #'index-consistency/index-consistency-checker
                 #'index-consistency/expected-states
                 op)]
    (is (true? (:valid? result)) (pr-str result))
    (is (= 0 (:mismatch-count result)))
    (is (= 0 (:failure-count result)))
    (is (= 1 (:indeterminate-count result)))))

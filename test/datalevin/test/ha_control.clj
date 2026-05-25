(ns datalevin.test.ha-control
  (:require
   [clojure.test :refer [deftest is]]
   [datalevin.ha.control :as ctrl]))

(def apply-state-command #'ctrl/apply-state-command)

(deftest membership-hash-update-cas-test
  (let [state {:leases {"app" {:lease {:db-identity "app"
                                        :leader-node-id 1
                                        :leader-endpoint "dtlv://n1/app"
                                        :term 1
                                        :lease-until-ms 1000}
                               :version 1}}
               :membership-hash nil
               :voters ["127.0.0.1:9001"]}
        init  (apply-state-command state
                                   {:op :init-membership-hash
                                    :membership-hash "old"})
        wrong (apply-state-command (:state init)
                                   {:op :update-membership-hash
                                    :req {:expected-membership-hash "stale"
                                          :membership-hash "new"}})
        updated (apply-state-command (:state init)
                                     {:op :update-membership-hash
                                      :req {:expected-membership-hash "old"
                                            :membership-hash "new"}})
        retried (apply-state-command (:state updated)
                                     {:op :update-membership-hash
                                      :req {:expected-membership-hash "old"
                                            :membership-hash "new"}})]
    (is (= {:ok? true
            :initialized? true
            :membership-hash "old"}
           (:result init)))
    (is (= {:ok? false
            :reason :membership-hash-mismatch
            :membership-hash "old"
            :expected "stale"}
           (:result wrong)))
    (is (= (:state init) (:state wrong)))
    (is (= "new" (get-in updated [:state :membership-hash])))
    (is (= {} (get-in updated [:state :leases])))
    (is (= {:ok? true
            :updated? true
            :membership-hash "new"
            :previous-membership-hash "old"
            :cleared-leases 1}
           (:result updated)))
    (is (= {:ok? true
            :updated? false
            :idempotent? true
            :membership-hash "new"
            :previous-membership-hash "new"
            :cleared-leases 0}
           (:result retried)))
    (is (= (:state updated) (:state retried)))))

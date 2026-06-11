(ns datalevin.test.ha-replication-store
  (:require
   [clojure.test :refer [deftest is]]
   [datalevin.constants :as c]
   [datalevin.core :as d]
   [datalevin.ha.replication.store :as ha-store]
   [datalevin.kv :as kv]
   [datalevin.util :as u])
  (:import
   [java.util UUID]))

(deftest promotion-local-lsn-uses-durable-payload-floor-test
  (let [dir (u/tmp-dir (str "ha-promotion-payload-floor-"
                            (UUID/randomUUID)))]
    (try
      (let [kv-store (d/open-kv dir {:wal? true})]
        (try
          (kv/transact-kv-without-txlog!
           kv-store
           [[:put c/kv-info c/wal-local-payload-lsn 3
             :keyword :data]])
          (is (= 3
                 (#'ha-store/fresh-ha-promotion-local-last-applied-lsn
                  {:ha-role :follower
                   :ha-local-last-applied-lsn 5
                   :store kv-store}
                  {:leader-last-applied-lsn 5})))
          (finally
            (d/close-kv kv-store))))
      (finally
        (u/delete-files dir)))))

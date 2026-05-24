(ns datalevin.test.replica
  (:require
   [clojure.test :refer [deftest is testing]]
   [datalevin.client :as cl]
   [datalevin.constants :as c]
   [datalevin.core :as d]
   [datalevin.server :as srv]
   [datalevin.test.core :as tdc]
   [datalevin.util :as u]
   [taoensso.timbre :as log])
  (:import
   [java.util UUID]))

(defn- server-uri
  [port db-name]
  (str "dtlv://" c/default-username ":" c/default-password
       "@127.0.0.1:" port "/" db-name))

(defn- start-test-server
  [port dir]
  (let [server (binding [c/*db-background-sampling?* false]
                 (srv/create {:port port
                              :root dir}))]
    (srv/start server)
    server))

(defn- eventually
  [f timeout-ms]
  (let [deadline (+ (System/currentTimeMillis) timeout-ms)]
    (loop []
      (if-let [res (f)]
        res
        (do
          (when (>= (System/currentTimeMillis) deadline)
            (is false "timed out waiting for condition")
            nil)
          (when (< (System/currentTimeMillis) deadline)
            (Thread/sleep 50)
            (recur)))))))

(deftest async-read-replica-datalog-test
  (let [db-name      (str "replica-test-" (UUID/randomUUID))
        primary-port (tdc/allocate-port)
        replica-port (tdc/allocate-port)
        primary-dir  (u/tmp-dir (str "replica-primary-" (UUID/randomUUID)))
        replica-dir  (u/tmp-dir (str "replica-copy-" (UUID/randomUUID)))
        schema       {:name {:db/valueType :db.type/string
                             :db/cardinality :db.cardinality/one}}
        query        '[:find [?name ...] :where [?e :name ?name]]
        primary-uri  (server-uri primary-port db-name)
        replica-uri  (server-uri replica-port db-name)]
    (log/set-min-level! :report)
    (binding [c/*db-background-sampling?* false]
      (let [primary-server (start-test-server primary-port primary-dir)
            replica-server (start-test-server replica-port replica-dir)]
        (try
          (let [primary-conn (d/create-conn
                              primary-uri
                              schema
                              {:wal? true
                               :wal-durability-profile :strict
                               :client-opts {:pool-size 1
                                             :time-out 20000}})
                _            (d/transact! primary-conn
                                          [{:db/id -1 :name "first"}])
                replica-conn (d/create-conn
                              replica-uri
                              schema
                              {:replica/read-only? true
                               :replica/source primary-uri
                               :replica/id "test-replica"
                               :replica/poll-ms 50
                               :replica/report-ms 100
                               :wal? true
                               :wal-durability-profile :strict
                               :client-opts {:pool-size 1
                                             :time-out 20000}})]
            (try
              (testing "bootstrap copy makes existing data readable"
                (eventually
                 #(= #{"first"} (set (d/q query @replica-conn)))
                 5000))
              (testing "WAL tailing catches up later primary writes"
                (d/transact! primary-conn [{:db/id -2 :name "second"}])
                (eventually
                 #(= #{"first" "second"} (set (d/q query @replica-conn)))
                 5000))
              (testing "replica status is exposed"
                (let [client (cl/new-client replica-uri {:pool-size 1
                                                         :time-out 20000})]
                  (try
                    (let [status (eventually
                                  #(let [status (cl/replica-status client db-name)]
                                     (when (and (:replica/read-only? status)
                                                (zero? (long (:replica-lag-lsn
                                                              status))))
                                       status))
                                  5000)]
                      (is (= primary-uri (:replica/source status)))
                      (is (= "test-replica" (:replica/id status)))
                      (is (integer? (:replica-applied-lsn status)))
                      (is (integer? (:replica-source-durable-lsn status))))
                    (finally
                      (cl/disconnect client)))))
              (testing "user writes are rejected at server dispatch"
                (is (thrown? Exception
                             (d/transact! replica-conn
                                          [{:db/id -3 :name "reject"}]))))
              (finally
                (d/close primary-conn)
                (d/close replica-conn))))
          (finally
            (srv/stop primary-server)
            (srv/stop replica-server)
            (u/delete-files primary-dir)
            (u/delete-files replica-dir)))))))

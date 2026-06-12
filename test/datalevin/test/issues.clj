(ns datalevin.test.issues
  (:require
   [datalevin.binding.cpp :as cpp]
   [datalevin.core :as d]
   [datalevin.kv :as kv]
   [datalevin.query-optimizer :as qo]
   [datalevin.test.core :as tdc :refer [db-fixture]]
   [clojure.test :refer [deftest testing is use-fixtures]]
   [datalevin.util :as u])
  (:import
   [datalevin.storage Store]
   [java.util UUID]))

(use-fixtures :each db-fixture)

(deftest issue-262
  (let [dir (u/tmp-dir (str "query-or-" (UUID/randomUUID)))
        db  (d/db-with (d/empty-db dir)
                       [{:attr "A"} {:attr "B"}])]
    (is (= (d/q '[:find ?a ?b
                  :where [_ :attr ?a]
                  [(vector ?a) ?b]]
                db)
           #{["A" ["A"]] ["B" ["B"]]}))
    (d/close-db db)
    (u/delete-files dir)))

(deftest issue-371-zero-count-must-not-empty-result
  ;; Counted-index metadata can drift and report 0 datoms for an attribute
  ;; that still holds data. The optimizer must verify a zero count against
  ;; the actual index instead of short-circuiting to an empty result.
  (let [dir    (u/tmp-dir (str "issue-371-" (UUID/randomUUID)))
        schema {:job/completed-at {:db/valueType :db.type/instant}
                :job/tag          {:db/valueType :db.type/string}}
        conn   (d/get-conn dir schema)]
    (try
      (d/transact! conn (mapv (fn [^long n]
                                {:db/id            (- (inc n))
                                 :job/completed-at (java.util.Date.)
                                 :job/tag          "a"})
                              (range 100)))
      (with-redefs [qo/fast-clause-count (fn ^long [_db _e _clause ^long _mc] 0)]
        (testing "zero count for a populated free-value clause"
          (is (= 100 (ffirst (d/q '[:find (count ?j)
                                    :where [?j :job/completed-at _]]
                                  @conn)))))
        (testing "zero count for a populated attribute-value clause"
          (is (= 100 (ffirst (d/q '[:find (count ?j)
                                    :where [?j :job/tag "a"]]
                                  @conn)))))
        (testing "zero count for a populated known-entity clause"
          (let [eid (ffirst (d/q '[:find ?j
                                   :where [?j :job/tag "a"]]
                                 @conn))]
            (is (= 1 (count (d/q [:find '?v
                                  :where [eid :job/completed-at '?v]]
                                 @conn))))))
        (testing "a clause on an unknown attribute still yields empty result"
          (is (empty? (d/q '[:find ?j :where [?j :job/nonexistent _]]
                           @conn)))))
      (testing "healthy counts still produce the same results"
        (is (= 100 (ffirst (d/q '[:find (count ?j)
                                  :where [?j :job/completed-at _]]
                                @conn)))))
      (testing "a genuinely empty clause still returns an empty result"
        (is (empty? (d/q '[:find ?j :where [?j :job/tag "nope"]]
                         @conn))))
      (finally
        (d/close conn)
        (u/delete-files dir)))))

(deftest issue-371-fill-db-refreshes-count-cache
  (let [dir    (u/tmp-dir (str "issue-371-fill-db-" (UUID/randomUUID)))
        schema {:job/completed-at {:db/valueType :db.type/instant}}
        db*    (volatile! nil)]
    (try
      (let [db0 (d/empty-db dir schema {:background-sampling? false})]
        (vreset! db* db0)
        (is (zero? (d/count-datoms db0 nil :job/completed-at nil)))
        (let [datoms (mapv (fn [^long e]
                             (d/datom e :job/completed-at
                                      (java.util.Date. (+ 1700000000000 e))))
                           (range 1 11))
              db1    (d/fill-db db0 datoms)]
          (vreset! db* db1)
          (is (= 10 (d/count-datoms db1 nil :job/completed-at nil)))
          (is (= 10 (count (d/datoms db1 :ave :job/completed-at))))
          (is (= 10 (d/max-eid db1)))))
      (finally
        (when-let [db @db*]
          (d/close-db db))
        (u/delete-files dir)))))

(deftest issue-366-stale-cursor-after-reader-close
  (let [dir    (u/tmp-dir (str "stale-reader-cursor-" (UUID/randomUUID)))
        schema {:name {:db/valueType   :db.type/string
                       :db/cardinality :db.cardinality/one}}
        query  '[:find [?name ...] :where [?e :name ?name]]
        conn   (d/get-conn dir schema)]
    (try
      (d/transact! conn [{:db/id -1 :name "first"}])
      (is (= #{"first"} (set (d/q query @conn))))
      (cpp/invalidate-thread-reader!
        (kv/raw-lmdb (.-lmdb ^Store (:store @conn))))
      (is (= #{"first"} (set (d/q query @conn))))
      (finally
        (d/close conn)
        (u/delete-files dir)))))

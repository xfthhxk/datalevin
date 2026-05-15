(ns datalevin.test.server-runtime-lock
  (:require
   [clojure.test :refer [deftest is]]
   [datalevin.interface :as i]
   [datalevin.server :as srv]
   [datalevin.server.dispatch :as dispatch]
   [datalevin.server.handlers :as handlers])
  (:import
   [java.util.concurrent ConcurrentHashMap ConcurrentLinkedQueue CountDownLatch
    FutureTask Semaphore TimeUnit]
   [java.util.concurrent.atomic AtomicBoolean]))

(defn- test-server
  []
  (let [dbs (ConcurrentHashMap.)]
    (.put dbs "db" {})
    (srv/->Server (AtomicBoolean. true)
                  0
                  ""
                  0
                  nil
                  nil
                  (ConcurrentLinkedQueue.)
                  nil
                  nil
                  nil
                  (ConcurrentHashMap.)
                  dbs)))

(deftest wire-safe-diagnostic-stringifies-opaque-values-test
  (let [opaque (Object.)
        result (#'handlers/wire-safe-diagnostic
                {:ok true
                 :opaque opaque
                 :nested [{:x opaque}]})]
    (is (= true (:ok result)))
    (is (string? (:opaque result)))
    (is (string? (get-in result [:nested 0 :x])))))

(deftest runtime-read-access-uses-message-db-name-test
  (let [server         (test-server)
        writer-entered (CountDownLatch. 1)
        release-writer (CountDownLatch. 1)
        reader-entered (CountDownLatch. 1)
        writer         (future
                         (#'srv/with-db-runtime-store-swap
                          server
                          "db"
                          (fn []
                            (.countDown writer-entered)
                            (.await release-writer 5 TimeUnit/SECONDS))))
        _              (is (.await writer-entered 1 TimeUnit/SECONDS))
        reader         (future
                         (#'srv/with-db-runtime-read-access
                          server
                          {:type :open-kv
                           :db-name "DB"}
                          (fn []
                            (.countDown reader-entered)
                            :read)))]
    (try
      (is (false? (.await reader-entered 100 TimeUnit/MILLISECONDS)))
      (.countDown release-writer)
      (is (= :read (deref reader 1000 ::timeout)))
      (is (true? (deref writer 1000 ::timeout)))
      (is (.await reader-entered 1 TimeUnit/SECONDS))
      (finally
        (.countDown release-writer)
        (future-cancel writer)
        (future-cancel reader)))))

(deftest remove-store-waits-for-runtime-read-access-test
  (let [^datalevin.server.Server
        server           (test-server)
        ^ConcurrentHashMap
        dbs              (.-dbs server)
        reader-entered   (CountDownLatch. 1)
        release-reader   (CountDownLatch. 1)
        close-called     (CountDownLatch. 1)
        remover-finished (CountDownLatch. 1)]
    (.put dbs
          "db"
          {:store (reify i/IStore
                    (close [_]
                      (.countDown close-called)))})
    (let [reader  (future
                    (srv/with-db-runtime-store-read-access
                     server
                     "db"
                     (fn []
                       (.countDown reader-entered)
                       (.await release-reader 5 TimeUnit/SECONDS)
                       :read)))
          _       (is (.await reader-entered 1 TimeUnit/SECONDS))
          remover (future
                    (#'srv/remove-store server "db")
                    (.countDown remover-finished)
                    :removed)]
      (try
        (is (false? (.await close-called 100 TimeUnit/MILLISECONDS)))
        (is (false? (.await remover-finished 100 TimeUnit/MILLISECONDS)))
        (.countDown release-reader)
        (is (= :read (deref reader 1000 ::timeout)))
        (is (= :removed (deref remover 1000 ::timeout)))
        (is (.await close-called 1 TimeUnit/SECONDS))
        (is (.await remover-finished 1 TimeUnit/SECONDS))
        (is (nil? (get dbs "db")))
        (finally
          (.countDown release-reader)
          (future-cancel reader)
          (future-cancel remover))))))

(deftest stop-ha-background-loops-cancels-loop-futures-test
  (let [server             (test-server)
        ^ConcurrentHashMap
        dbs                (.-dbs ^datalevin.server.Server server)
        renew-running?    (AtomicBoolean. true)
        follower-running? (AtomicBoolean. true)
        renew-future      (FutureTask. ^Runnable (fn []) nil)
        follower-future   (FutureTask. ^Runnable (fn []) nil)]
    (.put dbs
          "db"
          {:ha-renew-loop-running? renew-running?
           :ha-renew-loop-future renew-future
           :ha-follower-loop-running? follower-running?
           :ha-follower-loop-future follower-future})
    (#'srv/stop-ha-background-loops! server)
    (is (false? (.get renew-running?)))
    (is (false? (.get follower-running?)))
    (is (.isCancelled renew-future))
    (is (.isCancelled follower-future))))

(deftest copy-message-uses-handler-scoped-runtime-read-lock-test
  (is (false? (#'dispatch/runtime-read-access-message?
               {:type :copy
                :args ["db"]})))
  (is (#'dispatch/runtime-read-access-message?
       {:type :db-info
        :args ["db"]}))
  (is (false? (#'dispatch/runtime-read-access-message?
               {:type :db-info
                :args ["db"]
                :writing? true}))))

(deftest copy-transaction-slot-rejects-active-write-transaction-test
  (let [^Semaphore lock (Semaphore. 1)
        deps            {:get-lock (fn [_ _] lock)}]
    (.acquire lock)
    (try
      (is (thrown-with-msg?
           clojure.lang.ExceptionInfo
           #"write transaction is active"
           (#'handlers/with-copy-db-transaction-slot
            deps nil "db" (fn [] :copied))))
      (is (zero? (.availablePermits lock)))
      (finally
        (.release lock)))))

(deftest copy-transaction-slot-releases-after-success-test
  (let [^Semaphore lock (Semaphore. 1)
        deps            {:get-lock (fn [_ _] lock)}]
    (is (= :copied
           (#'handlers/with-copy-db-transaction-slot
            deps nil "db" (fn [] :copied))))
    (is (= 1 (.availablePermits lock)))))

(deftest copy-closes-copied-store-before-streaming-test
  (let [^Semaphore lock (Semaphore. 1)
        closed?         (atom false)
        events          (atom [])
        copied-store    (reify i/IStore
                          (opts [_] {})
                          (close [_] (reset! closed? true))
                          (closed? [_] @closed?))
        deps            {:handle-message-error!
                         (fn [_ e] (throw e))
                         :get-lock
                         (fn [_ _] lock)
                         :lmdb
                         (fn [_ _ _ _] ::source-store)
                         :with-db-runtime-store-read-access
                         (fn [_ _ f] (f))
                         :server-copy-store!
                         (fn [_ _ _]
                           (swap! events conj :copy-source))
                         :open-server-copied-store!
                         (fn [_ _ _]
                           (swap! events conj :open-copy)
                           copied-store)
                         :copy-response-meta
                         (fn [_ store base-meta]
                           (is (identical? copied-store store))
                           (swap! events conj :meta)
                           (assoc base-meta :db-name "db"))
                         :sync-copy-response-store!
                         (fn [store]
                           (is (identical? copied-store store))
                           (swap! events conj :sync)
                           store)
                         :close-server-copied-store!
                         (fn [store]
                           (is (identical? copied-store store))
                           (swap! events conj :close)
                           (i/close store))
                         :copy-server-file-out!
                         (fn [_ _ copy-meta]
                           (is (true? @closed?))
                           (is (= "db" (:db-name copy-meta)))
                           (swap! events conj :stream))
                         :cleanup-copy-tmp-dir!
                         (fn [_] nil)
                         :unpin-server-copy-backup-floor!
                         (fn [& _] nil)}]
    (#'handlers/copy deps nil nil {:args ["db" false]})
    (is (= [:copy-source :open-copy :meta :sync :close :stream]
           @events))
    (is (= 1 (.availablePermits lock)))))

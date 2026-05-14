(ns datalevin.test.secondary-index
  (:require
   [datalevin.built-ins :as bi]
   [clojure.test :refer [deftest is]]
   [datalevin.constants :as c]
   [datalevin.core :as d]
   [datalevin.embedding :as emb]
   [datalevin.interface :as i]
   [datalevin.secondary-index :as si]
   [datalevin.util :as u]
   [datalevin.validate :as vld])
  (:import
   [datalevin.db DB]
   [datalevin.lmdb KVTxData]
   [datalevin.storage Store]
   [java.util.concurrent CountDownLatch TimeUnit]
   [java.util UUID]))

(def ^:private test-embedding-metadata
  {:embedding/provider {:kind :test
                        :id :async-secondary-index}
   :embedding/output {:dimensions 2}})

(defn- throwing-embedding-provider
  []
  (reify
    emb/IEmbeddingProvider
    (embedding [_ _items _opts]
      (throw (ex-info "provider down" {})))
    (embedding-metadata [_]
      test-embedding-metadata)
    (embedding-dimensions [_]
      2)
    (close-provider [_]
      nil)

    java.lang.AutoCloseable
    (close [this]
      (emb/close-provider this))))

(defn- constant-vector-provider
  []
  (reify
    emb/IEmbeddingProvider
    (embedding [_ items _opts]
      (mapv (fn [_] (float-array [1.0 0.0])) items))
    (embedding-metadata [_]
      test-embedding-metadata)
    (embedding-dimensions [_]
      2)
    (close-provider [_]
      nil)

    java.lang.AutoCloseable
    (close [this]
      (emb/close-provider this))))

(defn- switchable-embedding-provider
  [state]
  (reify
    emb/IEmbeddingProvider
    (embedding [_ items _opts]
      (case @state
        :down (throw (ex-info "provider down" {}))
        :up (mapv (fn [_] (float-array [1.0 0.0])) items)))
    (embedding-metadata [_]
      test-embedding-metadata)
    (embedding-dimensions [_]
      2)
    (close-provider [_]
      nil)

    java.lang.AutoCloseable
    (close [this]
      (emb/close-provider this))))

(defn- blocking-embedding-provider
  [^CountDownLatch started ^CountDownLatch release]
  (reify
    emb/IEmbeddingProvider
    (embedding [_ items _opts]
      (.countDown started)
      (when-not (.await release 5 TimeUnit/SECONDS)
        (throw (ex-info "timed out waiting for release" {})))
      (mapv (fn [_] (float-array [1.0 0.0])) items))
    (embedding-metadata [_]
      test-embedding-metadata)
    (embedding-dimensions [_]
      2)
    (close-provider [_]
      nil)

    java.lang.AutoCloseable
    (close [this]
      (emb/close-provider this))))

(defn- secondary-index-job-values
  [conn]
  (let [^Store store (.-store ^DB @conn)]
    (mapv second
          (i/get-range (.-lmdb store)
                       c/secondary-index-jobs
                       [:all]
                       :data
                       :data))))

(defn- wait-for-status
  [conn pred timeout-ms]
  (let [deadline (+ (System/currentTimeMillis) (long timeout-ms))]
    (loop []
      (let [status (d/secondary-index-status conn)]
        (cond
          (pred status) status
          (>= (System/currentTimeMillis) (long deadline)) status
          :else (do
                  (Thread/sleep 20)
                  (recur)))))))

(defn- neighbor-entity-attrs
  [results]
  (mapv (fn [res]
          (let [res (vec res)]
            [(nth res 0) (nth res 1)]))
        results))

(deftest indexing-mode-validation-test
  (is (= :sync (si/normalize-indexing-mode nil)))
  (is (= :sync (si/normalize-indexing-mode :sync)))
  (is (= :async (si/normalize-indexing-mode :async)))
  (is (thrown-with-msg?
       clojure.lang.ExceptionInfo
       #"Unsupported secondary indexing mode"
       (si/normalize-indexing-mode :later)))
  (is (= {:embedding-opts {:indexing-mode :async}}
         (vld/validate-embedding-options
          {:embedding-opts {:indexing-mode :async}})))
  (is (thrown-with-msg?
       clojure.lang.ExceptionInfo
       #"Embedding indexing mode is not supported"
       (vld/validate-embedding-options
        {:embedding-domains {"docs" {:indexing-mode :later}}})))
  (is (= {:search-opts {:indexing-mode :async}}
         (vld/validate-search-options
          {:search-opts {:indexing-mode :async}})))
  (is (thrown-with-msg?
       clojure.lang.ExceptionInfo
       #"Search indexing mode is not supported"
       (vld/validate-search-options
        {:search-domains {"docs" {:indexing-mode :later}}})))
  (is (= {:vector-opts {:indexing-mode :async}}
         (vld/validate-vector-options
          {:vector-opts {:indexing-mode :async}})))
  (is (thrown-with-msg?
       clojure.lang.ExceptionInfo
       #"Vector indexing mode is not supported"
       (vld/validate-vector-options
        {:vector-domains {"vecs" {:indexing-mode :later}}})))
  (is (= {:async-secondary-index-worker-max-jobs 1
          :async-secondary-index-worker-lease-ms 100
          :async-secondary-index-retry-base-ms 10
          :async-secondary-index-retry-max-ms 20}
         (vld/validate-secondary-index-worker-options
          {:async-secondary-index-worker-max-jobs 1
           :async-secondary-index-worker-lease-ms 100
           :async-secondary-index-retry-base-ms 10
           :async-secondary-index-retry-max-ms 20})))
  (is (thrown-with-msg?
       clojure.lang.ExceptionInfo
       #"expects a positive integer"
       (vld/validate-secondary-index-worker-options
        {:async-secondary-index-worker-max-jobs 0})))
  (is (thrown-with-msg?
       clojure.lang.ExceptionInfo
       #"must be <="
       (vld/validate-secondary-index-worker-options
        {:async-secondary-index-retry-base-ms 20
         :async-secondary-index-retry-max-ms 10}))))

(deftest secondary-index-job-test
  (let [job (si/make-job {:type :embedding
                          :domain "docs"
                          :op :add
                          :ref [1 2 "hello"]
                          :value "hello"
                          :tx 42
                          :ordinal 7
                          :created-ms 100})]
    (is (= [:embedding "docs" 42 7] (:job/id job)))
    (is (= :pending (:job/status job)))
    (is (= 0 (:job/attempts job)))
    (is (= 100 (:job/created-ms job)))
    (is (= 100 (:job/updated-ms job)))
    (let [tx (si/job-tx job)]
      (is (instance? KVTxData tx))
      (is (= c/secondary-index-jobs (.-dbi-name ^KVTxData tx))))))

(deftest async-embedding-enqueues-job-without-provider-call-test
  (let [dir (u/tmp-dir (str "test-async-secondary-index-" (UUID/randomUUID)))
        provider (throwing-embedding-provider)
        schema {:text {:db/valueType :db.type/string
                       :db/embedding true}}
        opts {:embedding-providers {:default provider}
              :embedding-opts {:dimensions 2
                               :embedding-metadata test-embedding-metadata
                               :indexing-mode :async}}
        conn (d/create-conn dir schema opts)]
    (try
      (is (map? (d/transact! conn [{:text "hello"}])))
      (let [jobs (secondary-index-job-values conn)]
        (is (= 1 (count jobs)))
        (is (= :embedding (get-in jobs [0 :job/type])))
        (is (= :add (get-in jobs [0 :job/op])))
        (is (= "hello" (get-in jobs [0 :job/value])))
        (is (#{:pending :failed} (get-in jobs [0 :job/status]))))
      (finally
        (d/close conn)
        (u/delete-files dir)))))

(deftest process-async-embedding-job-test
  (let [dir (u/tmp-dir (str "test-process-async-secondary-index-"
                            (UUID/randomUUID)))
        provider (constant-vector-provider)
        schema {:text {:db/valueType :db.type/string
                       :db/embedding true}}
        opts {:embedding-providers {:default provider}
              :embedding-opts {:dimensions 2
                               :embedding-metadata test-embedding-metadata
                               :indexing-mode :async}}
        conn (d/create-conn dir schema opts)]
    (try
      (d/transact! conn [{:text "hello"}])
      (is (true?
           (:caught-up?
            (d/wait-for-secondary-index conn
                                        {:timeout-ms 3000
                                         :poll-ms 20}))))
      (is (= [[1 :text "hello"]]
             (mapv vec (bi/embedding-neighbors @conn
                                                "hello"
                                                {:domains ["datalevin"]
                                                 :top 1}))))
      (is (= 1 (:completed-count (d/secondary-index-status conn))))
      (finally
        (d/close conn)
        (u/delete-files dir)))))

(deftest async-worker-claims-running-job-test
  (let [dir (u/tmp-dir (str "test-async-secondary-index-claim-"
                            (UUID/randomUUID)))
        started (CountDownLatch. 1)
        release (CountDownLatch. 1)
        provider (blocking-embedding-provider started release)
        schema {:text {:db/valueType :db.type/string
                       :db/embedding true}}
        opts {:embedding-providers {:default provider}
              :embedding-opts {:dimensions 2
                               :embedding-metadata test-embedding-metadata
                               :indexing-mode :async}
              :async-secondary-index-worker-lease-ms 5000}
        conn (d/create-conn dir schema opts)]
    (try
      (d/transact! conn [{:text "hello"}])
      (is (.await started 3 TimeUnit/SECONDS))
      (let [status (wait-for-status conn
                                    #(pos? (long (:running-count %)))
                                    3000)
            jobs (secondary-index-job-values conn)
            job (first jobs)]
        (is (= 1 (:running-count status)))
        (is (integer? (:next-lease-ms status)))
        (is (= :running (:job/status job)))
        (is (string? (:job/lease-owner job)))
        (is (integer? (:job/lease-until-ms job))))
      (.countDown release)
      (is (true?
           (:caught-up?
            (d/wait-for-secondary-index conn
                                        {:timeout-ms 3000
                                         :poll-ms 20}))))
      (is (= 1 (:completed-count (d/secondary-index-status conn))))
      (finally
        (.countDown release)
        (d/close conn)
        (u/delete-files dir)))))

(deftest expired-secondary-index-lease-is-reclaimed-test
  (let [dir (u/tmp-dir (str "test-async-secondary-index-expired-lease-"
                            (UUID/randomUUID)))
        v1 (float-array [1.0 0.0])
        schema {:embedding {:db/valueType :db.type/vec}}
        opts {:vector-opts {:dimensions 2
                            :indexing-mode :async}
              :async-secondary-index-worker-lease-ms 10}
        conn (d/create-conn dir schema opts)]
    (try
      (let [^Store store (.-store ^DB @conn)
            aid (get-in (i/schema store) [:embedding :db/aid])
            now-ms (System/currentTimeMillis)
            job (-> (si/make-job {:type :vector
                                   :domain "embedding"
                                   :op :add
                                   :ref [1 aid v1]
                                   :value v1
                                   :tx 1
                                   :ordinal 0
                                   :created-ms now-ms
                                   :updated-ms now-ms})
                    (si/claimed-job "dead-worker" (- now-ms 1000) now-ms))]
        (i/transact-kv (.-lmdb store) [(si/job-tx job)]))
      (let [result (d/process-secondary-index-jobs! conn {:owner "live-worker"})]
        (is (= 1 (:claimed-count result)))
        (is (= 1 (:completed-count result))))
      (let [job (first (secondary-index-job-values conn))]
        (is (= :completed (:job/status job)))
        (is (nil? (:job/lease-owner job)))
        (is (nil? (:job/lease-until-ms job))))
      (is (= [[1 :embedding]]
             (neighbor-entity-attrs
              (bi/vec-neighbors @conn :embedding v1 {:top 1}))))
      (finally
        (d/close conn)
        (u/delete-files dir)))))

(deftest sync-fulltext-index-default-test
  (let [dir (u/tmp-dir (str "test-sync-fulltext-index-"
                            (UUID/randomUUID)))
        schema {:text {:db/valueType :db.type/string
                       :db/fulltext true
                       :db.fulltext/domains ["docs"]}}
        opts {:search-domains {"docs" {}}}
        conn (d/create-conn dir schema opts)]
    (try
      (d/transact! conn [{:text "red fox"}])
      (is (empty? (secondary-index-job-values conn)))
      (is (= [[1 :text]]
             (neighbor-entity-attrs
              (bi/fulltext @conn "red" {:domains ["docs"] :top 1}))))
      (finally
        (d/close conn)
        (u/delete-files dir)))))

(deftest async-fulltext-index-worker-test
  (let [dir (u/tmp-dir (str "test-async-fulltext-index-"
                            (UUID/randomUUID)))
        schema {:text {:db/valueType :db.type/string
                       :db/fulltext true
                       :db.fulltext/domains ["docs"]}}
        opts {:search-domains {"docs" {:indexing-mode :async}}
              :async-secondary-index-worker-max-jobs 1}
        conn (d/create-conn dir schema opts)]
    (try
      (d/transact! conn [{:text "red fox"}])
      (let [jobs (secondary-index-job-values conn)]
        (is (= 1 (count jobs)))
        (is (= :fulltext (get-in jobs [0 :job/type])))
        (is (= :add (get-in jobs [0 :job/op])))
        (is (= "docs" (get-in jobs [0 :job/domain])))
        (is (= "red fox" (get-in jobs [0 :job/value])))
        (is (#{:pending :completed} (get-in jobs [0 :job/status]))))
      (is (true?
           (:caught-up?
            (d/wait-for-secondary-index conn
                                        {:timeout-ms 3000
                                         :poll-ms 20}))))
      (is (= [[1 :text]]
             (neighbor-entity-attrs
              (bi/fulltext @conn "red" {:domains ["docs"] :top 1}))))
      (d/transact! conn [{:db/id 1 :text "blue whale"}])
      (is (true?
           (:caught-up?
            (d/wait-for-secondary-index conn
                                        {:timeout-ms 3000
                                         :poll-ms 20}))))
      (is (= [[1 :text]]
             (neighbor-entity-attrs
              (bi/fulltext @conn "blue" {:domains ["docs"] :top 1}))))
      (is (empty?
           (neighbor-entity-attrs
            (bi/fulltext @conn "red" {:domains ["docs"] :top 1}))))
      (is (= 3 (:completed-count (d/secondary-index-status conn))))
      (finally
        (d/close conn)
        (u/delete-files dir)))))

(deftest sync-vector-index-default-test
  (let [dir (u/tmp-dir (str "test-sync-vector-index-"
                            (UUID/randomUUID)))
        v1 (float-array [1.0 0.0])
        schema {:embedding {:db/valueType :db.type/vec}}
        opts {:vector-opts {:dimensions 2}}
        conn (d/create-conn dir schema opts)]
    (try
      (d/transact! conn [{:embedding v1}])
      (is (empty? (secondary-index-job-values conn)))
      (is (= [[1 :embedding]]
             (neighbor-entity-attrs
              (bi/vec-neighbors @conn :embedding v1 {:top 1}))))
      (finally
        (d/close conn)
        (u/delete-files dir)))))

(deftest async-vector-index-worker-test
  (let [dir (u/tmp-dir (str "test-async-vector-index-"
                            (UUID/randomUUID)))
        v1 (float-array [1.0 0.0])
        v2 (float-array [0.0 1.0])
        schema {:embedding {:db/valueType :db.type/vec}}
        opts {:vector-opts {:dimensions 2
                            :indexing-mode :async}
              :async-secondary-index-worker-max-jobs 1}
        conn (d/create-conn dir schema opts)]
    (try
      (d/transact! conn [{:embedding v1}])
      (let [jobs (secondary-index-job-values conn)]
        (is (= 1 (count jobs)))
        (is (= :vector (get-in jobs [0 :job/type])))
        (is (= :add (get-in jobs [0 :job/op])))
        (is (= "embedding" (get-in jobs [0 :job/domain])))
        (is (#{:pending :completed} (get-in jobs [0 :job/status]))))
      (is (true?
           (:caught-up?
            (d/wait-for-secondary-index conn
                                        {:timeout-ms 3000
                                         :poll-ms 20}))))
      (is (= [[1 :embedding]]
             (neighbor-entity-attrs
              (bi/vec-neighbors @conn :embedding v1 {:top 1}))))
      (d/transact! conn [{:db/id 1 :embedding v2}])
      (is (true?
           (:caught-up?
            (d/wait-for-secondary-index conn
                                        {:timeout-ms 3000
                                         :poll-ms 20}))))
      (is (= [[1 :embedding]]
             (neighbor-entity-attrs
              (bi/vec-neighbors @conn :embedding v2 {:top 1}))))
      (is (= 3 (:completed-count (d/secondary-index-status conn))))
      (finally
        (d/close conn)
        (u/delete-files dir)))))

(deftest failed-async-embedding-job-survives-reopen-test
  (let [dir (u/tmp-dir (str "test-reopen-async-secondary-index-"
                            (UUID/randomUUID)))
        provider-state (atom :down)
        provider (switchable-embedding-provider provider-state)
        schema {:text {:db/valueType :db.type/string
                       :db/embedding true}}
        opts {:embedding-providers {:default provider}
              :embedding-opts {:dimensions 2
                               :embedding-metadata test-embedding-metadata
                               :indexing-mode :async}
              :async-secondary-index-retry-base-ms 10
              :async-secondary-index-retry-max-ms 20}]
    (try
      (let [conn (d/create-conn dir schema opts)]
        (d/transact! conn [{:text "hello"}])
        (is (= 1 (:failed-count
                  (wait-for-status conn
                                   #(pos? (long (:failed-count %)))
                                   3000))))
        (d/close conn))
      (reset! provider-state :up)
      (let [conn (d/create-conn dir schema opts)]
        (try
          (is (true?
               (:caught-up?
                (d/wait-for-secondary-index conn
                                            {:timeout-ms 3000
                                             :poll-ms 20}))))
          (is (= [[1 :text "hello"]]
                 (mapv vec (bi/embedding-neighbors @conn
                                                    "hello"
                                                    {:domains ["datalevin"]
                                                     :top 1}))))
          (finally
            (d/close conn))))
      (finally
        (u/delete-files dir)))))

(deftest background-async-secondary-index-worker-test
  (let [dir (u/tmp-dir (str "test-background-async-secondary-index-"
                            (UUID/randomUUID)))
        provider (constant-vector-provider)
        schema {:text {:db/valueType :db.type/string
                       :db/embedding true}}
        opts {:embedding-providers {:default provider}
              :embedding-opts {:dimensions 2
                               :embedding-metadata test-embedding-metadata
                               :indexing-mode :async}
              :async-secondary-index-worker-max-jobs 1}
        conn (d/create-conn dir schema opts)]
    (try
      (d/transact! conn [{:text "hello"}])
      (is (true?
           (:caught-up?
            (d/wait-for-secondary-index conn
                                        {:timeout-ms 3000
                                         :poll-ms 20}))))
      (is (= [[1 :text "hello"]]
             (mapv vec (bi/embedding-neighbors @conn
                                                "hello"
                                                {:domains ["datalevin"]
                                                 :top 1}))))
      (finally
        (d/close conn)
        (u/delete-files dir)))))

(deftest failed-async-embedding-job-can-be-retried-test
  (let [dir (u/tmp-dir (str "test-retry-async-secondary-index-"
                            (UUID/randomUUID)))
        provider-state (atom :down)
        provider (switchable-embedding-provider provider-state)
        schema {:text {:db/valueType :db.type/string
                       :db/embedding true}}
        opts {:embedding-providers {:default provider}
              :embedding-opts {:dimensions 2
                               :embedding-metadata test-embedding-metadata
                               :indexing-mode :async}
              :async-secondary-index-retry-base-ms 10
              :async-secondary-index-retry-max-ms 20}
        conn (d/create-conn dir schema opts)]
    (try
      (d/transact! conn [{:text "hello"}])
      (let [status (wait-for-status conn
                                    #(pos? (long (:failed-count %)))
                                    3000)]
        (is (= 1 (:failed-count status)))
        (is (= "provider down" (:last-error status)))
        (is (integer? (:next-retry-ms status))))
      (is (false?
           (:caught-up?
            (d/wait-for-secondary-index conn {:timeout-ms 0}))))
      (reset! provider-state :up)
      (is (true?
           (:caught-up?
            (d/wait-for-secondary-index conn
                                        {:timeout-ms 3000
                                         :poll-ms 20}))))
      (is (= [[1 :text "hello"]]
             (mapv vec (bi/embedding-neighbors @conn
                                                "hello"
                                                {:domains ["datalevin"]
                                                 :top 1}))))
      (finally
        (d/close conn)
        (u/delete-files dir)))))

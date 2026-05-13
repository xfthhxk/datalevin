(ns datalevin.test.secondary-index
  (:require
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
        {:embedding-domains {"docs" {:indexing-mode :later}}}))))

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
      (let [^Store store (.-store ^DB @conn)
            jobs (i/get-range (.-lmdb store)
                              c/secondary-index-jobs
                              [:all]
                              :data
                              :data)]
        (is (= 1 (count jobs)))
        (is (= :embedding (get-in jobs [0 1 :job/type])))
        (is (= :add (get-in jobs [0 1 :job/op])))
        (is (= "hello" (get-in jobs [0 1 :job/value])))
        (is (= :pending (get-in jobs [0 1 :job/status]))))
      (finally
        (d/close conn)
        (u/delete-files dir)))))

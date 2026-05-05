(ns datalevin.test.embedding
  (:require
   [clojure.test :refer [deftest is]]
   [datalevin.built-ins :as bi]
   [datalevin.core :as d]
   [datalevin.embedding :as emb]
   [datalevin.interface :as i]
   [datalevin.storage :as st]
   [datalevin.util :as u])
  (:import
   [java.util UUID]))

(def ^:private test-embedding-metadata
  {:embedding/provider {:kind :test
                        :id   :fake}
   :embedding/output   {:dimensions 2}})

(defn- constant-vector-provider
  [calls]
  (let [closed? (atom false)]
    (reify
      emb/IEmbeddingProvider
      (embedding [_ items _opts]
        (swap! calls + (count items))
        (mapv (fn [_] (float-array [1.0 0.0])) items))
      (embedding-metadata [_]
        test-embedding-metadata)
      (embedding-dimensions [_]
        2)
      (close-provider [_]
        (reset! closed? true))

      java.lang.AutoCloseable
      (close [this]
        (emb/close-provider this)))))

(deftest bulk-load-indexes-embedding-datoms
  (let [calls    (atom 0)
        provider (constant-vector-provider calls)
        dir      (u/tmp-dir (str "test-embedding-bulk-" (UUID/randomUUID)))
        schema   {:text {:db/valueType :db.type/string
                         :db/embedding true}}
        opts     {:embedding-providers {:default provider}
                  :embedding-opts      {:dimensions          2
                                        :embedding-metadata
                                        test-embedding-metadata}}
        db       (d/init-db [(d/datom 1 :text "hello")] dir schema opts)]
    (try
      (is (= 1 @calls))
      (is (= [[1 :text "hello"]]
             (mapv vec (bi/embedding-neighbors db
                                                "hello"
                                                {:domains ["datalevin"]
                                                 :top     1}))))
      (is (= 2 @calls))
      (finally
        (d/close-db db)
        (u/delete-files dir)))))

(deftest shared-open-reuses-owned-embedding-provider
  (let [created (atom 0)
        closed  (atom [])
        live-count #(- (long @created) (long (count @closed)))
        factory (fn [_]
                  (let [id      (swap! created inc)
                        closed? (atom false)]
                    (reify
                      emb/IEmbeddingProvider
                      (embedding [_ items _opts]
                        (mapv (fn [_] (float-array [1.0 0.0])) items))
                      (embedding-metadata [_]
                        test-embedding-metadata)
                      (embedding-dimensions [_]
                        2)
                      (close-provider [_]
                        (when (compare-and-set! closed? false true)
                          (swap! closed conj id)))

                      java.lang.AutoCloseable
                      (close [this]
                        (emb/close-provider this)))))
        dir     (u/tmp-dir (str "test-embedding-shared-" (UUID/randomUUID)))
        schema  {:text {:db/valueType :db.type/string
                        :db/embedding true}}
        opts    {:embedding-opts {:provider           :openai-compatible
                                  :model              "fake-model"
                                  :dimensions         2
                                  :embedding-metadata
                                  test-embedding-metadata}}]
    (with-redefs [emb/*openai-compatible-provider-factory* factory]
      (let [s1 (st/open dir schema opts)]
        (try
          (emb/embedding (st/embedding-provider s1 "datalevin") ["a"] nil)
          (is (= 1 (live-count)))
          (let [s2 (st/open dir schema opts)]
            (try
              (emb/embedding (st/embedding-provider s2 "datalevin") ["b"] nil)
              (is (= 1 (live-count)))
              (finally
                (i/close s2))))
          (is (= 1 (live-count)))
          (finally
            (i/close s1)
            (u/delete-files dir))))
      (is (= 0 (live-count))))))

(deftest openai-compatible-provider-reuses-http-client
  (let [clients  (atom [])
        provider (emb/init-embedding-provider
                   {:provider   :openai-compatible
                    :model      "fake-model"
                    :dimensions 2})]
    (with-redefs [emb/*openai-compatible-request!*
                  (fn [{:keys [client]}]
                    (swap! clients conj client)
                    {:status 200
                     :body   {:data [{:embedding [1.0 0.0]}]}})]
      (try
        (emb/embedding provider ["a"] nil)
        (emb/embedding provider ["b"] nil)
        (is (= 2 (count @clients)))
        (is (every? some? @clients))
        (is (identical? (first @clients) (second @clients)))
        (finally
          (emb/close-provider provider))))))

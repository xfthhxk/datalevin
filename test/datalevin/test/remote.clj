(ns datalevin.test.remote
  (:require
   [clojure.test :refer [deftest is use-fixtures]]
   [datalevin.client :as cl]
   [datalevin.constants :as c]
   [datalevin.core :as d]
   [datalevin.datom :as dd]
   [datalevin.test.core :refer [server-fixture]])
  (:import
   [java.util UUID]))

(use-fixtures :each server-fixture)

(defn- remote-uri
  [db-name]
  (str "dtlv://"
       c/default-username ":"
       c/default-password
       "@localhost:" cl/*default-port*
       "/" db-name))

(deftest remote-assoc-opt-requires-alter-permission-test
  (let [db-name       (str "remote-assoc-opt-auth-" (UUID/randomUUID))
        username      (str "viewer-" (UUID/randomUUID))
        password      "viewer-secret"
        admin-uri     (str "dtlv://"
                           c/default-username ":"
                           c/default-password
                           "@localhost:" cl/*default-port*)
        viewer-uri    (str "dtlv://"
                           username ":"
                           password
                           "@localhost:" cl/*default-port*)
        admin-client  (cl/new-client admin-uri)
        viewer-client (atom nil)]
    (try
      (cl/create-database admin-client db-name c/dl-type)
      (cl/create-user admin-client username password)
      (cl/grant-permission admin-client
                           (keyword "datalevin.role" username)
                           :datalevin.server/view
                           :datalevin.server/database
                           db-name)
      (reset! viewer-client (cl/new-client viewer-uri))
      (cl/open-database @viewer-client db-name c/db-store-datalog)
      (is (thrown-with-msg?
           Exception
           #"Don't have permission to alter the database"
           (cl/normal-request
            @viewer-client :assoc-opt [db-name :validate-data? false])))
      (finally
        (when @viewer-client
          (cl/disconnect @viewer-client))
        (cl/disconnect admin-client)))))

(deftest remote-server-local-options-require-server-control-test
  (let [open-db-name  (str "remote-ha-open-auth-" (UUID/randomUUID))
        assoc-db-name (str "remote-ha-assoc-auth-" (UUID/randomUUID))
        username      (str "creator-" (UUID/randomUUID))
        password      "creator-secret"
        role-key      (keyword "datalevin.role" username)
        hook          {:cmd ["/bin/echo" "ok"]
                       :timeout-ms 1000
                       :retries 0
                       :retry-delay-ms 0}
        admin-uri     (str "dtlv://"
                           c/default-username ":"
                           c/default-password
                           "@localhost:" cl/*default-port*)
        creator-uri   (str "dtlv://"
                           username ":"
                           password
                           "@localhost:" cl/*default-port*)
        admin-client  (cl/new-client admin-uri)
        creator-client (atom nil)]
    (try
      (cl/create-user admin-client username password)
      (cl/grant-permission admin-client
                           role-key
                           :datalevin.server/create
                           :datalevin.server/database
                           nil)
      (reset! creator-client (cl/new-client creator-uri))
      (is (thrown-with-msg?
           Exception
           #"Server control permission is required"
           (cl/open-database
            @creator-client
            open-db-name
            c/db-store-datalog
            nil
            {:ha-fencing-hook hook})))
      (is (thrown-with-msg?
           Exception
           #"Server control permission is required"
           (cl/open-database
            @creator-client
            open-db-name
            c/db-store-datalog
            nil
            {:snapshot-dir "/tmp/datalevin-denied-snapshots"})))
      (is (thrown-with-msg?
           Exception
           #"Server control permission is required"
           (cl/open-database
            @creator-client
            open-db-name
            c/db-store-datalog
            nil
            {:runtime-opts {:ha-require-udf-ready? true}})))

      (cl/create-database admin-client assoc-db-name c/dl-type)
      (cl/grant-permission admin-client
                           role-key
                           :datalevin.server/view
                           :datalevin.server/database
                           assoc-db-name)
      (cl/grant-permission admin-client
                           role-key
                           :datalevin.server/alter
                           :datalevin.server/database
                           assoc-db-name)
      (cl/open-database @creator-client assoc-db-name c/db-store-datalog)
      (is (thrown-with-msg?
           Exception
           #"Server control permission is required"
           (cl/normal-request
            @creator-client
            :assoc-opt
            [assoc-db-name :ha-fencing-hook hook])))
      (is (thrown-with-msg?
           Exception
           #"Server control permission is required"
           (cl/normal-request
            @creator-client
            :assoc-opt
            [assoc-db-name :snapshot-dir "/tmp/datalevin-denied-snapshots"])))
      (is (thrown-with-msg?
           Exception
           #"Server control permission is required"
           (cl/normal-request
            @creator-client
            :assoc-opt
            [assoc-db-name :runtime-opts {:ha-require-udf-ready? true}])))
      (finally
        (when @creator-client
          (cl/disconnect @creator-client))
        (cl/disconnect admin-client)))))

(deftest remote-db-state-sync-optional-test
  (let [db-name (str "remote-state-sync-" (UUID/randomUUID))
        conn    (d/get-conn (remote-uri db-name)
                            {:user/email {:db/unique :db.unique/identity}})]
    (try
      (d/transact! conn [{:user/email "eva@example.com"}])
      (is (= "eva@example.com"
             (-> (d/entity @conn [:user/email "eva@example.com"])
                 d/touch
                 :user/email)))
      (is (= [(dd/datom 1 :user/email "eva@example.com")]
             (d/datoms @conn :eav)))

      (d/update-schema conn {:identifier {:db/valueType :db.type/long}})
      (d/transact! conn [{:identifier 1} {:identifier 2}])
      (is (= [(dd/datom 1 :user/email "eva@example.com")
              (dd/datom 2 :identifier 1)
              (dd/datom 3 :identifier 2)]
             (d/datoms @conn :eav)))
      (finally
        (d/close conn)))))

(deftest remote-q-placeholder-value-test
  (let [db-name (str "remote-q-placeholder-" (UUID/randomUUID))
        conn    (d/get-conn (remote-uri db-name))]
    (try
      (is (= #{} (d/q '[:find ?e
                        :in $
                        :where [?e :dataset/name _]]
                      @conn)))
      (d/transact! conn [{:db/id 1 :dataset/name "foo"}])
      (is (= #{[1]} (d/q '[:find ?e
                           :in $
                           :where [?e :dataset/name _]]
                         @conn)))
      (is (= #{[1]} (d/q '[:find ?e
                           :in $
                           :where [?e :dataset/name "foo"]]
                         @conn)))
      (finally
        (d/close conn)))))

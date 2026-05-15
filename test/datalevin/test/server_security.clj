(ns datalevin.test.server-security
  (:require
   [clojure.test :refer [deftest is]]
   [datalevin.constants :as c]
   [datalevin.server :as srv]
   [datalevin.test.core :refer [allocate-port]]
   [datalevin.util :as u])
  (:import
   [java.util UUID]))

(defn- with-default-password-env-unset
  [f]
  (with-redefs-fn {#'datalevin.server/default-password-env-set?
                   (constantly false)}
    f))

(deftest non-loopback-bind-requires-configured-default-password-test
  (let [dir (u/tmp-dir (str "server-security-test-" (UUID/randomUUID)))]
    (try
      (with-default-password-env-unset
        #(is (thrown-with-msg?
               Exception
               #"Refusing to bind Datalevin server to non-loopback host"
               (srv/create {:host "0.0.0.0"
                            :port (allocate-port)
                            :root dir}))))
      (finally
        (when (u/file-exists dir)
          (u/delete-files dir))))))

(deftest loopback-bind-allows-built-in-default-password-test
  (let [dir    (u/tmp-dir (str "server-security-test-" (UUID/randomUUID)))
        server (atom nil)]
    (try
      (with-default-password-env-unset
        #(do
           (reset! server
                   (binding [c/*db-background-sampling?* false]
                     (srv/create {:host "127.0.0.1"
                                  :port (allocate-port)
                                  :root dir})))
           (is (some? @server))))
      (finally
        (when-some [server @server]
          (srv/stop server))
        (when (u/file-exists dir)
          (u/delete-files dir))))))

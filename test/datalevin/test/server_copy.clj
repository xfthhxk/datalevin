;;
;; Copyright (c) Huahai Yang. All rights reserved.
;; The use and distribution terms for this software are covered by the
;; Eclipse Public License 2.0 (https://opensource.org/license/epl-2.0)
;; which can be found in the file LICENSE at the root of this distribution.
;; By using this software in any fashion, you are agreeing to be bound by
;; the terms of this license.
;; You must not remove this notice, or any other, from this software.
;;
(ns datalevin.test.server-copy
  (:require
   [clojure.test :refer [deftest is]]
   [datalevin.constants :as c]
   [datalevin.ha.replication :as drep]
   [datalevin.interface :as i]
   [datalevin.server.copy :as scopy]))

(defn- fake-kv-store
  ([values]
   (fake-kv-store values nil))
  ([values sync-calls]
   (fake-kv-store values sync-calls nil))
  ([values sync-calls watermarks]
   (reify i/ILMDB
     (closed-kv? [_] false)
     (env-opts [_] (:env-opts values))
     (sync [_]
       (when sync-calls
         (swap! sync-calls conj nil))
       {:synced? true})
     (sync [_ force]
       (when sync-calls
         (swap! sync-calls conj force))
       {:synced? true
        :force force})
     (get-value [_ dbi-name k]
       (get values [dbi-name k]))
     (get-value [_ dbi-name k _k-type]
       (get values [dbi-name k]))
     (get-value [_ dbi-name k _k-type _v-type]
       (get values [dbi-name k]))
     (get-value [_ dbi-name k _k-type _v-type _ignore-key?]
       (get values [dbi-name k]))

     i/ITxLog
     (txlog-watermarks [_]
       (or watermarks {:wal? false})))))

(deftest copy-response-meta-uses-copied-store-payload-lsn-test
  (let [copied-store
        (fake-kv-store {[c/kv-info c/wal-snapshot-current-lsn] 18
                        [c/kv-info c/wal-local-payload-lsn]    19
                        [c/opts :db-identity]                  "copied-id"})
        live-store
        (fake-kv-store {[c/kv-info c/wal-snapshot-current-lsn] 24
                        [c/kv-info c/wal-local-payload-lsn]    24})]
    (binding [drep/*ha-current-state-fn* (fn [] {:store live-store})]
      (is (= {:db-name                   "db"
              :db-identity               "copied-id"
              :snapshot-last-applied-lsn 18
              :payload-last-applied-lsn  19}
             (select-keys
              (scopy/copy-response-meta "db" copied-store {})
              [:db-name
               :db-identity
               :snapshot-last-applied-lsn
               :payload-last-applied-lsn]))))))

(deftest copy-response-meta-uses-txlog-watermark-as-payload-floor-test
  (let [copied-store
        (fake-kv-store {[c/kv-info c/wal-snapshot-current-lsn] 15
                        [c/kv-info c/wal-local-payload-lsn]    15
                        [c/opts :db-identity]                  "copied-id"}
                       nil
                       {:wal? true
                        :last-applied-lsn 23})]
    (is (= {:db-name                   "db"
            :db-identity               "copied-id"
            :snapshot-last-applied-lsn 15
            :payload-last-applied-lsn  23
            :txlog-last-applied-lsn    23}
           (select-keys
            (scopy/copy-response-meta "db" copied-store {})
            [:db-name
             :db-identity
             :snapshot-last-applied-lsn
             :payload-last-applied-lsn
             :txlog-last-applied-lsn])))))

(deftest sync-copy-response-store-syncs-copied-lmdb-test
  (let [sync-calls (atom [])
        copied-store (fake-kv-store {} sync-calls)]
    (is (identical? copied-store
                    (scopy/sync-copy-response-store! copied-store)))
    (is (= [1] @sync-calls))))

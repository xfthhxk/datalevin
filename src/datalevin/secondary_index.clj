;;
;; Copyright (c) Huahai Yang. All rights reserved.
;; The use and distribution terms for this software are covered by the
;; Eclipse Public License 2.0 (https://opensource.org/license/epl-2.0)
;; which can be found in the file LICENSE at the root of this distribution.
;; By using this software in any fashion, you are agreeing to be bound by
;; the terms of this license.
;; You must not remove this notice, or any other, from this software.
;;
(ns ^:no-doc datalevin.secondary-index
  "Helpers for durable secondary index work."
  (:require
   [datalevin.constants :as c]
   [datalevin.lmdb :as lmdb]
   [datalevin.util :as u]))

(def supported-indexing-modes
  #{:sync :async})

(def default-indexing-mode
  :sync)

(def supported-job-types
  #{:fulltext :vector :embedding :idoc})

(def supported-job-ops
  #{:add :delete})

(def pending-status
  :pending)

(defn normalize-indexing-mode
  [mode]
  (if (nil? mode)
    default-indexing-mode
    (if (supported-indexing-modes mode)
      mode
      (u/raise "Unsupported secondary indexing mode"
               {:mode mode
                :expected supported-indexing-modes}))))

(defn sync-indexing?
  [opts]
  (= :sync (normalize-indexing-mode (:indexing-mode opts))))

(defn async-indexing?
  [opts]
  (= :async (normalize-indexing-mode (:indexing-mode opts))))

(defn- required-job-value
  [job k]
  (let [v (get job k)]
    (when (nil? v)
      (u/raise "Secondary index job is missing required value"
               {:key k
                :job job}))
    v))

(defn make-job
  "Build a durable secondary index job record.

  `:tx` and `:ordinal` form a stable job id for the source transaction."
  [{:keys [domain ref value ordinal created-ms updated-ms]
    :as job}]
  (let [type (required-job-value job :type)
        op (required-job-value job :op)
        tx (long (required-job-value job :tx))
        ordinal (long (or ordinal 0))
        created-ms (long (or created-ms (System/currentTimeMillis)))
        updated-ms (long (or updated-ms created-ms))]
    (when-not (supported-job-types type)
      (u/raise "Unsupported secondary index job type"
               {:type type
                :expected supported-job-types}))
    (when-not (supported-job-ops op)
      (u/raise "Unsupported secondary index job op"
               {:op op
                :expected supported-job-ops}))
    {:job/id         [type domain tx ordinal]
     :job/type       type
     :job/domain     domain
     :job/op         op
     :job/ref        ref
     :job/value      value
     :job/tx         tx
     :job/ordinal    ordinal
     :job/status     (or (:status job) pending-status)
     :job/attempts   (long (or (:attempts job) 0))
     :job/created-ms created-ms
     :job/updated-ms updated-ms}))

(defn job-tx
  "Return an LMDB put operation that enqueues `job`."
  [job]
  (let [job (if (:job/id job) job (make-job job))]
    (lmdb/kv-tx :put c/secondary-index-jobs (:job/id job) job :data :data)))

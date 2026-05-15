(ns datalevin.test.bits
  (:require
   [clojure.test :refer [deftest is]]
   [datalevin.bits :as b]
   [taoensso.nippy :as nippy]))

(deftest deserialize-supports-current-and-legacy-nippy-payloads-test
  (let [payload {:op :init-membership-hash
                 :membership-hash "abc123"}]
    (is (= payload (b/deserialize (b/serialize payload))))
    (is (= payload (b/deserialize (nippy/freeze payload))))))

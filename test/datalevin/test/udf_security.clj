(ns datalevin.test.udf-security
  (:require
   [clojure.test :refer [deftest is]]
   [datalevin.udf :as udf]))

(def ^:private descriptor
  {:udf/lang :test
   :udf/kind :query-fn
   :udf/id   :normalize-email})

(deftest udf-descriptor-rejects-unsupported-keys-test
  (is (thrown-with-msg?
       clojure.lang.ExceptionInfo
       #"UDF descriptor contains unsupported key"
       (udf/descriptor (assoc descriptor :class "java.lang.Runtime")))))

(deftest udf-descriptor-rejects-composite-version-test
  (is (thrown-with-msg?
       clojure.lang.ExceptionInfo
       #"UDF descriptor :udf/version"
       (udf/descriptor (assoc descriptor :udf/version {:major 1})))))

(deftest udf-descriptor-allows-scalar-version-test
  (is (= (assoc descriptor :udf/version "1")
         (udf/descriptor (assoc descriptor :udf/version "1")))))

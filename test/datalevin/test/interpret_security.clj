(ns datalevin.test.interpret-security
  (:require
   [clojure.test :refer [deftest is]]
   [datalevin.bits :as b]
   [datalevin.interpret :as interp]))

(deftest inter-fn-thaw-rejects-non-function-source-test
  (is (thrown-with-msg?
       clojure.lang.ExceptionInfo
       #"Invalid inter-fn source form"
       (#'interp/source->inter-fn
        '(do
           (spit "/tmp/datalevin-inter-fn-should-not-run" "bad")
           (fn [x] x)))))
  (is (thrown-with-msg?
       clojure.lang.ExceptionInfo
       #"Invalid inter-fn source form"
       (#'interp/source->inter-fn
        '(let [x (slurp "/etc/passwd")]
           (fn [] x))))))

(deftest inter-fn-thaw-allows-serialized-inter-fn-test
  (let [n       41
        f       (interp/inter-fn [x] (clojure.core/+ x n))
        thawed  (b/deserialize (b/serialize f))]
    (is (= 42 (thawed 1)))))

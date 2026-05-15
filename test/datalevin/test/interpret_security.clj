(ns datalevin.test.interpret-security
  (:require
   [clojure.test :refer [deftest is]]
   [datalevin.bits :as b]
   [datalevin.core :as d]
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

(deftest inter-fn-preserves-quoted-query-aggregate-symbols-test
  (let [xs [1 3 2]
        f  (interp/inter-fn []
             (d/q '[:find (max ?x) .
                    :in [?x ...]]
                  xs))]
    (is (= 3 (f)))))

(deftest inter-fn-rejects-host-io-and-interop-test
  (is (thrown-with-msg?
       clojure.lang.ExceptionInfo
       #"Disallowed inter-fn symbol slurp"
       (#'interp/source->inter-fn
        '(fn [] (slurp "/etc/hosts")))))
  (is (thrown-with-msg?
       clojure.lang.ExceptionInfo
       #"Disallowed inter-fn symbol java.lang.System/getProperty"
       (#'interp/source->inter-fn
        '(fn [] (java.lang.System/getProperty "user.home")))))
  (is (thrown-with-msg?
       clojure.lang.ExceptionInfo
       #"Disallowed inter-fn symbol \.toString"
       (#'interp/source->inter-fn
        '(fn [x] (.toString x))))))

(deftest inter-fn-rejects-server-local-datalevin-apis-test
  (is (thrown-with-msg?
       clojure.lang.ExceptionInfo
       #"Disallowed inter-fn symbol datalevin.core/empty-db"
       (#'interp/source->inter-fn
        '(fn [] (datalevin.core/empty-db)))))
  (is (thrown-with-msg?
       clojure.lang.ExceptionInfo
       #"Disallowed inter-fn symbol datalevin.core/get-conn"
       (#'interp/source->inter-fn
        '(fn [] (datalevin.core/get-conn "/tmp/inter-fn-should-not-open")))))
  (is (thrown-with-msg?
       clojure.lang.ExceptionInfo
       #"Disallowed inter-fn symbol datalevin.interpret/exec-code"
       (#'interp/source->inter-fn
        '(fn [] (datalevin.interpret/exec-code "(+ 1 2)"))))))

(deftest inter-fn-rejects-disallowed-symbols-inside-quoted-data-test
  (is (thrown-with-msg?
       clojure.lang.ExceptionInfo
       #"Disallowed inter-fn symbol clojure.core/slurp"
       (#'interp/source->inter-fn
        '(fn [x]
           (datalevin.core/q
            '[:find ?x
              :in ?x
              :where [(clojure.core/slurp "/etc/hosts")]]
            x))))))

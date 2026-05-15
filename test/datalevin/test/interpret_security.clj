(ns datalevin.test.interpret-security
  (:require
   [clojure.test :refer [deftest is]]
   [datalevin.bits :as b]
   [datalevin.core :as d]
  [datalevin.interpret :as interp])
  (:import
   [java.nio ByteBuffer]
   [java.util HashMap]))

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

(deftest inter-fn-allows-bounded-thread-sleep-test
  (let [macro-f        (interp/inter-fn [] (Thread/sleep 1) :done)
        qualified-f    (#'interp/source->inter-fn
                        '(fn [] (java.lang.Thread/sleep 1) :done))
        unqualified-f  (#'interp/source->inter-fn
                        '(fn [] (Thread/sleep 1) :done))
        out-of-bounds  (#'interp/source->inter-fn
                        '(fn [] (java.lang.Thread/sleep 1001) :done))]
    (is (= :done (macro-f)))
    (is (= :done (qualified-f)))
    (is (= :done (unqualified-f)))
    (is (thrown-with-msg?
         clojure.lang.ExceptionInfo
         #"inter-fn Thread/sleep exceeds allowed bound"
         (out-of-bounds)))))

(deftest inter-fn-allows-selected-storage-read-helpers-test
  (let [bf        (ByteBuffer/allocate 16)
        read-long (interp/inter-fn [bb] (b/read-buffer bb :long))
        core-read (interp/inter-fn [bb] (d/read-buffer bb :long))]
    (b/put-bf bf 42 :long)
    (is (= 42 (read-long bf)))
    (.rewind bf)
    (is (= 42 (core-read bf))))
  (is (fn? (#'interp/source->inter-fn
            '(fn [kv] (datalevin.lmdb/v kv)))))
  (is (fn? (#'interp/source->inter-fn
            '(fn [kv] (datalevin.core/k kv)))))
  (is (fn? (#'interp/source->inter-fn
            '(fn [iter] (datalevin.lmdb/has-next-val iter)))))
  (is (thrown-with-msg?
       clojure.lang.ExceptionInfo
       #"Disallowed inter-fn symbol datalevin.bits/put-buffer"
       (#'interp/source->inter-fn
        '(fn [bf x] (datalevin.bits/put-buffer bf x :long)))))
  (is (thrown-with-msg?
       clojure.lang.ExceptionInfo
       #"Disallowed inter-fn symbol datalevin.lmdb/open-kv"
       (#'interp/source->inter-fn
        '(fn [dir] (datalevin.lmdb/open-kv dir))))))

(deftest inter-fn-allows-nested-inter-fn-test
  (let [outer (interp/inter-fn [n]
                (interp/inter-fn [x]
                  (clojure.core/+ x n)))
        inner (outer 40)]
    (is (interp/inter-fn? inner))
    (is (= 42 (inner 2))))
  (is (thrown-with-msg?
       clojure.lang.ExceptionInfo
       #"Disallowed inter-fn symbol datalevin.interpret/compile-inter-fn-source"
       (#'interp/source->inter-fn
        '(fn [src] (datalevin.interpret/compile-inter-fn-source src))))))

(deftest inter-fn-allows-selected-interop-methods-test
  (let [index-of (interp/inter-fn [^String text token]
                   (.indexOf text token))
        hm       (HashMap.)
        put-entry (interp/inter-fn [k v]
                    (.put hm k v))]
    (is (= 2 (index-of "abc" "c")))
    (put-entry :a 1)
    (is (= {:a 1} (into {} hm)))))

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
        '(fn [x] (.toString x)))))
  (is (thrown-with-msg?
       clojure.lang.ExceptionInfo
       #"Disallowed inter-fn symbol \.clear"
       (#'interp/source->inter-fn
        '(fn [x] (.clear x)))))
  (is (thrown-with-msg?
       clojure.lang.ExceptionInfo
       #"Disallowed inter-fn symbol java\.util\.HashMap\."
       (#'interp/source->inter-fn
        '(fn [] (java.util.HashMap.)))))
  (is (thrown-with-msg?
       clojure.lang.ExceptionInfo
       #"Disallowed inter-fn symbol HashMap\."
       (#'interp/source->inter-fn
        '(fn [] (HashMap.))))))

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

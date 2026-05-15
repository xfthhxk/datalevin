(ns datalevin.test.server-auth
  (:require
   [clojure.test :refer [deftest is]]
   [datalevin.server.auth :as auth]))

(deftest password-matches-test
  (let [password "correct horse battery staple"
        salt     (auth/salt)
        hash     (auth/password-hashing password salt)]
    (is (true? (auth/password-matches? password hash salt)))
    (is (false? (auth/password-matches? "wrong password" hash salt)))
    (is (false? (auth/password-matches? nil hash salt)))
    (is (false? (auth/password-matches? password nil salt)))))

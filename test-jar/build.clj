(ns build
  (:require [clojure.tools.build.api :as b]))

(def version (format "1.2.%s" (b/git-count-revs nil)))
(def class-dir "target/classes")
(def uber-file "target/test-jar.jar")

(defn- datalevin-version []
  (or (second (re-find #"def version \"([0-9\.]+)\"" (slurp "../project.clj")))
      (throw (ex-info "Datalevin version not found in ../project.clj" {}))))

(defn- datalevin-jar []
  (let [jar (format "../target/datalevin-%s.jar" (datalevin-version))]
    (when-not (.exists (b/resolve-path jar))
      (throw (ex-info (str "Datalevin jar not found: " jar)
                      {:jar jar})))
    jar))

;; delay to defer side effects (artifact downloads)
(def basis
  (delay
    (b/create-basis
      {:project "deps.edn"
       :extra   {:deps {'datalevin/datalevin {:local/root (datalevin-jar)}}}})))

(defn clean [_]
  (b/delete {:path "target"}))

(defn uber [_]
  (clean nil)
  (b/copy-dir {:src-dirs   ["src" "resources"]
               :target-dir class-dir})
  (b/compile-clj {:basis      @basis
                  :ns-compile '[test-jar.core]
                  :class-dir  class-dir})
  (b/uber {:class-dir class-dir
           :uber-file uber-file
           :basis     @basis
           :main      'test-jar.core}))

;; Licensed to the Apache Software Foundation (ASF) under one
;; or more contributor license agreements. See the NOTICE file
;; distributed with this work for additional information
;; regarding copyright ownership. The ASF licenses this file
;; to you under the Apache License, Version 2.0 (the
;; "License"); you may not use this file except in compliance
;; with the License. You may obtain a copy of the License at
;;
;;   http://www.apache.org/licenses/LICENSE-2.0
;;
;; Unless required by applicable law or agreed to in writing,
;; software distributed under the License is distributed on an
;; "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
;; KIND, either express or implied. See the License for the
;; specific language governing permissions and limitations
;; under the License.

(ns jepsen.kudu-test
  (:require [clojure.test :refer :all]
            [jepsen.core :as jepsen]
            [jepsen.nemesis :as jn]
            [jepsen.tests :as tests]
            [jepsen.kudu :as kudu]
            [jepsen.kudu.nemesis :as kn]
            [jepsen.kudu.register :as kudu-register]))

(defn check
  [tcasefun opts]
  (is (:valid? (:results (jepsen/run! (tcasefun opts))))))

(defmacro dt
  [tfun tsuffix topts]
  (let [tname# (symbol (str (name tfun) "-" tsuffix))]
  `(clojure.test/deftest ~tname# (check ~tfun ~topts))))

(defn dt-func
  [tfun tscenario topts]
  `(dt ~tfun ~tscenario ~topts))

(defmacro instantiate-tests
  [tfun config topts]
  (let [seqtfun# (reduce (fn [out _] (conj out tfun)) [] (eval config))
        seqtscenario# (reduce (fn [out e]
                              (conj out (:scenario e))) [] (eval config))
        seqtopts# (reduce (fn [out e]
                            (conj out (merge (eval topts)
                                             {:nemesis (:nemesis e)})))
                          [] (eval config))]
    `(do ~@(map dt-func seqtfun# seqtscenario# seqtopts#))))

;; Configurations for tests.  Every configuration corresponds to running
;; a test with particular nemesis (let's call it "scenario").
(def register-test kudu-register/register-test)
(def register-test-configs
  [
   {:scenario "noop-nemesis"
    :nemesis '((fn [] jn/noop))}
   {:scenario "tserver-random-halves"
    :nemesis '(kn/tserver-partition-random-halves)}
   {:scenario "tserver-majorities-ring"
    :nemesis '(kn/tserver-partition-majorities-ring)}
   {:scenario "kill-restart-2-tservers"
    :nemesis '(kn/kill-restart-tserver (comp (partial take 2) shuffle))}
   {:scenario "kill-restart-3-tservers"
    :nemesis '(kn/kill-restart-tserver (comp (partial take 3) shuffle))}
   {:scenario "kill-restart-all-tservers"
    :nemesis '(kn/kill-restart-tserver shuffle)}
   {:scenario "all-random-halves"
    :nemesis '(jn/partition-random-halves)}
   {:scenario "all-majorities-ring"
    :nemesis '(jn/partition-majorities-ring)}
   {:scenario "hammer-2-tservers"
    :nemesis '(kn/tserver-hammer-time (comp (partial take 2) shuffle))}
   {:scenario "hammer-3-tservers"
    :nemesis '(kn/tserver-hammer-time (comp (partial take 3) shuffle))}
   {:scenario "hammer-all-tservers"
    :nemesis '(kn/tserver-hammer-time shuffle)}
   ])

(defmacro instantiate-all-kudu-tests
  [opts]
  `(instantiate-tests register-test register-test-configs ~opts))

(instantiate-all-kudu-tests {})

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

;;
;; This is a starter script for maven-clojure-plugin. The script parses passed
;; command-line arguments and starts Kudu jepsen tests with appropriate
;; parameters.
;;
;; The script is invoked via calling 'mvn clojure:run'. The 'clojure:run' target
;; is used instead of 'clojure:test' because the latter does not allow to
;; pass necessary customization parameters for the tests.
;;
;; The script accepts the following command-line options:
;;   --masters=<list_of_kudu_master_hostnames>
;;   --tservers=<list_of_kudu_tserver_hostnames>
;;   --ssh-key-path=<path_to_private_ssh_key_to_login_into_kudu_nodes or empty>
;;   --iter-num=<number_of_iterations_to_run> (default is 1)
;; The list of nodes/hostnames can be separated either by single space or comma.
;;

(ns jepsen.kudu-test-runner
  "Run Kudu jepsen tests via clojure-maven-plugin on 'mvn clojure:run'"
  (:require [clojure.tools.logging :refer :all]
            [clojure.string :as string]
            [clojure.test :refer [run-tests]]
            [clojure.tools.cli :refer [parse-opts]]
            [jepsen.nemesis :as jn]
            [jepsen.control :as jc]
            [jepsen.kudu.nemesis :as kn]
            [jepsen.kudu-test :refer [instantiate-all-kudu-tests]]))

(def parse-hostnames #(string/split % #"[, ]"))

(defn parse-path
  [input]
  (def trimmed (string/trim input))
  (if (= 0 (count trimmed)) nil trimmed))

(def cli-options
  [
   [:long-opt "--masters"
    :required "<nodes>"
    :desc "Set of Kudu master nodes"
    :missing "Kudu master nodes are missing"
    :parse-fn parse-hostnames]
   [:long-opt "--tservers"
    :required "<nodes>"
    :desc "Set of Kudu tserver nodes"
    :missing "Kudu tserver nodes are missing"
    :parse-fn parse-hostnames]
   [:long-opt "--ssh-key-path"
    :required "<path_to_private_ssh_key>"
    :desc "Path to the SSH private key to login into the Kudu nodes.
          If not specified or empty, keys are retrieved from SSH agent."
    :missing "Path to the SSH private key is not specified, using SSH agent."
    :parse-fn parse-path]
   [:long-opt "--iter-num"
    :required "<number_of_iterations>"
    :default 1
    :desc "Number of iterations to run the test suite in cycle."
    :parse-fn #(Integer/parseInt %)]
   ])

(defn get-cmd-line-opts
  []
  (let [{:keys [options arguments errors summary]}
        (parse-opts *command-line-args* cli-options)]
    options))

(do
  (def cmd-line-opts (get-cmd-line-opts))
  (def test-opts (dissoc cmd-line-opts :ssh-key-path :iter-num))
  (def private-key-path (:ssh-key-path cmd-line-opts))
  (def iter-num (:iter-num cmd-line-opts))
  ;; Custom reporting for the tests.
  (defmulti custom-report :type)
  (def old-report clojure.test/report)
  (defmethod custom-report :default [m]
  (old-report m))
  ;; Print the name of the test upon starting it.
  (defmethod custom-report :begin-test-var [m]
  (println (-> m :var meta :name)))
  (println "Running" iter-num "iteration(s) of the test suite")
  (println "Running with ssh key:" private-key-path)
  (println "Running with test options:" test-opts)
  (jepsen.kudu-test/instantiate-all-kudu-tests test-opts)
  (binding [jc/*strict-host-key-checking* :no
            jc/*private-key-path* private-key-path
            clojure.test/report custom-report]
    (loop [iteration 0]
      (when (< iteration iter-num)
        (let [summary (run-tests 'jepsen.kudu-test-runner)]
          (when-not (= 0 (:fail summary))
            (println "FAILURE: tests failed.")
            (System/exit 1))
          (when-not (= 0 (:error summary))
            (println "ERROR: encountered errors while running the tests.")
            (System/exit 1))
          (println "SUCCESS: all tests passed; no errors.")
          (recur (inc iteration)))))
          (System/exit 0)))

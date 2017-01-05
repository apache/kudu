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

(ns jepsen.kudu
  "Tests for Apache Kudu"
  (:require [clojure.tools.logging :refer :all]
            [clojure.java.io :as io]
            [clojure.java.shell :refer [sh]]
            [clojure.string :as str]
            [clojure.pprint :refer [pprint]]
            [jepsen
             [control :as c :refer [|]]
             [db :as db]
             [net :as net]
             [tests :as tests]
             [util :as util :refer [meh]]]
            [jepsen.control.net :as cnet :refer [heal]]
            [jepsen.control.util :as cu]
            [jepsen.os.debian :as debian]
            [jepsen.kudu.nemesis :as kn]
            [jepsen.kudu.util :as ku]))

(defn db
  "The setup/teardown procedure for a Kudu node.  A node can run either
  a master or a tablet server."
  []
  (reify db/DB
    (setup! [_ test node]
      (info node "Setting up environment")
      (c/su
        ;; Restore the network.  This is to clean-up left-overs from prior
        ;; nemesis-induced grudges.
        (meh (cnet/heal))

        (c/exec :service :rsyslog :start)

        (ku/prepare-node test node)
        (ku/sync-time test node)
        (ku/start-kudu test node))
      (info node "Kudu ready"))

    (teardown! [_ test node]
      (info node "Tearing down Kudu")
      (c/su
        (when (.contains (:tservers test) node)
          (ku/stop-kudu-tserver test node))
        (when (.contains (:masters test) node)
          (ku/stop-kudu-master test node)))
        ;; TODO collect table data for debugging
      (info node "Kudu stopped"))

    db/LogFiles
    (log-files [_ test node]
      (cond-> []
        (.contains (:tservers test) node) (conj ku/kudu-tserver-log-file)
        (.contains (:masters test) node) (conj ku/kudu-master-log-file)))))


(defn merge-options
  "Merges the common options for all Kudu tests with the specific options
  set on the test itself. This does not include 'db' or 'nodes'."
  [opts]
  (let [default-opts {:os         debian/os
                      :net        net/iptables
                      :db         (db)
                      ;; The list of nodes that will run tablet servers.
                      :tservers   [:n1 :n2 :n3 :n4 :n5]
                      ;; The list of nodes that will run the kudu master.
                      :masters    [:m1]
                      :table-name
                        (str (:name opts) "-" (System/currentTimeMillis))
                      :ts-hb-interval-ms 1000
                      :ts-hb-max-failures-before-backoff 3
                      :ts-raft-hb-interval-ms 50
                      :ranges      []}

        custom-opts (merge default-opts opts)

        derived-opts {:master-addresses
                      (ku/concatenate-addresses ku/master-rpc-port
                                                (:masters custom-opts))
                      :nodes (vec (concat (:tservers custom-opts)
                                          (:masters custom-opts)))}]
    (merge custom-opts derived-opts)))

;; Common setup for all kudu tests.
(defn kudu-test
  "Sets up the test parameters."
  [opts]
  (merge-options opts))

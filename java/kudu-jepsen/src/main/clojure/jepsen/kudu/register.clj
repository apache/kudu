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

(ns jepsen.kudu.register
  "Simple linearizability test for a read/write register."
  (:refer-clojure :exclude [test])
  (:require [jepsen
             [kudu :as kudu]
             [checker    :as checker]
             [generator  :as gen]
             [nemesis    :as nemesis]]
            [jepsen.kudu.client :as kc]
            [jepsen.kudu.table :as kt]
            [knossos.model :as model]))

(def register-key "x")

(defn r   [_ _] {:type :invoke, :f :read, :value nil})
(defn w   [_ _] {:type :invoke, :f :write, :value (rand-int 10)})

(defn client
  [client-atom kclient ktable]
  (reify jepsen.client/Client
    (setup! [_ test _]
      "Create the client and the test table. Use the same Kudu client instance "
      "across all test actors to achieve timestamp propagation for all "
      "operations."
      (let [table-name (:table-name test)
            kclient (locking client-atom
                     (when (compare-and-set!
                             client-atom nil (kc/sync-client (:master-addresses test)))
                       (kc/create-table @client-atom table-name kt/kv-table-schema
                         (let [ranges (:table-ranges test)
                               rep-factor (:num-replicas test)]
                           (if (nil? ranges)
                             (kt/kv-table-options-hash rep-factor (count (:tservers test)))
                             (kt/kv-table-options-range rep-factor ranges))))
                       @client-atom)
                       @client-atom)
            ktable (kc/open-table kclient table-name)]
        (client client-atom kclient ktable)))

    (invoke! [_ _ op]
      (case (:f op)
        :read  (assoc op :type :ok,
                         :value (kt/kv-read kclient ktable register-key))
        :write (do (kt/kv-write kclient ktable register-key (:value op))
                   (assoc op :type :ok))))

    (teardown! [_ _]
      (kc/close-client kclient))))

(defn register-test
  [opts]
  (kudu/kudu-test
    (merge
      {:name    "rw-register"
       :client (client (atom nil) nil nil)
       :concurrency 10
       :num-replicas 5
       :nemesis  nemesis/noop
       :model   (model/register)
       :generator (->> (gen/reserve 5 (gen/mix [w r]) r)
                       (gen/stagger 1/3)
                       (gen/nemesis
                         (gen/seq (cycle [(gen/sleep 5)
                                          {:type :info, :f :start}
                                          (gen/sleep 5)
                                          {:type :info, :f :stop}])))
                       (gen/time-limit 60))
       :checker (checker/compose
                  {:perf   (checker/perf)
                   :linear checker/linearizable})}
      opts)))

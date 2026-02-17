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

;; The 'sets' checker validates that:
;; 1) the total row count of a shared table read by a client is
;;    greater than or equal to the count of successful writes
;;    performed by that client.
;; 2) the row count never goes down from the previous reads of
;;    the same client.
;;
;; For example, sets' checker considers a history as follows valid:
;;
;;   [{:type :invoke, :f :add, :value 1, :process 0}
;;    {:type :ok, :f :add, :value 1, :process 0}
;;    {:type :invoke, :f :count, :value nil, :process 1}
;;    {:type :ok, :f :count, :value 2, :process 1}
;;    {:type :invoke, :f :add, :value 22, :process 0}
;;    {:type :ok, :f :add, :value 22, :process 0}
;;    {:type :invoke, :f :count, :value nil, :process 0}
;;    {:type :ok, :f :count, :value 2, :process 0}]
;;
;; while the following sequence as invalid. Since after process 0
;; successfully inserted three rows, the total number of rows
;; returned from the count operation of the same process is two,
;; which conflicts with the validation constraints:
;;
;;   [{:type :invoke, :f :add, :value 1, :process 0}
;;    {:type :ok, :f :add, :value 1, :process 0}
;;    {:type :invoke, :f :add, :value 2, :process 0}
;;    {:type :ok, :f :add, :value 2, :process 0}
;;    {:type :invoke, :f :count, :value nil, :process 0}
;;    {:type :ok, :f :count, :value 2, :process 0}
;;    {:type :invoke, :f :count, :value nil, :process 1}
;;    {:type :ok, :f :count, :value 2, :process 1}
;;    {:type :invoke, :f :add, :value 13, :process 1}
;:    {:type :ok, :f :add, :value 13, :process 1}
;;    {:type :invoke, :f :add, :value 22, :process 0}
;;    {:type :ok, :f :add, :value 22, :process 0}
;;    {:type :invoke, :f :count, :value nil, :process 0}
;;    {:type :ok, :f :count, :value 2, :process 0}
;;    {:type :invoke, :f :add, :value 250, :process 1}
;;    {:type :ok, :f :add, :value 250, :process 1}]
;;
;; This checker model is a variation of cockroach 'sets' checker,
;; see https://github.com/jepsen-io/jepsen/blob/master/cockroachdb/src/jepsen/cockroach/sets.clj.
(ns jepsen.kudu.sets
  "Set test"
  (:refer-clojure :exclude [test])
  (:require [jepsen [kudu :as kudu]
                    [client :as client]
                    [checker    :as checker]
                    [generator  :as gen]
                    [nemesis    :as nemesis]]
            [jepsen.kudu.client :as kc]
            [jepsen.kudu.table :as kt]
            [clojure.core.reducers :as r]
            [knossos.op :as op])
  (:import [org.apache.kudu.client AsyncKuduScanner$ReadMode]))

(defn add-op []
  "The add operation to be performed by the processes."
  (->> (range)
       (map (partial array-map
                     :type :invoke
                     :f :add
                     :value))))

(defn count-op []
  "The count operation to be performed by the processes."
  {:type :invoke, :f :count, :value nil})

(defn count-by-set
  "Given a set of :add operations or a :count operation, count the number
   of successful :add operations or get the value from the :count operation."
  [history]
  (let [adds    (->> history
                     (r/filter op/ok?)
                     (r/filter #(= :add (:f %)))
                     (r/map :value)
                     (into #{}))
        counts  (->> history
                     (r/filter op/ok?)
                     (r/filter #(= :count (:f %)))
                     (r/map :value)
                     (reduce (fn [_ x] x) nil))]
    {:counts counts :adds (count adds)}))

(defn validate-counts
  "Validate a series of :add and :count operations, making sure that:
    1) the total row count of the table read is greater than or equal
       to the count of successful writes.
    2) the count never goes down from the previous counts."
  [history]
  (let [results (reduce-kv
                  (fn [coll key value]
                    (let [last-coll (last coll)
                          last-count (:total-counts last-coll)
                          adds (+ (:total-adds last-coll 0) (:adds value))]
                      (if (= (:counts value) nil)
                        (conj coll (assoc value :total-counts last-count :total-adds adds
                                                :valid? (:valid? last-coll)))
                        ;; validates the total row count is greater than or equal
                        ;; to the count of writes, and the row count never goes down.
                        (let [is-valid (and (<= adds (:counts value))
                                            (<= last-count (:counts value)))]
                          (conj coll (assoc value :total-counts (:counts value) :total-adds adds
                                                  :valid? (and is-valid (:valid? last-coll))))))))
                  [{:valid? true :total-counts 0 :total-adds 0 }]
                  history)]
    results))

(defn partition-by-set
  "Given a history of operations, partition it to multiple groups of
   sets. Each set is composed of a set of :add operations followed by
   a :count operation. And validate each set."
  [history]
  (let [partition-history (->> history
                               (partition-by #(and (= :count (:f %))
                                                   (= :ok (:type %))))
                               (into []))]

    (let [count-map (reduce-kv
                      (fn [coll key value]
                        (assoc coll key (count-by-set value)))
                      {}
                      partition-history)]
      (let [result (->> count-map
                        (sort)
                        (vals)
                        (remove #(and (= (:counts %) nil) (= (:adds %) 0)))
                        (into [])
                        (validate-counts)
                        (last)
                        (:valid?))]
           {:valid? result}))))

(defn check-sets
  "Given the history of all processes/clients, validate the history per
   process/clients."
  []
  (reify checker/Checker
    (check [this test model history opts]
      (let [results (reduce-kv
                      (fn [coll key val]
                        (assoc coll key (partition-by-set val))) {} (group-by :process history))]
        (assoc results :valid? (jepsen.checker/merge-valid (mapcat vals (vals results))))))))

(defn client
  "Create a shared table if it doesn't exist. The client can perform
  two kinds of operations, :add that inserts a unique row into the
  table, :count that counts the number of rows of the table."
  [table-created? kclient ktable read-mode]
  (reify client/Client
    (setup! [_ test _]
      (let [kclient (kc/sync-client (:master-addresses test))
              table-name (:table-name test)
              ktable (locking table-created?
                       (when (compare-and-set! table-created? false true)
                         (kc/create-table kclient table-name kt/kv-table-schema
                           (let [ranges (:table-ranges test)
                                 rep-factor (:num-replicas test)]
                             (if (nil? ranges)
                               (kt/kv-table-options-hash rep-factor (count (:tservers test)))
                               (kt/kv-table-options-range rep-factor ranges)))))
                       (kc/open-table kclient table-name))]
          (client table-created? kclient ktable read-mode)))

     (invoke! [_ _ op]
       (case (:f op)
              :count (assoc op :type :ok,
                               :value (kt/count-rows kclient ktable read-mode))
              :add (do (kt/kv-write kclient ktable (str (:value op)) (:value op))
                   (assoc op :type :ok))))
     (teardown! [_ _]
       (kc/close-client kclient))))

(defn sets-test
  "This test creates multiple clients. Each client either writes a
   unique value in a shared table or counts the number of rows for that
   table concurrently. It uses 'sets' checker to validate Read-Your-Writes
   and Read-Your-Reads consistency."
  [opts]
  (kudu/kudu-test
    (merge
      {:name    "sets"
       :client (client (atom false) nil nil AsyncKuduScanner$ReadMode/READ_YOUR_WRITES)
       :concurrency 10
       :num-replicas 5
       :nemesis  nemesis/noop
       ;; generator take a random mixture of add operations (that inserts
       ;; a sequence of values) and count operations.
       :generator (->> (gen/mix [(->> (add-op)
                                      gen/seq
                                      (gen/stagger 1)) count-op])
                       (gen/stagger 1/3)
                       (gen/nemesis
                         (gen/seq (cycle [(gen/sleep 5)
                                          {:type :info, :f :start}
                                          (gen/sleep 5)
                                          {:type :info, :f :stop}])))
                       (gen/time-limit 60))
       :checker (check-sets)}
      opts)))
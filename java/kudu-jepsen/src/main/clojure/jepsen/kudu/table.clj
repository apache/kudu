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

(ns jepsen.kudu.table
  "Utilities to work with kudu tables, for testing."
  (:require [clojure.tools.logging :refer :all]
            [jepsen.kudu.client :as c])
  (:import [org.apache.kudu ColumnSchema
                            ColumnSchema$ColumnSchemaBuilder
                            Schema
                            Type])
  (:import [org.apache.kudu.client AbstractKuduScannerBuilder
                                   AsyncKuduScanner$ReadMode
                                   BaseKuduTest
                                   CreateTableOptions
                                   KuduClient
                                   KuduClient$KuduClientBuilder
                                   KuduPredicate
                                   KuduPredicate$ComparisonOp
                                   KuduScanner
                                   KuduSession
                                   KuduTable
                                   OperationResponse
                                   PartialRow
                                   RowResult
                                   RowResultIterator
                                   Upsert]))
;;
;; KV Table utilities
;;

;; Creates a basic schema for a Key/Value table where the key is a string and
;; the value is an int.
(def kv-table-schema
  (new Schema [(c/column-schema "key" Type/STRING true)
               (c/column-schema "value" Type/INT32 false)]))


(defn kv-table-options-range
  "Returns options to create a K/V table with partitions on 'ranges'.
  Ranges should be a vector of [start, end) keys.  The resulting table
  has (count ranges) tablets with the exact coverage set on the ranges.
  The resulting table has the specified replication factor."
  [num-replicas ranges]
  (let [options (new CreateTableOptions)]
    (.setRangePartitionColumns options ["key"])
    (.setNumReplicas options num-replicas)
    (doseq [range ranges]
      (let [lower (.newPartialRow kv-table-schema)
            upper (.newPartialRow kv-table-schema)]
        (.addString lower "key" (get range 0))
        (.addString upper "key" (get range 1))
        (.addRangePartition options lower upper)))
    options))


(defn kv-table-options-hash
  "Returns options to create a K/V table with key column hash partitioned
  into the given number of buckets.  The resulting table has the specified
  replication factor."
  [num-replicas buckets-num]
  (let [options (new CreateTableOptions)]
    (.setRangePartitionColumns options ["key"])
    (.setNumReplicas options num-replicas)
    (.addHashPartitions options ["key"] buckets-num)
    options))


(defn kv-write
  "Upsert a row on a KV table."
  [sync-client table key value]
  (let [upsert (.newUpsert table)
        row (.getRow upsert)]
    (.addString row "key" key)
    (.addInt row "value" (int value))
    (let [response (.apply (.newSession sync-client) upsert)]
      (assert (not (.hasRowError response)) (str "Got a row error: " response)))))

(defn kv-read
  "Read the value associated with key."
  [sync-client table key]
  (let [scanner-builder (.newScannerBuilder sync-client table)
        predicate (KuduPredicate/newComparisonPredicate (c/column-schema "key" Type/STRING)
                                                        KuduPredicate$ComparisonOp/EQUAL
                                                        key)]
    (.readMode scanner-builder AsyncKuduScanner$ReadMode/READ_AT_SNAPSHOT)
    (.addPredicate scanner-builder predicate)
    (let [rows (c/drain-scanner-to-tuples (.build scanner-builder))]
      (case (count rows)
        0 nil
        1 (:value (get rows 0))
        (assert false (str "Expected 0 or 1 rows. Got: " (count rows)))))))

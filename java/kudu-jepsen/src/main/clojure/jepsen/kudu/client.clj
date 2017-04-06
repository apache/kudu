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

(ns jepsen.kudu.client
  "Thin wrappers around Kudu Java client."
  (:require [clojure.tools.logging :refer :all])
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
                                   RowResultIterator]))

(defn sync-client
  "Builds and returns a new synchronous Kudu client."
  [master-addresses]
  (let [builder (new KuduClient$KuduClientBuilder master-addresses)
        client (. builder build)]
    client))

(defn close-client
  [sync-client]
  (try (.close sync-client) (catch Exception e (info "Error closing client: " e))))

(defn column-schema
  ([name type] (column-schema name type false))
  ([name type key?]
   (-> (new ColumnSchema$ColumnSchemaBuilder name, type) (.key key?) .build)))

(defn create-table
  [sync-client name schema options]
  (.createTable sync-client name schema options))

(defn open-table
  [sync-client name]
  (.openTable sync-client name))

(defn rr->tuple
  "Transforms a RowResult into a tuple."
  [row-result]
  (let [columns (-> row-result .getSchema .getColumns)]
    (into {}
          (for [[idx column] (map-indexed vector columns)]
            (let [name (.getName column)
                  type (.getType column)
                  value (condp = type
                          Type/INT8 (.getByte row-result idx)
                          Type/INT16 (.getShort row-result idx)
                          Type/INT32 (.getInt row-result idx)
                          Type/INT64 (.getLong row-result idx)
                          Type/BINARY (.getBinaryCopy row-result idx)
                          Type/STRING (.getString row-result idx)
                          Type/BOOL (.getBoolean row-result idx)
                          Type/FLOAT (.getFloat row-result idx)
                          Type/DOUBLE (.getDouble row-result idx)
                          Type/UNIXTIME_MICROS (.getLong row-result idx))]
              {(keyword name) value})))))

(defn drain-scanner-to-tuples
  "Drains a scanner to a vector of tuples."
  [scanner]
  (let [result (atom [])]
    (while (.hasMoreRows scanner)
      (let [rr-iter (.nextRows scanner)]
        (while (.hasNext rr-iter)
          (swap! result conj (rr->tuple (.next rr-iter))))))
    @result))

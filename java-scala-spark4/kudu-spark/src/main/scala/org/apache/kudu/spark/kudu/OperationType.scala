// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package org.apache.kudu.spark.kudu

import org.apache.kudu.client.KuduTable
import org.apache.kudu.client.Operation

/**
 * OperationType enumerates the types of Kudu write operations.
 */
private[kudu] sealed trait OperationType {
  def operation(table: KuduTable): Operation

  def toString(): String
}
private[kudu] case object Insert extends OperationType {
  override def operation(table: KuduTable): Operation = table.newInsert()

  override def toString(): String = "insert"
}
private[kudu] case object InsertIgnore extends OperationType {
  override def operation(table: KuduTable): Operation = table.newInsertIgnore()

  override def toString(): String = "insert_ignore"
}
private[kudu] case object Update extends OperationType {
  override def operation(table: KuduTable): Operation = table.newUpdate()

  override def toString(): String = "update"
}
private[kudu] case object UpdateIgnore extends OperationType {
  override def operation(table: KuduTable): Operation = table.newUpdateIgnore()

  override def toString(): String = "update_ignore"
}
private[kudu] case object Upsert extends OperationType {
  override def operation(table: KuduTable): Operation = table.newUpsert()

  override def toString(): String = "upsert"
}
private[kudu] case object Delete extends OperationType {
  override def operation(table: KuduTable): Operation = table.newDelete()

  override def toString(): String = "delete"
}
private[kudu] case object DeleteIgnore extends OperationType {
  override def operation(table: KuduTable): Operation = table.newDeleteIgnore()

  override def toString(): String = "delete_ignore"
}

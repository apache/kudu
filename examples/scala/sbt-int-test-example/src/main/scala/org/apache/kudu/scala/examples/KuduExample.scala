//
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

package org.apache.kudu.scala.examples

import org.apache.kudu.ColumnSchema.ColumnSchemaBuilder
import org.apache.kudu.client.{CreateTableOptions, KuduClient, KuduTable}
import org.apache.kudu.{Schema, Type}

import scala.collection.JavaConverters._
import scala.util.Try

class KuduExample(client: KuduClient) {

  private val cols = Seq(
    new ColumnSchemaBuilder("id", Type.INT64).key(true).build(),
    new ColumnSchemaBuilder("title", Type.STRING).build(),
    new ColumnSchemaBuilder("subtitle", Type.STRING).nullable(true).build(),
    new ColumnSchemaBuilder("releaseDate", Type.STRING).build(),
    new ColumnSchemaBuilder("director", Type.STRING).build()
  )

  def createMovieTable(name: String): Try[KuduTable] = {

    val schema = new Schema(cols.asJava)
    val options = new CreateTableOptions
    options.addHashPartitions(List("id").asJava, 2)
    Try(client.createTable(name, schema, options))
  }
}

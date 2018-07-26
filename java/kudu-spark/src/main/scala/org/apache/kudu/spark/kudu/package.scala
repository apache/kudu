/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.kudu.spark

import org.apache.spark.sql.{DataFrame, DataFrameReader, DataFrameWriter}

package object kudu {

  /**
   * Adds a method, `kudu`, to DataFrameReader that allows you to read Kudu tables using
   * the DataFrameReader.
   */
  implicit class KuduDataFrameReader(reader: DataFrameReader) {
    def kudu: DataFrame = reader.format("org.apache.kudu.spark.kudu").load
  }

  /**
   * Adds a method, `kudu`, to DataFrameWriter that allows writes to Kudu using
   * the DataFileWriter
   */
  implicit class KuduDataFrameWriter[T](writer: DataFrameWriter[T]) {
    def kudu = writer.format("org.apache.kudu.spark.kudu").save
  }
}

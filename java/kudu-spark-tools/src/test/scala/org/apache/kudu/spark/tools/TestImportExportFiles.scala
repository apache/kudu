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

package org.apache.kudu.spark.tools

import java.io.File
import java.nio.file.Files
import java.nio.file.Paths

import com.google.common.collect.ImmutableList
import org.apache.kudu.ColumnSchema.ColumnSchemaBuilder
import org.apache.kudu.Schema
import org.apache.kudu.Type
import org.apache.kudu.client.CreateTableOptions
import org.apache.kudu.client.KuduTable
import org.apache.kudu.spark.kudu._
import org.junit.Assert._
import org.junit.Before
import org.junit.Test

import scala.collection.JavaConverters._

class TestImportExportFiles extends KuduTestSuite {

  private val TableDataPath = "/TestImportExportFiles.csv"
  private val TableName = "TestImportExportFiles"
  private val TableSchema = {
    val columns = ImmutableList.of(
      new ColumnSchemaBuilder("key", Type.STRING).key(true).build(),
      new ColumnSchemaBuilder("column1_i", Type.STRING).build(),
      new ColumnSchemaBuilder("column2_d", Type.STRING)
        .nullable(true)
        .build(),
      new ColumnSchemaBuilder("column3_s", Type.STRING).build(),
      new ColumnSchemaBuilder("column4_b", Type.STRING).build()
    )
    new Schema(columns)
  }
  private val options = new CreateTableOptions()
    .setRangePartitionColumns(List("key").asJava)
    .setNumReplicas(1)

  @Before
  def setUp(): Unit = {
    kuduClient.createTable(TableName, TableSchema, options)
  }

  @Test
  def testCSVImport() {
    // Get the absolute path of the resource file.
    val schemaResource =
      classOf[TestImportExportFiles].getResource(TableDataPath)
    val dataPath = Paths.get(schemaResource.toURI).toAbsolutePath

    ImportExportFiles.testMain(
      Array(
        "--operation=import",
        "--format=csv",
        s"--master-addrs=${harness.getMasterAddressesAsString}",
        s"--path=$dataPath",
        s"--table-name=$TableName",
        "--delimiter=,",
        "--header=true",
        "--inferschema=true"
      ),
      ss
    )
    val rdd = kuduContext.kuduRDD(ss.sparkContext, TableName, List("key"))
    assert(rdd.collect.length == 4)
    assertEquals(rdd.collect().mkString(","), "[1],[2],[3],[4]")
  }

  @Test
  def testRoundTrips(): Unit = {
    val table = kuduClient.openTable(TableName)
    loadSampleData(table, 50)
    runRoundTripTest(TableName, s"$TableName-avro", "avro")
    runRoundTripTest(TableName, s"$TableName-csv", "csv")
    runRoundTripTest(TableName, s"$TableName-parquet", "parquet")
  }

  // TODO(KUDU-2454): Use random schemas and random data to ensure all type/values round-trip.
  private def loadSampleData(table: KuduTable, numRows: Int): Unit = {
    val session = kuduClient.newSession()
    val rows = Range(0, numRows).map { i =>
      val insert = table.newInsert
      val row = insert.getRow
      row.addString(0, i.toString)
      row.addString(1, i.toString)
      row.addString(3, i.toString)
      row.addString(4, i.toString)
      session.apply(insert)
    }
    session.close
  }

  private def runRoundTripTest(fromTable: String, toTable: String, format: String): Unit = {
    val dir = Files.createTempDirectory("round-trip")
    val path = new File(dir.toFile, s"$fromTable-$format").getAbsolutePath

    // Export the data.
    ImportExportFiles.testMain(
      Array(
        "--operation=export",
        s"--format=$format",
        s"--master-addrs=${harness.getMasterAddressesAsString}",
        s"--path=$path",
        s"--table-name=$fromTable",
        s"--header=true"
      ),
      ss
    )

    // Create the target table.
    kuduClient.createTable(toTable, TableSchema, options)

    // Import the data.
    ImportExportFiles.testMain(
      Array(
        "--operation=import",
        s"--format=$format",
        s"--master-addrs=${harness.getMasterAddressesAsString}",
        s"--path=$path",
        s"--table-name=$toTable",
        s"--header=true"
      ),
      ss
    )

    // Verify the tables match.
    // TODO(KUDU-2454): Verify every value to ensure all values round trip.
    val rdd1 = kuduContext.kuduRDD(ss.sparkContext, fromTable, List("key"))
    val rdd2 = kuduContext.kuduRDD(ss.sparkContext, toTable, List("key"))
    assertResult(rdd1.count())(rdd2.count())
  }
}

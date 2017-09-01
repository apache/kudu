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

import java.io.{File, FileOutputStream}

import org.apache.kudu.ColumnSchema.ColumnSchemaBuilder
import org.apache.kudu.{Schema, Type}
import org.apache.kudu.client.CreateTableOptions
import org.apache.kudu.spark.kudu._
import org.junit.Assert._
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import org.scalatest.{FunSuite, Matchers}
import org.spark_project.guava.collect.ImmutableList

import scala.collection.JavaConverters._

@RunWith(classOf[JUnitRunner])
class TestImportExportFiles  extends FunSuite with TestContext with  Matchers {

  private val TABLE_NAME: String = classOf[TestImportExportFiles].getName + "-" + System.currentTimeMillis

  test("Spark Import Export") {
    val schema: Schema = {
      val columns = ImmutableList.of(
        new ColumnSchemaBuilder("key", Type.STRING).key(true).build(),
        new ColumnSchemaBuilder("column1_i", Type.STRING).build(),
        new ColumnSchemaBuilder("column2_d", Type.STRING).nullable(true).build(),
        new ColumnSchemaBuilder("column3_s", Type.STRING).build(),
        new ColumnSchemaBuilder("column4_b", Type.STRING).build())
      new Schema(columns)
    }
    val tableOptions = new CreateTableOptions().setRangePartitionColumns(List("key").asJava)
      .setNumReplicas(1)
    kuduClient.createTable(TABLE_NAME, schema, tableOptions)

    val data: File = new File("target/", TABLE_NAME+".csv")
    writeCsvFile(data)

    ImportExportFiles.testMain(Array("--operation=import",
      "--format=csv",
      s"--master-addrs=${miniCluster.getMasterAddresses}",
      s"--path=${"target/"+TABLE_NAME+".csv"}",
      s"--table-name=$TABLE_NAME",
      "--delimiter=,",
      "--header=true",
      "--inferschema=true"), ss)
    val rdd = kuduContext.kuduRDD(ss.sparkContext, TABLE_NAME, List("key"))
    assert(rdd.collect.length == 4)
    assertEquals(rdd.collect().mkString(","),"[1],[2],[3],[4]")
  }

  def writeCsvFile(data: File)
  {
    val fos: FileOutputStream = new FileOutputStream(data)
    fos.write("key,column1_i,column2_d,column3_s,column4_b\n".getBytes)
    fos.write("1,3,2.3,some string,true\n".getBytes)
    fos.write("2,5,4.5,some more,false\n".getBytes)
    fos.write("3,7,1.2,wait this is not a double bad row,true\n".getBytes)
    fos.write("4,9,10.1,trailing separator isn't bad mkay?,true\n".getBytes)
    fos.close()
  }
}

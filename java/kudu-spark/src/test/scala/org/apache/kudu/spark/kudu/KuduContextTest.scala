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

import java.io.ByteArrayInputStream
import java.io.ByteArrayOutputStream
import java.io.ObjectInputStream
import java.io.ObjectOutputStream
import java.math.BigDecimal
import java.nio.charset.StandardCharsets.UTF_8
import java.sql.Date
import java.sql.Timestamp

import org.apache.spark.sql.types._
import org.apache.kudu.client._
import org.apache.kudu.Type
import scala.collection.JavaConverters._

import org.apache.kudu.util.DateUtil
import org.apache.kudu.util.TimestampUtil
import org.apache.spark.sql.functions.decode
import org.junit.Test
import org.scalatest.matchers.should.Matchers

class KuduContextTest extends KuduTestSuite with Matchers {
  val rowCount = 10

  private def serialize(value: Any): Array[Byte] = {
    val stream: ByteArrayOutputStream = new ByteArrayOutputStream()
    val oos = new ObjectOutputStream(stream)
    try {
      oos.writeObject(value)
      stream.toByteArray
    } finally {
      oos.close()
    }
  }

  private def deserialize(bytes: Array[Byte]): Any = {
    val ois = new ObjectInputStream(new ByteArrayInputStream(bytes))
    try {
      ois.readObject
    } finally {
      ois.close()
    }
  }

  @Test
  def testKuduContextSerialization() {
    val serialized = serialize(kuduContext)
    KuduClientCache.clearCacheForTests()
    val deserialized = deserialize(serialized).asInstanceOf[KuduContext]
    assert(deserialized.authnCredentials != null)
    // Make a nonsense call just to make sure the re-hydrated client works.
    deserialized.tableExists("foo")
  }

  @Test
  def testBasicKuduRDD() {
    val rows = insertRows(table, rowCount)
    val scanList = kuduContext
      .kuduRDD(
        ss.sparkContext,
        "test",
        Seq(
          "key",
          "c1_i",
          "c2_s",
          "c3_double",
          "c4_long",
          "c5_bool",
          "c6_short",
          "c7_float",
          "c8_binary",
          "c9_unixtime_micros",
          "c10_byte",
          "c11_decimal32",
          "c12_decimal64",
          "c13_decimal128",
          "c14_varchar",
          "c15_date"
        )
      )
      .map(r => r.toSeq)
      .collect()
    scanList.foreach(r => {
      val index = r.apply(0).asInstanceOf[Int]
      assert(r.apply(0).asInstanceOf[Int] == rows.apply(index)._1)
      assert(r.apply(1).asInstanceOf[Int] == rows.apply(index)._2)
      assert(r.apply(2).asInstanceOf[String] == rows.apply(index)._3)
      assert(r.apply(3).asInstanceOf[Double] == rows.apply(index)._2.toDouble)
      assert(r.apply(4).asInstanceOf[Long] == rows.apply(index)._2.toLong)
      assert(r.apply(5).asInstanceOf[Boolean] == (rows.apply(index)._2 % 2 == 1))
      assert(r.apply(6).asInstanceOf[Short] == rows.apply(index)._2.toShort)
      assert(r.apply(7).asInstanceOf[Float] == rows.apply(index)._2.toFloat)
      val binaryBytes = s"bytes ${rows.apply(index)._2}".getBytes(UTF_8).toSeq
      assert(r.apply(8).asInstanceOf[Array[Byte]].toSeq == binaryBytes)
      assert(
        r.apply(9).asInstanceOf[Timestamp] ==
          TimestampUtil.microsToTimestamp(rows.apply(index)._4))
      assert(r.apply(10).asInstanceOf[Byte] == rows.apply(index)._2.toByte)
      assert(r.apply(11).asInstanceOf[BigDecimal] == BigDecimal.valueOf(rows.apply(index)._2))
      assert(r.apply(12).asInstanceOf[BigDecimal] == BigDecimal.valueOf(rows.apply(index)._2))
      assert(r.apply(13).asInstanceOf[BigDecimal] == BigDecimal.valueOf(rows.apply(index)._2))
      assert(r.apply(14).asInstanceOf[String] == rows.apply(index)._3)
      assert(r.apply(15).asInstanceOf[Date] == DateUtil.epochDaysToSqlDate(rows.apply(index)._2))
    })
  }

  @Test
  def testKuduSparkDataFrame() {
    insertRows(table, rowCount)
    val sqlContext = ss.sqlContext
    val dataDF = sqlContext.read
      .options(Map("kudu.master" -> harness.getMasterAddressesAsString, "kudu.table" -> "test"))
      .format("kudu")
      .load
    dataDF
      .sort("key")
      .select("c8_binary")
      .first
      .get(0)
      .asInstanceOf[Array[Byte]]
      .shouldBe("bytes 0".getBytes(UTF_8))
    // decode the binary to string and compare
    dataDF
      .sort("key")
      .withColumn("c8_binary", decode(dataDF("c8_binary"), "UTF-8"))
      .select("c8_binary")
      .first
      .get(0)
      .shouldBe("bytes 0")
  }

  @Test
  def testArrayColumnReadWrite(): Unit = {
    insertRows(table, rowCount)

    val dataDF = ss.read
      .options(Map("kudu.master" -> harness.getMasterAddressesAsString, "kudu.table" -> "test"))
      .format("kudu")
      .load

    val sample = dataDF
      .select(
        "c16_bool_array",
        "c17_int8_array",
        "c18_int16_array",
        "c19_int32_array",
        "c20_int64_array",
        "c21_float_array",
        "c22_double_array",
        "c23_date_array",
        "c24_unixtime_array",
        "c25_string_array",
        "c26_varchar_array",
        "c27_binary_array",
        "c28_decimal_array"
      )
      .limit(1)
      .collect()(0)

    // Note: getAs[] will automatically convert nulls inside the Seq as null elements.

    val bools = sample.getAs[Seq[Boolean]]("c16_bool_array")
    val bytes = sample.getAs[Seq[Byte]]("c17_int8_array")
    val shorts = sample.getAs[Seq[Short]]("c18_int16_array")
    val ints = sample.getAs[Seq[Int]]("c19_int32_array")
    val longs = sample.getAs[Seq[Long]]("c20_int64_array")
    val floats = sample.getAs[Seq[Float]]("c21_float_array")
    val doubles = sample.getAs[Seq[Double]]("c22_double_array")
    val dates = sample.getAs[Seq[java.sql.Date]]("c23_date_array")
    val timestamps = sample.getAs[Seq[java.sql.Timestamp]]("c24_unixtime_array")
    val strings = sample.getAs[Seq[String]]("c25_string_array")
    val varchars = sample.getAs[Seq[String]]("c26_varchar_array")
    val binaries = sample.getAs[Seq[Array[Byte]]]("c27_binary_array")
    val decimals = sample.getAs[Seq[java.math.BigDecimal]]("c28_decimal_array")

    // Validate structure (size, presence)
    val allArrays = Seq(
      "bools" -> bools,
      "bytes" -> bytes,
      "shorts" -> shorts,
      "ints" -> ints,
      "longs" -> longs,
      "floats" -> floats,
      "doubles" -> doubles,
      "dates" -> dates,
      "timestamps" -> timestamps,
      "strings" -> strings,
      "varchars" -> varchars,
      "binaries" -> binaries,
      "decimals" -> decimals
    )

    allArrays.foreach {
      case (name, arr) =>
        assert(arr != null, s"$name array should not be null")
        assert(arr.nonEmpty, s"$name array should not be empty")
        assert(arr.size == 3, s"$name array should contain at least two elements")
    }

    // Verify expected head values or pattern
    assert(bools.head)
    assert(bytes.head == 0)
    assert(shorts.head == 0)
    assert(ints.head == 0)
    assert(longs.head == 0)
    assert(math.abs(floats.head - 0.0f) < 1e-6)
    assert(math.abs(doubles.head - 0.0) < 1e-12)
    assert(strings.head.startsWith("val-"))
    assert(varchars.head.startsWith("vchar-"))
    assert(new String(binaries.head, UTF_8).startsWith("bin-"))
    assert(decimals.head.compareTo(new java.math.BigDecimal("0.00")) == 0)

    // Check null propagation: middle element was intentionally null (validity false)
    allArrays.foreach {
      case (name, arr) =>
        if (arr.size > 1)
          assert(arr(1) == null, s"Middle element of $name should be null (validity=false)")
    }

    // Write back one row to ensure insert works
    val newDF = dataDF.limit(1).withColumn("key", dataDF("key") + 100)
    kuduContext.insertRows(newDF, "test")

    val checkDF = ss.read
      .options(Map("kudu.master" -> harness.getMasterAddressesAsString, "kudu.table" -> "test"))
      .format("kudu")
      .load

    assert(checkDF.filter("key = 100").count() == 1)
  }

  @Test
  def testSchemaDriftWithNewArrayColumn(): Unit = {
    val spark = ss
    import spark.implicits._

    val driftTable = "test_schema_drift_array"

    // Create table using Spark StructType
    val baseStruct = new StructType()
      .add("id", IntegerType, nullable = false)
      .add("name", StringType, nullable = true)

    val createOpts = new CreateTableOptions()
      .setRangePartitionColumns(List("id").asJava)
      .setNumReplicas(1)

    // use the KuduContext overload that accepts a Spark StructType
    kuduContext.createTable(driftTable, baseStruct, keys = Seq("id"), createOpts)

    // Seed some rows (no array column yet)
    val df1 = Seq((1, "foo"), (2, "bar")).toDF("id", "name")
    kuduContext.insertRows(df1, driftTable)

    // Evolve the DF schema by adding a new ARRAY<STRING> column
    val df2 = Seq(
      (3, "baz", Seq("alpha", "beta")),
      (4, "qux", Seq("gamma", "delta"))
    ).toDF("id", "name", "tags")

    // Write the rows with SchemaDrift enabled
    val writeOpts = KuduWriteOptions(handleSchemaDrift = true)
    kuduContext.insertRows(df2, driftTable, writeOpts)

    // Verify schema drift materialized the array column
    val kuduSchema = kuduContext.syncClient.openTable(driftTable).getSchema
    val tagsCol = kuduSchema.getColumn("tags")
    assert(tagsCol != null, "Schema drift should have added 'tags'")
    assert(tagsCol.isArray, "tags should be an array column")
    assert(tagsCol.getType == Type.NESTED, "array columns are stored as Type.NESTED")

    // Verify data round-trip
    val readDf = spark.read
      .options(Map("kudu.master" -> harness.getMasterAddressesAsString, "kudu.table" -> driftTable))
      .format("kudu")
      .load

    val rows = readDf.filter("id >= 3").orderBy("id").collect()
    assert(rows.head.getAs[Seq[String]]("tags") == Seq("alpha", "beta"))
  }
}

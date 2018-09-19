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
package org.apache.kudu.spark.kudu

import java.io.ByteArrayInputStream
import java.io.ByteArrayOutputStream
import java.io.ObjectInputStream
import java.io.ObjectOutputStream
import java.math.BigDecimal
import java.sql.Timestamp

import org.apache.kudu.util.TimestampUtil
import org.apache.spark.sql.functions.decode
import org.junit.Test
import org.scalatest.Matchers

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
          "c13_decimal128"
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
      val binaryBytes = s"bytes ${rows.apply(index)._2}".getBytes().toSeq
      assert(r.apply(8).asInstanceOf[Array[Byte]].toSeq == binaryBytes)
      assert(
        r.apply(9).asInstanceOf[Timestamp] ==
          TimestampUtil.microsToTimestamp(rows.apply(index)._4))
      assert(r.apply(10).asInstanceOf[Byte] == rows.apply(index)._2.toByte)
      assert(r.apply(11).asInstanceOf[BigDecimal] == BigDecimal.valueOf(rows.apply(index)._2))
      assert(r.apply(12).asInstanceOf[BigDecimal] == BigDecimal.valueOf(rows.apply(index)._2))
      assert(r.apply(13).asInstanceOf[BigDecimal] == BigDecimal.valueOf(rows.apply(index)._2))
    })
  }

  @Test
  def testKuduSparkDataFrame() {
    insertRows(table, rowCount)
    val sqlContext = ss.sqlContext
    val dataDF = sqlContext.read
      .options(Map("kudu.master" -> miniCluster.getMasterAddressesAsString, "kudu.table" -> "test"))
      .kudu
    dataDF
      .sort("key")
      .select("c8_binary")
      .first
      .get(0)
      .asInstanceOf[Array[Byte]]
      .shouldBe("bytes 0".getBytes)
    // decode the binary to string and compare
    dataDF
      .sort("key")
      .withColumn("c8_binary", decode(dataDF("c8_binary"), "UTF-8"))
      .select("c8_binary")
      .first
      .get(0)
      .shouldBe("bytes 0")
  }
}

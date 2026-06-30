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

import org.junit.Test
import org.scalatest.matchers.should.Matchers

/**
 * Unit tests for [[HdrHistogramAccumulator]] and [[HistogramWrapper]].
 *
 * These do not require a Kudu cluster: they exercise the accumulator's
 * value/zero semantics directly.
 */
class HdrHistogramAccumulatorTest extends Matchers {

  @Test
  def testIsZeroOnFreshAccumulator(): Unit = {
    val acc = new HdrHistogramAccumulator()
    acc.isZero shouldBe true
    acc.toString shouldBe "0ms"
  }

  @Test
  def testIsZeroAfterAdd(): Unit = {
    val acc = new HdrHistogramAccumulator()
    acc.add(5)
    acc.isZero shouldBe false
  }

  @Test
  def testCopyAndResetIsZero(): Unit = {
    val acc = new HdrHistogramAccumulator()
    acc.add(42)
    acc.isZero shouldBe false
    // Spark calls copyAndReset() when shipping the accumulator to executors;
    // the returned copy must be zero (Spark also asserts this internally).
    val fresh = acc.copyAndReset()
    fresh.isZero shouldBe true
  }

  @Test
  def testResetMakesZero(): Unit = {
    val acc = new HdrHistogramAccumulator()
    acc.add(7)
    acc.reset()
    acc.isZero shouldBe true
  }

  /**
   * Regression test: Spark's executor-heartbeater thread calls isZero() on the
   * accumulators of running tasks. The executor-side copy can have its internal
   * `histogram` field observed as null -- e.g. due to unsafe publication across
   * threads before the field write is visible, or constructor-bypassing
   * deserialization (Kryo) -- before the task records its first write. isZero()
   * must treat that as "zero" rather than throwing a NullPointerException,
   * because an uncaught exception on the heartbeater thread kills the executor.
   */
  @Test
  def testIsZeroWithNullHistogramDoesNotThrow(): Unit = {
    val acc = new HdrHistogramAccumulator()
    setFieldToNull(acc, classOf[HistogramWrapper])
    acc.isZero shouldBe true
    acc.toString shouldBe "0ms"
  }

  /** As above, but with the wrapper's inner Option observed as null. */
  @Test
  def testIsZeroWithNullInnerHistogramDoesNotThrow(): Unit = {
    val wrapper = new HistogramWrapper()
    setFieldToNull(wrapper, classOf[Option[_]])
    wrapper.isZero shouldBe true
    wrapper.toString shouldBe "0ms"
  }

  /**
   * Plain Java serialization of the wrapper (it is Serializable) must round-trip
   * to a valid, empty (zero) state.
   */
  @Test
  def testWrapperSerializationRoundTripIsZero(): Unit = {
    val wrapper = new HistogramWrapper()
    val restored = deserialize(serialize(wrapper)).asInstanceOf[HistogramWrapper]
    restored.isZero shouldBe true
  }

  /** Sets the first declared field of the given type on `target` to null. */
  private def setFieldToNull(target: AnyRef, fieldType: Class[_]): Unit = {
    val field = target.getClass.getDeclaredFields
      .find(f => fieldType.isAssignableFrom(f.getType))
      .getOrElse(throw new AssertionError(
        s"no field of type ${fieldType.getName} on ${target.getClass.getName}"))
    field.setAccessible(true)
    field.set(target, null)
  }

  private def serialize(value: Any): Array[Byte] = {
    val stream = new ByteArrayOutputStream()
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
}

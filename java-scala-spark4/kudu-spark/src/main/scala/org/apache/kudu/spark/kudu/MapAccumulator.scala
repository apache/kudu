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

import java.util.Collections
import java.util.function.BiConsumer
import java.util.function.BiFunction

import org.apache.spark.util.AccumulatorV2

/**
 * Spark accumulator implementation that takes 2-tuples as input and
 * Map[K, V] as output. The accumulator requires a merge function
 * to handle updates to existing entries in the map.
 *
 * @param mergeFn a function applied to two values for the same Map key
 * @tparam K type of the map key
 * @tparam V type of the map value
 */
class MapAccumulator[K, V](mergeFn: (V, V) => V)
    extends AccumulatorV2[(K, V), java.util.Map[K, V]] {
  import MapAccumulator._

  private val map = Collections.synchronizedMap(new java.util.HashMap[K, V]())
  private val mergeFunc = new SerializableBiFunction[V, V, V] {
    override def apply(t: V, u: V): V = mergeFn(t, u)
  }

  override def isZero: Boolean = map.isEmpty

  override def copy(): AccumulatorV2[(K, V), java.util.Map[K, V]] = {
    val newAcc = new MapAccumulator[K, V](mergeFn)
    map.synchronized {
      newAcc.map.putAll(map)
    }
    newAcc
  }

  override def reset(): Unit = map.clear()

  override def add(v: (K, V)): Unit = {
    map.merge(v._1, v._2, mergeFunc)
  }

  override def merge(other: AccumulatorV2[(K, V), java.util.Map[K, V]]): Unit = {
    other match {
      case o: MapAccumulator[K, V] =>
        map.synchronized {
          o.map.forEach(new BiConsumer[K, V]() {
            override def accept(k: K, v: V): Unit = {
              add((k, v))
            }
          })
        }
      case _ =>
        throw new UnsupportedOperationException(
          s"Cannot merge ${this.getClass.getName} with ${other.getClass.getName}")
    }
  }

  override def value: java.util.Map[K, V] = map.synchronized {
    java.util.Collections.unmodifiableMap[K, V](new java.util.HashMap[K, V](map))
  }
}

object MapAccumulator {
  abstract class SerializableBiFunction[T, U, R] extends BiFunction[T, U, R] with Serializable
}

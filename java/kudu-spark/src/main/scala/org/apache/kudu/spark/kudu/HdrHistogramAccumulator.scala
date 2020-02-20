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

import scala.collection.JavaConverters._

import org.apache.spark.util.AccumulatorV2
import org.HdrHistogram.IntCountsHistogram

/*
 * A Spark accumulator that aggregates values into an HDR histogram.
 *
 * This class is a wrapper for a wrapper around an HdrHistogram[1]. The purpose
 * of the double-wrapping is to work around how Spark displays accumulators in
 * its web UI. Accumulators are displayed using AccumulatorV2#value's toString
 * and not the toString method of the AccumulatorV2 (see [2]). So, to provide
 * a useful display for the histogram on the web UI, we wrap the HdrHistogram
 * in a wrapper class, implement toString on the wrapper class, and make the
 * wrapper class the value class of the Accumulator.
 *
 * [1]: https://github.com/HdrHistogram/HdrHistogram
 * [2]: https://github.com/apache/spark/blob/master/core/src/main/scala/org/apache/spark/ui/jobs/StagePage.scala#L216
 */
private[kudu] class HdrHistogramAccumulator(histogram: HistogramWrapper = new HistogramWrapper())
    extends AccumulatorV2[Int, HistogramWrapper] {

  override def isZero: Boolean = {
    histogram.isZero
  }

  override def copy(): AccumulatorV2[Int, HistogramWrapper] = {
    new HdrHistogramAccumulator(histogram.copy())
  }

  override def reset(): Unit = {
    histogram.reset()
  }

  override def add(v: Int): Unit = {
    histogram.add(v)
  }

  override def merge(other: AccumulatorV2[Int, HistogramWrapper]): Unit = {
    histogram.add(other.value)
  }

  override def value: HistogramWrapper = histogram

  override def toString: String = histogram.toString
}

/*
 * A wrapper for a IntCountsHistogram from the HdrHistogram library. See the
 * comment on the declaration of the HdrHistogramAccumulator for why this class
 * exists.
 *
 * synchronized is used because accumulators may be read from multiple threads concurrently.
 *
 * An option is used for innerHistogram so we can only initialize the histogram if it is used.
 */
private[kudu] class HistogramWrapper(var innerHistogram: Option[IntCountsHistogram] = None)
    extends Serializable {

  def isZero: Boolean = {
    innerHistogram.synchronized {
      innerHistogram.isEmpty
    }
  }

  def copy(): HistogramWrapper = {
    innerHistogram.synchronized {
      new HistogramWrapper(innerHistogram.map(_.copy()))
    }
  }

  def reset(): Unit = {
    innerHistogram.synchronized {
      if (innerHistogram.isDefined) {
        innerHistogram.get.reset()
      }
      innerHistogram = None
    }
  }

  def add(v: Int) {
    innerHistogram.synchronized {
      initializeIfEmpty()
      innerHistogram.get.recordValue(v)
    }
  }

  def add(other: HistogramWrapper) {
    innerHistogram.synchronized {
      if (other.innerHistogram.isEmpty) {
        return
      }
      initializeIfEmpty()
      innerHistogram.get.add(other.innerHistogram.get)
    }
  }

  private def initializeIfEmpty(): Unit = {
    if (innerHistogram.isEmpty) {
      innerHistogram = Some(new IntCountsHistogram(2))
    }
  }

  override def toString: String = {
    innerHistogram.synchronized {
      if (innerHistogram.isEmpty) {
        return "0ms"
      }

      if (innerHistogram.get.getTotalCount == 1) {
        return s"${innerHistogram.get.getMinValue}ms"
      }
      // The argument to SynchronizedHistogram#percentiles is the number of
      // ticks per half distance to 100%. So, a value of 1 produces values for
      // the percentiles 50, 75, 87.5, ~95, ~97.5, etc., until all histogram
      // values have been exhausted. It's a little wonky if there are very few
      // values in the histogram-- it might print out the same percentile a
      // couple of times- but it's really nice for larger histograms.
      innerHistogram.get
        .percentiles(1)
        .asScala
        .map { pv =>
          s"${pv.getPercentile}%: ${pv.getValueIteratedTo}ms"
        }
        .mkString(", ")
    }
  }
}

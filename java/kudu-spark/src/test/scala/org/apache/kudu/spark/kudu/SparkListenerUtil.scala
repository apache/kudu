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

import org.apache.kudu.test.junit.AssertHelpers
import org.apache.kudu.test.junit.AssertHelpers.BooleanExpression
import org.apache.spark.SparkContext
import org.apache.spark.scheduler.SparkListener
import org.apache.spark.scheduler.SparkListenerJobEnd
import org.apache.spark.scheduler.SparkListenerTaskEnd

object SparkListenerUtil {

  // TODO: Use org.apache.spark.TestUtils.withListener if it becomes public test API
  def withJobTaskCounter(sc: SparkContext)(body: Any => Unit): Int = {
    // Add a SparkListener to count the number of tasks that end.
    var numTasks = 0
    var jobDone = false
    val listener: SparkListener = new SparkListener {
      override def onTaskEnd(taskEnd: SparkListenerTaskEnd): Unit = {
        numTasks += 1
      }
      override def onJobEnd(jobEnd: SparkListenerJobEnd): Unit = {
        jobDone = true
      }
    }
    sc.addSparkListener(listener)
    try {
      body()
    } finally {
      // Because the SparkListener events are processed on an async queue which is behind
      // private API, we use the jobEnd event to know that all of the taskEnd events
      // must have been processed.
      AssertHelpers.assertEventuallyTrue("Spark job did not complete", new BooleanExpression {
        override def get(): Boolean = jobDone
      }, 5000)
      sc.removeSparkListener(listener)
    }
    numTasks
  }
}

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

import org.apache.kudu.test.junit.AssertHelpers
import org.apache.kudu.test.junit.AssertHelpers.BooleanExpression
import org.apache.spark.SparkContext
import org.apache.spark.scheduler.SparkListener
import org.apache.spark.scheduler.SparkListenerJobEnd
import org.apache.spark.scheduler.SparkListenerJobStart
import org.apache.spark.scheduler.SparkListenerTaskEnd

import scala.collection.mutable.ListBuffer

object SparkListenerUtil {

  def withJobTaskCounter(sc: SparkContext)(body: () => Unit): Int = {
    // Add a SparkListener to count the number of tasks that end.
    var numTasks = 0
    val listener: SparkListener = new SparkListener {
      override def onTaskEnd(taskEnd: SparkListenerTaskEnd): Unit = {
        numTasks += 1
      }
    }
    withListener(sc, listener)(body)
    numTasks
  }

  def withJobDescriptionCollector(sc: SparkContext)(body: () => Unit): List[String] = {
    // Add a SparkListener to collect the job descriptions.
    val jobDescriptions = new ListBuffer[String]()
    val listener: SparkListener = new SparkListener {
      override def onJobStart(jobStart: SparkListenerJobStart): Unit = {
        // TODO: Use SparkContext.SPARK_JOB_DESCRIPTION when public.
        val description = jobStart.properties.getProperty("spark.job.description")
        if (description != null) {
          jobDescriptions += description
        }
      }
    }
    withListener(sc, listener)(body)
    jobDescriptions.toList
  }

  // TODO: Use org.apache.spark.TestUtils.withListener if it becomes public test API
  def withListener[L <: SparkListener](sc: SparkContext, listener: L)(body: () => Unit): Unit = {
    val jobDoneListener = new JobDoneListener
    sc.addSparkListener(jobDoneListener)
    sc.addSparkListener(listener)
    try {
      body()
    } finally {
      // Because the SparkListener events are processed on an async queue which is behind
      // private API, we use the jobEnd event to know that all of the taskEnd events
      // must have been processed.
      AssertHelpers.assertEventuallyTrue("Spark job did not complete", new BooleanExpression {
        override def get(): Boolean = jobDoneListener.isDone
      }, 5000)
      sc.removeSparkListener(listener)
      sc.removeSparkListener(jobDoneListener)
    }
  }

  private class JobDoneListener extends SparkListener {
    var jobDone = false

    override def onJobEnd(jobEnd: SparkListenerJobEnd): Unit = {
      jobDone = true
    }
    def isDone = jobDone
  }
}

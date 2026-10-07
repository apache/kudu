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

import org.apache.spark.sql.execution.streaming.runtime.{MemoryStream => SparkMemoryStream}

/**
 * Spark 4.x moved the test-only MemoryStream to
 * `org.apache.spark.sql.execution.streaming.runtime`. Tests reference it through
 * this variant-specific alias (see the sibling under src/test/scala-spark3, which
 * targets the Spark 3.x `org.apache.spark.sql.execution.streaming` location).
 */
private[kudu] object MemoryStreamCompat {
  val MemoryStream = SparkMemoryStream
}

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

import org.apache.kudu.client.SessionConfiguration.FlushMode
import org.apache.kudu.mapreduce.tools.BigLinkedListCommon._
import org.apache.kudu.spark.kudu.KuduTestSuite
import org.junit.Assert._
import org.junit.Test

import scala.collection.JavaConverters._

class ITBigLinkedListTest extends KuduTestSuite {

  @Test
  def testSparkITBLL() {
    Generator.testMain(
      Array(
        "--tasks=2",
        "--lists=2",
        "--nodes=10000",
        "--hash-partitions=2",
        "--range-partitions=2",
        "--replicas=1",
        s"--master-addrs=${miniCluster.getMasterAddressesAsString}"
      ),
      ss
    )

    // Insert bad nodes in order to test the verifier:
    //
    //  (0, 0) points to an undefined node (-1, -1)
    //  (0, 1) points to (0, 0)
    //  (0, 2) points to (0, 0)
    //
    // Thus, (-1, -1) is undefined, (0, 0) is overreferenced,
    // and (0, 1) and (0, 2) are unreferenced.

    val table = kuduClient.openTable(DEFAULT_TABLE_NAME)
    val session = kuduClient.newSession()
    session.setFlushMode(FlushMode.MANUAL_FLUSH)

    for ((key1, key2, prev1, prev2) <- List((0, 0, -1, -1), (0, 1, 0, 0), (0, 2, 0, 0))) {
      val insert = table.newInsert()
      insert.getRow.addLong(COLUMN_KEY_ONE_IDX, key1)
      insert.getRow.addLong(COLUMN_KEY_TWO_IDX, key2)
      insert.getRow.addLong(COLUMN_PREV_ONE_IDX, prev1)
      insert.getRow.addLong(COLUMN_PREV_TWO_IDX, prev2)
      insert.getRow.addLong(COLUMN_ROW_ID_IDX, -1)
      insert.getRow.addString(COLUMN_CLIENT_IDX, "bad-nodes")
      insert.getRow.addInt(COLUMN_UPDATE_COUNT_IDX, 0)
      session.apply(insert)
    }

    for (response <- session.flush().asScala) {
      if (response.hasRowError) {
        // This might indicate that the generated linked lists overlapped with
        // the bad nodes, but the odds are low.
        throw new AssertionError(response.getRowError.getErrorStatus.toString)
      }
    }

    val counts = Verifier
      .testMain(Array(s"--master-addrs=${miniCluster.getMasterAddressesAsString}"), ss)
    assertEquals(2 * 2 * 10000, counts.referenced)
    assertEquals(1, counts.extrareferences)
    assertEquals(2, counts.unreferenced)
    assertEquals(1, counts.undefined)
  }
}

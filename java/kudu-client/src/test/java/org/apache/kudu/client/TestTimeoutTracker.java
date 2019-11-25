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

package org.apache.kudu.client;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.util.concurrent.atomic.AtomicLong;

import com.google.common.base.Stopwatch;
import com.google.common.base.Ticker;
import org.junit.Rule;
import org.junit.Test;

import org.apache.kudu.test.junit.RetryRule;

public class TestTimeoutTracker {

  @Rule
  public RetryRule retryRule = new RetryRule();

  @Test
  public void testTimeout() {
    final AtomicLong timeToReturn = new AtomicLong();
    Ticker ticker = new Ticker() {
      @Override
      public long read() {
        return timeToReturn.get();
      }
    };
    Stopwatch stopwatch = Stopwatch.createUnstarted(ticker);

    // no timeout set
    TimeoutTracker tracker = new TimeoutTracker(stopwatch);
    tracker.setTimeout(0);
    assertFalse(tracker.hasTimeout());
    assertFalse(tracker.timedOut());

    // 500ms timeout set
    tracker.reset();
    tracker.setTimeout(500);
    assertTrue(tracker.hasTimeout());
    assertFalse(tracker.timedOut());
    assertFalse(tracker.wouldSleepingTimeoutMillis(499));
    assertTrue(tracker.wouldSleepingTimeoutMillis(500));
    assertTrue(tracker.wouldSleepingTimeoutMillis(501));
    assertEquals(500, tracker.getMillisBeforeTimeout());

    // fast forward 200ms
    timeToReturn.set(200 * 1000000);
    assertTrue(tracker.hasTimeout());
    assertFalse(tracker.timedOut());
    assertFalse(tracker.wouldSleepingTimeoutMillis(299));
    assertTrue(tracker.wouldSleepingTimeoutMillis(300));
    assertTrue(tracker.wouldSleepingTimeoutMillis(301));
    assertEquals(300, tracker.getMillisBeforeTimeout());

    // fast forward another 400ms, so the RPC timed out
    timeToReturn.set(600 * 1000000);
    assertTrue(tracker.hasTimeout());
    assertTrue(tracker.timedOut());
    assertTrue(tracker.wouldSleepingTimeoutMillis(299));
    assertTrue(tracker.wouldSleepingTimeoutMillis(300));
    assertTrue(tracker.wouldSleepingTimeoutMillis(301));
    assertEquals(1, tracker.getMillisBeforeTimeout());
  }
}

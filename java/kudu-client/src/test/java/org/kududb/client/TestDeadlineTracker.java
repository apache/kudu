// Copyright (c) 2014, Cloudera, inc.
// Confidential Cloudera Information: Covered by NDA.
package org.kududb.client;

import static org.junit.Assert.*;

import com.google.common.base.Stopwatch;
import com.google.common.base.Ticker;
import org.junit.Test;

import java.util.concurrent.atomic.AtomicLong;

public class TestDeadlineTracker {

  @Test
  public void testTimeout() {
    final AtomicLong timeToReturn = new AtomicLong();
    Ticker ticker = new Ticker() {
      @Override
      public long read() {
        return timeToReturn.get();
      }
    };
    Stopwatch stopwatch = new Stopwatch(ticker);

    // no timeout set
    DeadlineTracker tracker = new DeadlineTracker(stopwatch);
    tracker.setDeadline(0);
    assertFalse(tracker.hasDeadline());
    assertFalse(tracker.timedOut());

    // 500ms timeout set
    tracker.reset();
    tracker.setDeadline(500);
    assertTrue(tracker.hasDeadline());
    assertFalse(tracker.timedOut());
    assertFalse(tracker.wouldSleepingTimeout(499));
    assertTrue(tracker.wouldSleepingTimeout(500));
    assertTrue(tracker.wouldSleepingTimeout(501));
    assertEquals(500, tracker.getMillisBeforeDeadline());

    // fast forward 200ms
    timeToReturn.set(200 * 1000000);
    assertTrue(tracker.hasDeadline());
    assertFalse(tracker.timedOut());
    assertFalse(tracker.wouldSleepingTimeout(299));
    assertTrue(tracker.wouldSleepingTimeout(300));
    assertTrue(tracker.wouldSleepingTimeout(301));
    assertEquals(300, tracker.getMillisBeforeDeadline());

    // fast forward another 400ms, so the RPC timed out
    timeToReturn.set(600 * 1000000);
    assertTrue(tracker.hasDeadline());
    assertTrue(tracker.timedOut());
    assertTrue(tracker.wouldSleepingTimeout(299));
    assertTrue(tracker.wouldSleepingTimeout(300));
    assertTrue(tracker.wouldSleepingTimeout(301));
    assertEquals(1, tracker.getMillisBeforeDeadline());
  }
}

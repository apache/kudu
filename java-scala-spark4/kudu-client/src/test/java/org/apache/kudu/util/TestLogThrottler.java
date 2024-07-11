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

package org.apache.kudu.util;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.io.Closeable;

import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.kudu.test.CapturingLogAppender;

/**
 * Test for {@link LogThrottler}. Tries logging eight similar messages but only one should be logged
 * every second according to the parameter so only first message will be logged. Rest of messages
 * until thread is paused should be suppressed. Thread is paused for two seconds after fourth
 * message, the subsequent message should be logged then the remaining logs should be suppressed.
 */
public class TestLogThrottler {

  @Test
  public void test() throws Exception {
    Logger log = LoggerFactory.getLogger(TestLogThrottler.class);
    LogThrottler logThrottler = new LogThrottler(log);
    CapturingLogAppender messageChecker = new CapturingLogAppender();
    try (Closeable c = messageChecker.attach()) {
      for (int i = 0; i < 8; i++) {
        logThrottler.info(1L,"Logging {}", i);
        if (i == 3) {
          Thread.sleep(2000);
        }
      }
    }
    String output = messageChecker.getAppendedText();
    assertTrue("Log doesn't contain Logging 0", output.contains("Logging 0"));
    for (int i = 1; i <= 3; i++) {
      assertFalse("Log contains Logging " + i, output.contains("Logging " + i));
    }
    assertTrue("Log doesn't contain Logging 4", output.contains("Logging 4"));
    for (int i = 5; i <= 7; i++) {
      assertFalse("Log contains Logging " + i, output.contains("Logging " + i));
    }
  }
}

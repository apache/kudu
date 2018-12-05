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
package org.apache.kudu.test.junit;

import org.junit.Rule;
import org.junit.Test;

import static org.junit.Assert.fail;

public class TestRetryRule {
  private static final int MAX_FAILURES = 2;

  // Track the number of times testRetry() failed, and was re-run, by the RetryRule so that we
  // can eventually cause it to succeed. Each failure increments this variable before it throws
  // an assertion exception.
  private int failures = 0;

  // We skip flaky test reporting for this test because it is designed to fail.
  @Rule
  public RetryRule retryRule = new RetryRule(MAX_FAILURES, /*skipReporting=*/ true);

  // Ensure that the RetryRule prevents test failures as long as we don't exceed MAX_FAILURES
  // failures.
  @Test
  public void testRetry() {
    if (failures < MAX_FAILURES) {
      failures++;
      fail(String.format("%d failures", failures));
    }
    // Pass the test (by not throwing) on the final retry.
  }

}

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

import com.google.common.base.Preconditions;

import org.apache.kudu.test.CapturingToFileLogAppender;
import org.apache.yetus.audience.InterfaceAudience;
import org.apache.yetus.audience.InterfaceStability;
import org.junit.rules.TestRule;
import org.junit.runner.Description;
import org.junit.runners.model.Statement;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.io.BufferedReader;
import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.HashSet;
import java.util.Set;

import static java.nio.charset.StandardCharsets.UTF_8;

/**
 * JUnit rule to retry failed tests.
 *
 * Uses the KUDU_FLAKY_TEST_LIST and KUDU_RETRY_ALL_FAILED_TESTS environment
 * variables to determine whether a test should be retried, and the
 * KUDU_FLAKY_TEST_ATTEMPTS environment variable to determine how many times.
 *
 * By default will use ResultReporter to report success/failure of each test
 * attempt to an external server; this may be skipped if desired.
 */
@InterfaceAudience.Private
@InterfaceStability.Unstable
public class RetryRule implements TestRule {
  private static final Logger LOG = LoggerFactory.getLogger(RetryRule.class);
  private static final int DEFAULT_RETRY_COUNT = 0;
  private static final Set<String> FLAKY_TESTS = new HashSet<>();

  private final int retryCount;
  private final ResultReporter reporter;

  static {
    // Initialize the flaky test set if it exists. The file will have one test
    // name per line.
    String value = System.getenv("KUDU_FLAKY_TEST_LIST");
    if (value != null) {
      try (BufferedReader br = Files.newBufferedReader(Paths.get(value), UTF_8)) {
        for (String l = br.readLine(); l != null; l = br.readLine()) {
          FLAKY_TESTS.add(l);
        }
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    }
  }

  public RetryRule() {
    this(DEFAULT_RETRY_COUNT, /*skipReporting=*/ false);
  }

  @InterfaceAudience.LimitedPrivate("Test")
  RetryRule(int retryCount, boolean skipReporting) {
    Preconditions.checkArgument(retryCount >= 0);
    this.retryCount = retryCount;
    this.reporter = skipReporting ? null : new ResultReporter();
  }

  private static boolean retryAllTests() {
    String value = System.getenv("KUDU_RETRY_ALL_FAILED_TESTS");
    return value != null && !value.isEmpty();
  }

  private static boolean retryThisTest(String humanReadableTestName) {
    return FLAKY_TESTS.contains(humanReadableTestName);
  }

  private static int getActualRetryCount() {
    String value = System.getenv("KUDU_FLAKY_TEST_ATTEMPTS");
    if (value == null) {
      return DEFAULT_RETRY_COUNT;
    }
    try {
      int val = Integer.parseInt(value);
      if (val < 1) {
        throw new NumberFormatException(
            String.format("expected non-zero positive value, got %d", val));
      }

      // Convert from number of "attempts" to number of "retries".
      return Integer.parseInt(value) - 1;
    } catch (NumberFormatException e) {
      LOG.warn("Could not parse KUDU_FLAKY_TEST_ATTEMPTS, using default value ({})",
               DEFAULT_RETRY_COUNT, e);
      return DEFAULT_RETRY_COUNT;
    }
  }

  @Override
  public Statement apply(Statement base, Description description) {
    String humanReadableTestName =
        description.getClassName() + "." + description.getMethodName();

    // Retrying and reporting are independent; the RetryStatement is used if
    // either is enabled. We'll retry the test under one of the following
    // circumstances:
    //
    // 1. The RetryRule was constructed with an explicit retry count.
    // 2. We've been asked to retry all tests via KUDU_RETRY_ALL_FAILED_TESTS.
    // 3. We've been asked to retry this test via KUDU_FLAKY_TEST_LIST.
    //
    // In the latter two cases, we consult KUDU_FLAKY_TEST_ATTEMPTS for the retry count.
    boolean retryExplicit = retryCount != DEFAULT_RETRY_COUNT;
    boolean retryAll = retryAllTests();
    boolean retryThis = retryThisTest(humanReadableTestName);
    if (retryExplicit || retryAll || retryThis || reporter != null) {
      int actualRetryCount = (retryAll || retryThis) ? getActualRetryCount() : retryCount;
      LOG.info("Creating RetryStatement {} result reporter and retry count of {} ({})",
               reporter != null ? "with" : "without",
               actualRetryCount,
               retryExplicit ? "explicit" :
                 retryAll ? "all tests" :
                   retryThis ? "this test" : "no retries");
      return new RetryStatement(base, actualRetryCount, reporter, humanReadableTestName);
    }
    return base;
  }

  private static class RetryStatement extends Statement {

    private final Statement base;
    private final int retryCount;
    private final ResultReporter reporter;
    private final String humanReadableTestName;

    RetryStatement(Statement base, int retryCount, ResultReporter reporter,
                   String humanReadableTestName) {
      this.base = base;
      this.retryCount = retryCount;
      this.reporter = reporter;
      this.humanReadableTestName = humanReadableTestName;
    }

    private void report(ResultReporter.Result result, File logFile) {
      reporter.tryReportResult(humanReadableTestName, result, logFile);
    }

    private void doOneAttemptAndReport(int attempt) throws Throwable {
      try (CapturingToFileLogAppender capturer =
           new CapturingToFileLogAppender(/*useGzip=*/ true)) {
        try {
          try (Closeable c = capturer.attach()) {
            base.evaluate();
          }

          // The test succeeded.
          //
          // We skip the file upload; this saves space and network bandwidth,
          // and we don't need the logs of successful tests.
          report(ResultReporter.Result.SUCCESS, /*logFile=*/ null);
          return;
        } catch (Throwable t) {
          // The test failed.
          //
          // Before reporting, capture the failing exception too.
          try (Closeable c = capturer.attach()) {
            LOG.error("{}: failed attempt {}", humanReadableTestName, attempt, t);
          }
          capturer.finish();
          report(ResultReporter.Result.FAILURE, capturer.getOutputFile());
          throw t;
        }
      }
    }

    private void doOneAttempt(int attempt) throws Throwable {
      try {
        base.evaluate();
      } catch (Throwable t) {
        LOG.error("{}: failed attempt {}", humanReadableTestName, attempt, t);
        throw t;
      }
    }

    @Override
    public void evaluate() throws Throwable {
      Throwable lastException;
      int attempt = 0;
      do {
        attempt++;
        try {
          if (reporter != null && reporter.isReportingEnabled()) {
            doOneAttemptAndReport(attempt);
          } else {
            doOneAttempt(attempt);
          }
          return;
        } catch (Throwable t) {
          lastException = t;
        }
      } while (attempt <= retryCount);
      LOG.error("{}: giving up after {} attempts", humanReadableTestName, attempt);
      throw lastException;
    }
  }
}

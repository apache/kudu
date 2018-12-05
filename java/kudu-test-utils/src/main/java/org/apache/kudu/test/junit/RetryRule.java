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

import org.apache.kudu.test.CapturingToFileLogAppender;
import org.apache.yetus.audience.InterfaceAudience;
import org.apache.yetus.audience.InterfaceStability;
import org.junit.rules.TestRule;
import org.junit.runner.Description;
import org.junit.runners.model.Statement;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.io.Closeable;
import java.io.File;
import java.io.IOException;

/**
 * JUnit rule to retry failed tests.
 *
 * The number of retries is controlled by the "rerunFailingTestsCount" system
 * property, mimicking Surefire in that regard.
 *
 * By default will use ResultReporter to report success/failure of each test
 * attempt to an external server; this may be skipped if desired.
 */
@InterfaceAudience.Private
@InterfaceStability.Unstable
public class RetryRule implements TestRule {

  private static final String RETRY_PROP = "rerunFailingTestsCount";

  private static final Logger LOG = LoggerFactory.getLogger(RetryRule.class);

  private final int retryCount;
  private final ResultReporter reporter;

  public RetryRule() {
    this(Integer.getInteger(RETRY_PROP, 0), /*skipReporting=*/ false);
  }

  @InterfaceAudience.LimitedPrivate("Test")
  RetryRule(int retryCount, boolean skipReporting) {
    this.retryCount = retryCount;
    this.reporter = skipReporting ? null : new ResultReporter();
  }

  @Override
  public Statement apply(Statement base, Description description) {
    return new RetryStatement(base, description, retryCount, reporter);
  }

  private static class RetryStatement extends Statement {

    private final Statement base;
    private final Description description;
    private final int retryCount;
    private final ResultReporter reporter;
    private final String humanReadableTestName;

    RetryStatement(Statement base, Description description,
                   int retryCount, ResultReporter reporter) {
      this.base = base;
      this.description = description;
      this.retryCount = retryCount;
      this.reporter = reporter;
      this.humanReadableTestName = description.getClassName() + "." + description.getMethodName();
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

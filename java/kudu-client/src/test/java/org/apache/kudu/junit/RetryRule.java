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
package org.apache.kudu.junit;

import org.junit.rules.TestRule;
import org.junit.runner.Description;
import org.junit.runners.model.Statement;

/**
 * A JUnit rule to retry failed tests.
 * We use this with Gradle because it doesn't support
 * Surefire/Failsafe rerunFailingTestsCount like Maven does. We use the system
 * property rerunFailingTestsCount to mimic the maven arguments closely.
 */
public class RetryRule implements TestRule {

  private static final int RETRY_COUNT = Integer.getInteger("rerunFailingTestsCount", 0);

  public RetryRule () {}

  @Override
  public Statement apply(Statement base, Description description) {
    return new RetryStatement(base, description, RETRY_COUNT);
  }

  private static class RetryStatement extends Statement {

    private final Statement base;
    private final Description description;
    private final int retryCount;

    RetryStatement(final Statement base, final Description description, final int retryCount) {
      this.base = base;
      this.description = description;
      this.retryCount = retryCount;
    }

    @Override
    public void evaluate() throws Throwable {
      // If there are no retries, just pass through to evaluate as usual.
      if (retryCount == 0) {
        base.evaluate();
        return;
      }

      // To retry we catch the exception for the evaluate, log a message, and retry.
      // We track and throw the last failure if all tries fail.
      Throwable lastException = null;
      for (int i = 0; i < retryCount; i++) {
        try {
          base.evaluate();
          return;
        } catch (Throwable t) {
          lastException = t;
          System.err.println(description.getDisplayName() + ": run " + (i + 1) + " failed.");
        }
      }
      System.err.println(description.getDisplayName() + ": giving up after " + retryCount + " failures.");
      throw lastException;
    }
  }
}

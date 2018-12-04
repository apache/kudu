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

import org.apache.yetus.audience.InterfaceAudience;
import org.apache.yetus.audience.InterfaceStability;
import org.junit.rules.TestRule;
import org.junit.runner.Description;
import org.junit.runners.model.Statement;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A JUnit rule to retry failed tests.
 * We use this with Gradle because it doesn't support
 * Surefire/Failsafe rerunFailingTestsCount like Maven does. We use the system
 * property rerunFailingTestsCount to mimic the maven arguments closely.
 */
@InterfaceAudience.Private
@InterfaceStability.Unstable
public class RetryRule implements TestRule {

  private static final Logger LOG = LoggerFactory.getLogger(RetryRule.class);
  private final int retryCount;

  public RetryRule() {
    this(Integer.getInteger("rerunFailingTestsCount", 0));
  }

  // Visible for testing.
  RetryRule(int retryCount) {
    this.retryCount = retryCount;
  }

  @Override
  public Statement apply(Statement base, Description description) {
    return new RetryStatement(base, description, retryCount);
  }

  private static class RetryStatement extends Statement {

    private final Statement base;
    private final Description description;
    private final int retryCount;

    RetryStatement(Statement base, Description description, int retryCount) {
      this.base = base;
      this.description = description;
      this.retryCount = retryCount;
    }

    @Override
    public void evaluate() throws Throwable {
      Throwable lastException;
      int attempt = 0;
      do {
        attempt++;
        try {
          base.evaluate();
          return;

        } catch (Throwable t) {
          // To retry, we catch the exception from evaluate(), log an error, and loop.
          // We retain and rethrow the last failure if all attempts fail.
          lastException = t;
          LOG.error("{}: failed attempt {}", description.getDisplayName(), attempt, t);
        }
      } while (attempt <= retryCount);
      LOG.error("{}: giving up after {} attempts", description.getDisplayName(), attempt);
      throw lastException;
    }
  }
}

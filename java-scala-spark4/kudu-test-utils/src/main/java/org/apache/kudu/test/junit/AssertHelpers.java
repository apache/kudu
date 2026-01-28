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

import static org.junit.Assert.assertTrue;

import org.apache.yetus.audience.InterfaceAudience;
import org.apache.yetus.audience.InterfaceStability;

@InterfaceAudience.Private
@InterfaceStability.Unstable
public class AssertHelpers {
  public interface BooleanExpression {
    boolean get() throws Exception;
  }

  // A looping check. It's mainly useful for scanners, since writes may take a little time to show
  // up.
  public static void assertEventuallyTrue(String description, BooleanExpression expression,
                                          long timeoutMillis) throws Exception {
    long deadlineNanos = System.nanoTime() + timeoutMillis * 1000000;
    boolean success;

    do {
      success = expression.get();
      if (success) {
        break;
      }
      Thread.sleep(50); // Sleep for 50ms
    } while (System.nanoTime() < deadlineNanos);

    assertTrue(description, success);
  }
}

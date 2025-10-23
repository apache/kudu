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

import org.apache.yetus.audience.InterfaceAudience;

/**
 * Exception indicating that an operation attempted to access a non-covered range partition.
 */
@InterfaceAudience.Private
public class NonCoveredRangeException extends NonRecoverableException {
  private final byte[] nonCoveredRangeStart;
  private final byte[] nonCoveredRangeEnd;

  public NonCoveredRangeException(byte[] nonCoveredRangeStart, byte[] nonCoveredRangeEnd) {
    super(Status.NotFound(getMessage(nonCoveredRangeStart, nonCoveredRangeEnd)));
    this.nonCoveredRangeStart = nonCoveredRangeStart;
    this.nonCoveredRangeEnd = nonCoveredRangeEnd;
  }

  private static String getMessage(byte[] rangeStart, byte[] rangeEnd) {
    return String.format("accessed range partition ([%s, %s)) does not exist in table",
            rangeStart.length == 0 ? "<start>" : Bytes.hex(rangeStart),
            rangeEnd.length == 0 ? "<end>" : Bytes.hex(rangeEnd));
  }

  @Override
  public String getMessage() {
    return getMessage(nonCoveredRangeStart, nonCoveredRangeEnd);
  }

  byte[] getNonCoveredRangeStart() {
    return nonCoveredRangeStart;
  }

  byte[] getNonCoveredRangeEnd() {
    return nonCoveredRangeEnd;
  }
}

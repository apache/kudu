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
import org.apache.yetus.audience.InterfaceStability;

/**
 * Container class used as a response when retrieving pending row errors.
 */
@InterfaceAudience.Public
@InterfaceStability.Evolving
public class RowErrorsAndOverflowStatus {
  private final RowError[] rowErrors;
  private final boolean overflowed;

  RowErrorsAndOverflowStatus(RowError[] rowErrors, boolean overflowed) {
    this.rowErrors = rowErrors;
    this.overflowed = overflowed;
  }

  /**
   * Get the collected row errors.
   * @return an array of row errors, may be empty
   */
  public RowError[] getRowErrors() {
    return rowErrors;
  }

  /**
   * Check if the error collector had an overflow and had to discard row errors.
   * @return true if row errors were discarded, false otherwise
   */
  public boolean isOverflowed() {
    return overflowed;
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append("rowErrors size: ").append(rowErrors.length);
    sb.append(", rowErrors: [");
    if (rowErrors.length > 0) {
      sb.append(rowErrors[0].toString());
      for (int i = 1; i < rowErrors.length; i++) {
        sb.append(", ").append(rowErrors[i].toString());
      }
    }
    sb.append("], overflowed: ").append(overflowed);
    return sb.toString();
  }
}

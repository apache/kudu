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

import org.apache.yetus.audience.InterfaceAudience;

import org.apache.kudu.ColumnTypeAttributes;

@InterfaceAudience.Private
public class CharUtil {
  public static final int MIN_VARCHAR_LENGTH = 1;
  public static final int MAX_VARCHAR_LENGTH = 65535;

  /** Non-constructable utility class. */
  private CharUtil() {
  }

  /**
   * Convenience method to create column type attributes for VARCHAR columns.
   * @param length the length.
   * @return the column type attributes.
   */
  public static ColumnTypeAttributes typeAttributes(int length) {
    return new ColumnTypeAttributes.ColumnTypeAttributesBuilder()
      .length(length)
      .build();
  }
}

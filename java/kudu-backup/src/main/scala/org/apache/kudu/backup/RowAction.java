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
package org.apache.kudu.backup;

import com.google.common.collect.ImmutableMap;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;

import java.util.Map;

/**
 * A RowAction is used to represent the action associated with a backed up row.
 *
 * Currently UPSERT is the default action, while rows with the IS_DELETED virtual
 * column have an action of DELETE. This value is serialized as a byte in the
 * output data format to be as space efficient as possible.
 *
 * Given there are currently only 2 options, IS_DELETED or not, we could have used an
 * IS_DELETED boolean column in the output format, but this RowAction allows for greater
 * format flexibility to support INSERT or UPDATE in the future if we have full fidelity
 * and sparse row backups.
 *
 * See {@link RowIterator} for backup side usage and {@link KuduRestore} for restore
 * side usage.
 */
@InterfaceAudience.Private
@InterfaceStability.Unstable
public enum RowAction {

  UPSERT((byte) 0),
  DELETE((byte) 1);

  /** The byte value used to represent this RowAction */
  private final byte value;

  private static Map<Byte, RowAction> byteRowAction;
  static {
    byteRowAction = new ImmutableMap.Builder<Byte, RowAction>()
        .put(UPSERT.getValue(), UPSERT)
        .put(DELETE.getValue(), DELETE)
        .build();
  }

  RowAction(byte value) {
    this.value = value;
  }

  public byte getValue() {
    return value;
  }

  public static RowAction fromValue(Byte value) {
    return byteRowAction.get(value);
  }
}

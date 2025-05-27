// Licensed to the Apache Software Foundation (ASF) under one or more
// contributor license agreements.  See the NOTICE file distributed with
// this work for additional information regarding copyright ownership.
// The ASF licenses this file to You under the Apache License, Version 2.0
// (the "License"); you may not use this file except in compliance with
// the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package org.apache.kudu.replication;

import java.util.Collections;
import java.util.List;

import org.apache.flink.connector.kudu.connector.writer.KuduOperationMapper;
import org.apache.flink.types.Row;
import org.apache.flink.types.RowKind;

import org.apache.kudu.client.KuduTable;
import org.apache.kudu.client.Operation;
import org.apache.kudu.client.PartialRow;

/**
 * A custom implementation of {@link KuduOperationMapper} that maps a Flink {@link Row}
 * to the appropriate Kudu {@link Operation} based on the row's kind.
 * <p>
 * If the row has kind {@link RowKind#DELETE}, a Kudu {@code DeleteIgnore} operation is created.
 * Otherwise, a {@code UpsertIgnore} operation is used by default.
 * </p>
 * <p>
 * All fields in the Flink row are written into the Kudu {@link PartialRow}.
 * </p>
 */
public class CustomReplicationOperationMapper implements KuduOperationMapper<Row> {
  private static final long serialVersionUID = 3364944402413430711L;

  public Operation createBaseOperation(Row input, KuduTable table) {
    RowKind kind = input.getKind();
    if (kind == RowKind.DELETE) {
      return table.newDeleteIgnore();
    } else {
      return table.newUpsertIgnore();
    }
  }

  @Override
  public List<Operation> createOperations(Row input, KuduTable table) {
    Operation operation = createBaseOperation(input, table);
    PartialRow partialRow = operation.getRow();
    for (int i = 0; i < input.getArity(); i++) {
      partialRow.addObject(i, input.getField(i));
    }
    return Collections.singletonList(operation);
  }
}

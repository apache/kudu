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

/*
 * TODO(mgreber): remove this file once FLINK-37473 is resolved.
 *
 * This class should live in the official Flink Kudu source connector.
 * Until then, it's included here as a temporary workaround in the replication job source.
 */

package org.apache.kudu.replication;

import org.apache.flink.connector.kudu.connector.converter.RowResultConverter;
import org.apache.flink.types.Row;
import org.apache.flink.types.RowKind;

import org.apache.kudu.Schema;
import org.apache.kudu.client.RowResult;

/**
 * A custom implementation of {@link RowResultConverter} that converts a Kudu {@link RowResult}
 * into a Flink {@link Row}.
 */
public class CustomReplicationRowRestultConverter implements RowResultConverter<Row> {
  private static final long serialVersionUID = -2668642733689083069L;

  public CustomReplicationRowRestultConverter() {
  }

  /**
   * If the Kudu schema has a delete marker column (as indicated by {@link Schema#hasIsDeleted()}),
   * and the row is marked as deleted, the resulting Flink row's kind is set to
   * {@link RowKind#DELETE}.
   *
   * The special delete marker column (accessible via {@link Schema#getIsDeletedIndex()}) is
   * excluded from the Flink row’s fields. All other non-null fields are copied into the Flink
   * {@code Row}.
   *
   * @param row the Kudu {@code RowResult} to convert
   * @return a Flink {@code Row} representation, with deletion semantics if applicable
   */
  @Override
  public Row convert(RowResult row) {
    Schema schema = row.getColumnProjection();
    int columnCount = schema.hasIsDeleted() ? schema.getColumnCount() - 1 : schema.getColumnCount();
    Row values = new Row(columnCount);

    int isDeletedIndex = schema.hasIsDeleted() ? schema.getIsDeletedIndex() : -1;

    if (row.hasIsDeleted() && row.isDeleted()) {
      values.setKind(RowKind.DELETE);
    }
    schema.getColumns().forEach(column -> {
      int pos = schema.getColumnIndex(column.getName());
      // Skip the isDeleted column if it exists
      if (pos == isDeletedIndex && row.hasIsDeleted()) {
        return;
      }
      if (!row.isNull(pos)) {
        values.setField(pos, row.getObject(pos));
      }
    });

    return values;
  }
}

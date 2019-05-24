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

import java.util.ArrayList;
import java.util.List;

import com.google.common.base.MoreObjects;
import com.google.common.base.Objects;
import org.apache.yetus.audience.InterfaceAudience;
import org.apache.yetus.audience.InterfaceStability;

@InterfaceAudience.Public
@InterfaceStability.Evolving
public class ListTablesResponse extends KuduRpcResponse {

  private final List<TableInfo> tableInfosList;
  private final List<String> tablesList;

  ListTablesResponse(long elapsedMillis, String tsUUID, List<TableInfo> tableInfosList) {
    super(elapsedMillis, tsUUID);
    List<String> tablesList = new ArrayList<>();
    for (TableInfo info : tableInfosList) {
      tablesList.add(info.getTableName());
    }
    this.tableInfosList = tableInfosList;
    this.tablesList = tablesList;
  }

  /**
   * Get the list of tables as specified in the request.
   * @return a list of table names
   */
  public List<String> getTablesList() {
    return tablesList;
  }

  /**
   * Get the list of tables as specified in the request.
   * @return a list of TableInfo
   */
  public List<TableInfo> getTableInfosList() {
    return tableInfosList;
  }

  public static class TableInfo {
    private final String tableId;
    private final String tableName;

    TableInfo(String tableId, String tableName) {
      this.tableId = tableId;
      this.tableName = tableName;
    }

    /**
     * @return the table id
     */
    public String getTableId() {
      return tableId;
    }

    /**
     * @return the table name
     */
    public String getTableName() {
      return tableName;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) return true;
      if (!(o instanceof TableInfo)) return false;
      TableInfo tableInfo = (TableInfo) o;
      return Objects.equal(tableId, tableInfo.tableId) &&
          Objects.equal(tableName, tableInfo.tableName);
    }

    @Override
    public int hashCode() {
      return Objects.hashCode(tableId, tableName);
    }

    @Override
    public String toString() {
      return MoreObjects.toStringHelper(this)
          .add("tableId", tableId)
          .add("tableName", tableName)
          .toString();
    }
  }
}

// Copyright (c) 2014, Cloudera, inc.
package kudu.rpc;

import java.util.List;

public class ListTablesResponse extends KuduRpcResponse {

  private final List<String> tablesList;

  ListTablesResponse(long ellapsedMillis, List<String> tablesList) {
    super(ellapsedMillis);
    this.tablesList = tablesList;
  }

  /**
   * Get the list of tables as specified in the request.
   * @return a list of table names
   */
  public List<String> getTablesList() {
    return tablesList;
  }
}

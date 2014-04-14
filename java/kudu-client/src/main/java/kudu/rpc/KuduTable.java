// Copyright (c) 2013, Cloudera, inc.
package kudu.rpc;

import kudu.Schema;
import kudu.util.Slice;

/**
 * A KuduTable represents a table on a particular cluster. It holds the current
 * schema of the table. Any given KuduTable instance belongs to a specific KuduClient
 * instance.
 *
 * Upon construction, the table is looked up in the catalog (or catalog cache),
 * and the schema fetched for introspection. TODO
 *
 * This class is thread-safe.
 */
public class KuduTable {

  private final Schema schema;
  private final KuduClient client;
  private final String name;
  private final Slice nameAsSlice;

  public KuduTable(KuduClient client, String name, Schema schema) {
    this.schema = schema;
    this.client = client;
    this.name = name;
    this.nameAsSlice = new Slice(this.name.getBytes());
  }

  public Schema getSchema() {
    return this.schema;
  }

  public String getName() {
    return this.name;
  }

  public KuduClient getClient() {
    return this.client;
  }

  public Slice getNameAsSlice() {
    return this.nameAsSlice;
  }

  public Insert newInsert() {
    return new Insert(this);
  }

  public Update newUpdate() {
    return new Update(this);
  }

  public Delete newDelete() {
    return new Delete(this);
  }


}

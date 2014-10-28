// Copyright (c) 2013, Cloudera, inc.
// Confidential Cloudera Information: Covered by NDA.
package kudu;

import java.util.ArrayList;
import static kudu.Type.INT32;
import static kudu.Type.STRING;

public class Tpch1Schema {

  public static Schema getTpch1Schema() {
    ArrayList<ColumnSchema> columns = new ArrayList<ColumnSchema>(16);
    columns.add(new ColumnSchema("l_orderkey", INT32, true));
    columns.add(new ColumnSchema("l_linenumber", INT32, true));
    columns.add(new ColumnSchema("l_partkey", INT32));
    columns.add(new ColumnSchema("l_suppkey", INT32));
    columns.add(new ColumnSchema("l_quantity", INT32));
    columns.add(new ColumnSchema("l_extendedprice", INT32));
    columns.add(new ColumnSchema("l_discount", INT32));
    columns.add(new ColumnSchema("l_tax", INT32));
    columns.add(new ColumnSchema("l_returnflag", STRING));
    columns.add(new ColumnSchema("l_linestatus", STRING));
    columns.add(new ColumnSchema("l_shipdate", STRING));
    columns.add(new ColumnSchema("l_commitdate", STRING));
    columns.add(new ColumnSchema("l_receiptdate", STRING));
    columns.add(new ColumnSchema("l_shipinstruct", STRING));
    columns.add(new ColumnSchema("l_shipmode", STRING));
    columns.add(new ColumnSchema("l_comment", STRING));
    return new Schema(columns);
  }
}

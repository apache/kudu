// Copyright (c) 2014, Cloudera, inc.
// Confidential Cloudera Information: Covered by NDA.
package kudu.mapreduce;

import kudu.rpc.Operation;
import org.apache.hadoop.mapreduce.Reducer;

public abstract class TableReducer<KEYIN, VALUEIN, KEYOUT>
    extends Reducer<KEYIN, VALUEIN, KEYOUT, Operation> {
}

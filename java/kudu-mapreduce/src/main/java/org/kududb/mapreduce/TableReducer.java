// Copyright (c) 2014, Cloudera, inc.
// Confidential Cloudera Information: Covered by NDA.
package org.kududb.mapreduce;

import org.kududb.annotations.InterfaceAudience;
import org.kududb.annotations.InterfaceStability;
import org.kududb.client.Operation;
import org.apache.hadoop.mapreduce.Reducer;

@InterfaceAudience.Public
@InterfaceStability.Evolving
public abstract class TableReducer<KEYIN, VALUEIN, KEYOUT>
    extends Reducer<KEYIN, VALUEIN, KEYOUT, Operation> {
}

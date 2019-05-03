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

package org.apache.kudu.hive.serde.input;

import java.util.Iterator;
import org.apache.kudu.client.KuduClient;
import org.apache.kudu.client.KuduException;
import org.apache.kudu.client.KuduScanToken;
import org.apache.kudu.client.KuduScanner;
import org.apache.kudu.hive.serde.HiveKuduBridgeUtils;
import org.apache.kudu.hive.serde.PartialRowWritable;
import java.io.IOException;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reporter;
import org.apache.kudu.client.RowResult;

public class KuduRecordReader implements RecordReader<NullWritable, PartialRowWritable> {


  private long splitLen; // for getPos()

  private boolean firstRecord = false;
  private boolean eof = false;
  private KuduScanner scanner;
  private Iterator<RowResult> iterator;
  private KuduClient client;

  KuduRecordReader(InputSplit oldSplit, JobConf jobConf) throws IOException {

    this.client = HiveKuduBridgeUtils.getKuduClient(jobConf);
    splitLen = oldSplit.getLength();
    KuduTableSplit split = (KuduTableSplit) oldSplit;
    scanner = KuduScanToken.deserializeIntoScanner(split.getScanToken(), client);
    iterator = scanner.iterator();
  }

  @Override
  public boolean next(NullWritable key, PartialRowWritable value) throws IOException {
    if (eof) {
      return false;
    }

    if (firstRecord) { // key & value are already read.
      firstRecord = false;
      return true;
    }

    if (iterator.hasNext()) {
      value.setRow(iterator.next());
      return true;
    }

    eof = true; // strictly not required, just for consistency
    return false;
  }


  @Override
  public NullWritable createKey() {
    return NullWritable.get();
  }

  @Override
  public PartialRowWritable createValue() {
    PartialRowWritable value = null;
    try {
      if (!firstRecord && !eof) {
        if (iterator.hasNext()) {
          firstRecord = true;
          value = new PartialRowWritable(iterator.next());
        } else {
          eof = true;
        }
      }
    } catch (IOException e) {
      throw new RuntimeException("Could not read first record (and it was not an EOF)", e);
    }
    return value;
  }

  @Override
  public long getPos() throws IOException {
    return (long) (splitLen * getProgress());
  }

  @Override
  public void close() throws IOException {
    try {
      scanner.close();
    } catch (KuduException e) {
      throw new IOException(e);
    }
    client.shutdown();
  }

  @Override
  public float getProgress() throws IOException {
    // TODO Guesstimate progress
    return 0;
  }
}

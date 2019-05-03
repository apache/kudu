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

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.kudu.client.Bytes;
import org.apache.kudu.client.KuduScanToken;

public class KuduTableSplit implements InputSplit, Configurable {

  private Configuration conf;

  /** The scan token that the split will use to scan the Kudu table. */
  private byte[] scanToken;

  /** The start partition key of the scan. Used for sorting splits. */
  private byte[] partitionKey;

  /** Tablet server locations which host the tablet to be scanned. */
  private String[] locations;

  @SuppressWarnings("unused") // MapReduce instantiates this.
  public KuduTableSplit() {
  }

  KuduTableSplit(KuduScanToken token, String[] locations) throws IOException {
    this.scanToken = token.serialize();
    this.partitionKey = token.getTablet().getPartition().getPartitionKeyStart();
    this.locations = locations;
  }

  public byte[] getScanToken() {
    return scanToken;
  }


  @Override
  public long getLength() throws IOException {
    // TODO Guesstimate a size
    return 0;
  }

  @Override
  public String[] getLocations() throws IOException {
    return locations;
  }

  @Override
  public void readFields(DataInput dataInput) throws IOException {
    scanToken = Bytes.readByteArray(dataInput);
    partitionKey = Bytes.readByteArray(dataInput);
    locations = new String[dataInput.readInt()];
    for (int i = 0; i < locations.length; i++) {
      byte[] str = Bytes.readByteArray(dataInput);
      locations[i] = Bytes.getString(str);
    }
  }

  @Override
  public void write(DataOutput dataOutput) throws IOException {
    Bytes.writeByteArray(dataOutput, scanToken);
    Bytes.writeByteArray(dataOutput, partitionKey);
    dataOutput.writeInt(locations.length);
    for (String location : locations) {
      byte[] str = Bytes.fromString(location);
      Bytes.writeByteArray(dataOutput, str);
    }
  }

  @Override
  public Configuration getConf() {
    return conf;
  }

  @Override
  public void setConf(Configuration conf) {
    this.conf = conf;
  }
}

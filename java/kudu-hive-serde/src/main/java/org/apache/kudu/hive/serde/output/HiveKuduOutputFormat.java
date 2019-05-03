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

package org.apache.kudu.hive.serde.output;

import java.io.IOException;
import java.util.Properties;
import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.exec.FileSinkOperator.RecordWriter;
import org.apache.hadoop.hive.ql.io.HiveOutputFormat;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.util.Progressable;
import org.apache.kudu.hive.serde.PartialRowWritable;

public class HiveKuduOutputFormat implements HiveOutputFormat<NullWritable, PartialRowWritable>, Configurable {

  private Configuration conf = null;
  @Override
  public Configuration getConf() {
    return conf;
  }

  @Override
  public void setConf(Configuration entries) {
    this.conf = new Configuration(entries);
  }

  @Override
  public RecordWriter getHiveRecordWriter(JobConf jc, Path finalOutPath, Class valueClass, boolean isCompressed,
      Properties tableProperties, Progressable progress) throws IOException {
    return new KuduRecordUpserter(jc);
  }

  @SuppressWarnings("RedundantThrows")
  @Override
  public org.apache.hadoop.mapred.RecordWriter<NullWritable, PartialRowWritable> getRecordWriter(FileSystem ignored,
      JobConf job, String name,
      Progressable progress) throws IOException {
    throw new RuntimeException(
        "getRecordWriter should not be called on a HiveOutputFormat, something went terribly wrong");
  }

  @Override
  public void checkOutputSpecs(FileSystem ignored, JobConf job) throws IOException {
  }
}

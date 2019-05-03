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
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.ql.exec.FileSinkOperator.RecordWriter;
import org.apache.hadoop.io.Writable;
import org.apache.kudu.client.AsyncKuduSession;
import org.apache.kudu.client.KuduClient;
import org.apache.kudu.client.KuduSession;
import org.apache.kudu.client.KuduTable;
import org.apache.kudu.client.Operation;
import org.apache.kudu.client.OperationResponse;
import org.apache.kudu.client.RowError;
import org.apache.kudu.hive.serde.HiveKuduBridgeUtils;
import org.apache.kudu.hive.serde.PartialRowWritable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class KuduRecordUpserter implements RecordWriter {

  private static final Logger LOGGER = LoggerFactory.getLogger(KuduRecordUpserter.class);

  /**
   * Job parameter that specifies the output table.
   */
  private static final String OUTPUT_TABLE_KEY = "kudu.mapreduce.output.table";

  /**
   * Number of rows that are buffered before flushing to the tablet server
   */
  private static final String BUFFER_ROW_COUNT_KEY = "kudu.mapreduce.buffer.row.count";
  private final AtomicLong rowsWithErrors = new AtomicLong();
  private KuduClient client;
  private KuduTable table;
  private KuduSession session;

  KuduRecordUpserter(Configuration jobConf) throws IOException {

    this.client = HiveKuduBridgeUtils.getKuduClient(jobConf);
    String tableName = jobConf.get(OUTPUT_TABLE_KEY);
    try {
      this.table = client.openTable(tableName);
    } catch (Exception ex) {
      throw new IOException("Could not obtain the table from the master, " +
          "is the master running and is this table created? tablename=" + tableName);
    }
    this.session = client.newSession();
    this.session.setFlushMode(AsyncKuduSession.FlushMode.AUTO_FLUSH_BACKGROUND);
    this.session.setMutationBufferSpace(jobConf.getInt(BUFFER_ROW_COUNT_KEY, 1000));
    this.session.setIgnoreAllDuplicateRows(true);
  }


  @Override
  public void write(Writable row) throws IOException {
    this.apply(table.newUpsert(), row);
  }


  private void apply(Operation operation, Object row) throws IOException {

    if (!(row instanceof PartialRowWritable)) {
      throw new IOException("Only accepts PartialRowWritable as Input");
    }
    PartialRowWritable writable = (PartialRowWritable) row;

    try {
      writable.mergeInto(operation.getRow());
      session.apply(operation);
    } catch (Exception e) {
      throw new IOException("Encountered an error while writing", e);
    }

    if (session.countPendingErrors() > 0) {
      throw new IOException("Encountered an error while writing: " +
          session.getPendingErrors().getRowErrors()[0].toString());
    }
  }

  @Override
  public void close(boolean abort) throws IOException {
    LOGGER.info("closing client, statistics");
    LOGGER.info(client.getStatistics().toString());
    try {
      processRowErrorsAndShutdown(session.close());
    } catch (Exception e) {
      throw new IOException("Encountered an error while closing this task", e);
    }
  }

  private void processRowErrorsAndShutdown(List<OperationResponse> responses) throws IOException {
    List<RowError> errors = OperationResponse.collectErrors(responses);
    client.shutdown();
    if (!errors.isEmpty()) {
      int rowErrorsCount = errors.size();
      rowsWithErrors.addAndGet(rowErrorsCount);
      throw new IOException("Kudu session encountered" + rowErrorsCount + " errors writing rows, the first one being "
          + errors.get(0).getErrorStatus());
    }
  }
}

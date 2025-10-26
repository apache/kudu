// Licensed to the Apache Software Foundation (ASF) under one or more
// contributor license agreements.  See the NOTICE file distributed with
// this work for additional information regarding copyright ownership.
// The ASF licenses this file to You under the Apache License, Version 2.0
// (the "License"); you may not use this file except in compliance with
// the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package org.apache.kudu.replication.wrappedsource;

import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;

import org.apache.flink.api.connector.source.ReaderOutput;
import org.apache.flink.api.connector.source.SourceReader;
import org.apache.flink.connector.kudu.source.split.KuduSourceSplit;
import org.apache.flink.core.io.InputStatus;

/**
 * A wrapper around KuduSourceReader that disables reader state checkpointing.
 * This ensures all split lifecycle management happens through the enumerator,
 * preventing duplicate split assignments on restore.
 *
 * Background: On restore, Flink's SourceOperator automatically restores reader
 * state (splits) while our enumerator re-enqueues pending splits to handle the
 * checkpoint race condition. This causes duplicate split IDs in the reader, leading
 * to potential crash when the first instance finishes and removes the split from internal
 * tracking, leaving the second instance orphaned.
 *
 * Solution: By making reader stateless (snapshotState returns empty list), all
 * splits flow through the enumerator's unassigned queue on restore, eliminating
 * duplicates.
 *
 * Efficiency: The Kudu connector does not implement partial progress tracking for
 * splits (KuduSourceSplit is used as both split and split state with no progress
 * tracking). Splits are always read from the beginning on assignment, so there's no
 * efficiency loss from not checkpointing reader state.
 */
public class StatelessKuduReaderWrapper<OUT>
    implements SourceReader<OUT, KuduSourceSplit> {

  private final SourceReader<OUT, KuduSourceSplit> delegate;

  public StatelessKuduReaderWrapper(SourceReader<OUT, KuduSourceSplit> delegate) {
    this.delegate = delegate;
  }

  @Override
  public void start() {
    delegate.start();
  }

  @Override
  public InputStatus pollNext(ReaderOutput<OUT> output) throws Exception {
    return delegate.pollNext(output);
  }

  @Override
  public List<KuduSourceSplit> snapshotState(long checkpointId) {
    return Collections.emptyList();
  }

  @Override
  public CompletableFuture<Void> isAvailable() {
    return delegate.isAvailable();
  }

  @Override
  public void addSplits(List<KuduSourceSplit> splits) {
    delegate.addSplits(splits);
  }

  @Override
  public void notifyNoMoreSplits() {
    delegate.notifyNoMoreSplits();
  }

  @Override
  public void close() throws Exception {
    delegate.close();
  }

  @Override
  public void notifyCheckpointComplete(long checkpointId) throws Exception {
    delegate.notifyCheckpointComplete(checkpointId);
  }

  @Override
  public void notifyCheckpointAborted(long checkpointId) throws Exception {
    delegate.notifyCheckpointAborted(checkpointId);
  }
}


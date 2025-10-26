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

import java.time.Duration;

import org.apache.flink.api.connector.source.Boundedness;
import org.apache.flink.api.connector.source.Source;
import org.apache.flink.api.connector.source.SourceReader;
import org.apache.flink.api.connector.source.SourceReaderContext;
import org.apache.flink.api.connector.source.SplitEnumerator;
import org.apache.flink.api.connector.source.SplitEnumeratorContext;
import org.apache.flink.connector.kudu.connector.KuduTableInfo;
import org.apache.flink.connector.kudu.connector.reader.KuduReaderConfig;
import org.apache.flink.connector.kudu.source.KuduSource;
import org.apache.flink.connector.kudu.source.enumerator.KuduSourceEnumeratorState;
import org.apache.flink.connector.kudu.source.split.KuduSourceSplit;
import org.apache.flink.core.io.SimpleVersionedSerializer;

/**
 * A wrapper around KuduSource that adds metrics support and checkpoint race condition fixes.
 * Uses the delegate pattern - all Source methods are delegated to the wrapped KuduSource,
 * except for:
 * - Enumerator creation: injects MetricWrappedKuduEnumerator for metrics and race fix
 * - Reader creation: wraps with StatelessKuduReaderWrapper to prevent duplicate split assignment
 *
 * TODO(mgreber): remove once FLINK-38187 and FLINK-38575 are resolved.
 */
public class MetricWrappedKuduSource<OUT> implements Source<OUT,
                                                            KuduSourceSplit,
                                                            KuduSourceEnumeratorState> {
  private final KuduSource<OUT> delegate;
  private final KuduReaderConfig readerConfig;
  private final KuduTableInfo tableInfo;
  private final Boundedness boundedness;
  private final Duration discoveryInterval;

  public MetricWrappedKuduSource(KuduSource<OUT> delegate) {
    this.delegate = delegate;
    this.boundedness = delegate.getBoundedness();

    // We use reflection here for convenience - ideally we could plumb configs in the
    // environment provider, but this wrapping approach allows us to simply supply a KuduSource
    // to the wrapped source in a clean way. This makes it easier to remove the wrapper later
    // when the metrics are implemented in the connector (FLINK-38187),
    // as it's a drop-in replacement.
    this.readerConfig = ReflectionSecurityUtils.getPrivateFieldValue(delegate, "readerConfig");
    this.tableInfo = ReflectionSecurityUtils.getPrivateFieldValue(delegate, "tableInfo");
    this.discoveryInterval =
        ReflectionSecurityUtils.getPrivateFieldValue(delegate, "discoveryPeriod");
  }



  @Override
  public Boundedness getBoundedness() {
    return boundedness;
  }

  @Override
  public SourceReader<OUT, KuduSourceSplit> createReader(
          SourceReaderContext readerContext) throws Exception {
    SourceReader<OUT, KuduSourceSplit> reader = delegate.createReader(readerContext);
    return new StatelessKuduReaderWrapper<>(reader);
  }

  @Override
  public SplitEnumerator<KuduSourceSplit, KuduSourceEnumeratorState> createEnumerator(
          SplitEnumeratorContext<KuduSourceSplit> context) {
    return new MetricWrappedKuduEnumerator(
            tableInfo, readerConfig, boundedness, discoveryInterval, context, null);
  }

  @Override
  public SplitEnumerator<KuduSourceSplit, KuduSourceEnumeratorState> restoreEnumerator(
          SplitEnumeratorContext<KuduSourceSplit> context,
          KuduSourceEnumeratorState checkpoint) {
    return new MetricWrappedKuduEnumerator(
            tableInfo, readerConfig, boundedness, discoveryInterval, context, checkpoint);
  }

  @Override
  public SimpleVersionedSerializer<KuduSourceSplit> getSplitSerializer() {
    return delegate.getSplitSerializer();
  }

  @Override
  public SimpleVersionedSerializer<KuduSourceEnumeratorState> getEnumeratorCheckpointSerializer() {
    return delegate.getEnumeratorCheckpointSerializer();
  }
}


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

package org.apache.kudu.replication;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * Provides a configured {@link StreamExecutionEnvironment} for the replication job.
 * This class does not execute the environment; it only prepares it.
 */
public class ReplicationEnvProvider {

  /**
   * Builds and configures the {@link StreamExecutionEnvironment} for the replication job.
   *
   * @return the fully configured {@link StreamExecutionEnvironment}
   * @throws Exception if table initialization or environment setup fails
   */
  public StreamExecutionEnvironment getEnv() throws Exception {
    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    return env;
  }
}

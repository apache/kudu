/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.kududb.flume.sink;

import org.kududb.annotations.InterfaceAudience;
import org.kududb.annotations.InterfaceStability;

/**
 * Constants used for configuration of KuduSink
 */

@InterfaceAudience.Public
@InterfaceStability.Evolving
public class KuduSinkConfigurationConstants {
  /**
   * Comma-separated list of "host:port" pairs of the masters (port optional).
   */
  public static final String MASTER_ADDRESSES = "masterAddresses";

  /**
   * The name of the table in Kudu to write to.
   */
  public static final String TABLE_NAME = "tableName";

  /**
   * The fully qualified class name of the Kudu event producer the sink should use.
   */
  public static final String PRODUCER = "producer";

  /**
   * Configuration to pass to the Kudu event producer.
   */
  public static final String PRODUCER_PREFIX = PRODUCER + ".";

  /**
   * Maximum number of events the sink should take from the channel per
   * transaction, if available.
   */
  public static final String BATCH_SIZE = "batchSize";

  /**
   * Timeout period for Kudu operations, in milliseconds.
   */
  public static final String TIMEOUT_MILLIS = "timeoutMillis";

  /**
   * Whether to ignore errors indicating that we attempted to insert duplicate rows into Kudu.
   */
  public static final String IGNORE_DUPLICATE_ROWS = "ignoreDuplicateRows";
}

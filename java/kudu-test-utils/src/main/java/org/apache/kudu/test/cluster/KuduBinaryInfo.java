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

package org.apache.kudu.test.cluster;

import org.apache.yetus.audience.InterfaceAudience;
import org.apache.yetus.audience.InterfaceStability;

/**
 * Simple struct to provide various properties of a binary artifact to callers.
 */
@InterfaceAudience.Private
@InterfaceStability.Unstable
public class KuduBinaryInfo {
  private final String binDir;
  private final String saslDir;

  public KuduBinaryInfo(String binDir, String saslDir) {
    this.binDir = binDir;
    this.saslDir = saslDir;
  }

  public KuduBinaryInfo(String binDir) {
    this(binDir, null);
  }

  /**
   * Return the binary directory of an extracted artifact.
   */
  public String getBinDir() {
    return binDir;
  }

  /**
   * Return the SASL module directory of an extracted artifact.
   * May be {@code null} if unknown.
   */
  public String getSaslDir() {
    return saslDir;
  }
}

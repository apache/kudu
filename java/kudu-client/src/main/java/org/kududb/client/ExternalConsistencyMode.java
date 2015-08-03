// Copyright (c) 2013, Cloudera, inc.
// Confidential Cloudera Information: Covered by NDA.
package org.kududb.client;

import org.kududb.Common;

/**
 * The possible external consistency modes on which Kudu operates.
 * @see src/kudu/common/common.proto for a detailed explanations on the
 *      meaning and implications of each mode.
 */
public enum ExternalConsistencyMode {
  CLIENT_PROPAGATED(Common.ExternalConsistencyMode.CLIENT_PROPAGATED),
  COMMIT_WAIT(Common.ExternalConsistencyMode.COMMIT_WAIT);

  private Common.ExternalConsistencyMode pbVersion;
  private ExternalConsistencyMode(Common.ExternalConsistencyMode pbVersion) {
    this.pbVersion = pbVersion;
  }
  public Common.ExternalConsistencyMode pbVersion() {
    return pbVersion;
  }
}

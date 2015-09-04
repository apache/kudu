// Copyright (c) 2013, Cloudera, inc.
// Confidential Cloudera Information: Covered by NDA.
package org.kududb.client;

import org.kududb.Common;
import org.kududb.annotations.InterfaceAudience;
import org.kududb.annotations.InterfaceStability;

/**
 * The possible external consistency modes on which Kudu operates.
 * See {@code src/kudu/common/common.proto} for a detailed explanations on the
 *      meaning and implications of each mode.
 */
@InterfaceAudience.Public
@InterfaceStability.Evolving
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

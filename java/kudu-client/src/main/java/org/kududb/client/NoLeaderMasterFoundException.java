// Copyright (c) 2015, Cloudera, inc.
// Confidential Cloudera Information: Covered by NDA.
package org.kududb.client;

import com.google.common.base.Functions;
import com.google.common.base.Joiner;
import com.google.common.collect.Lists;
import org.kududb.annotations.InterfaceAudience;
import org.kududb.annotations.InterfaceStability;

import java.util.List;

/**
 * Indicates that the request failed because we couldn't find a leader master server.
 */
@InterfaceAudience.Public
@InterfaceStability.Evolving
public final class NoLeaderMasterFoundException extends RecoverableException {

  NoLeaderMasterFoundException(final String msg) {
    super(msg);
  }
  NoLeaderMasterFoundException(final String msg, final Exception cause) {
    super(msg, cause);
  }

  /**
   * Factory method that creates a NoLeaderException given a message and a list
   * (which may be empty, but must be initialized) of exceptions encountered: they indicate
   * why {@link GetMasterRegistrationRequest} calls to the masters in the config
   * have failed, to aid in debugging the issue. If the list is non-empty, each exception's
   * 'toString()' message is appended to 'msg' and the last exception is used as the
   * cause for the exception.
   * @param msg A message detailing why this exception occured.
   * @param causes List of exceptions encountered when retrieving registration from individual
   *               masters.
   * @return An instantiated NoLeaderMasterFoundException which can be thrown.
   */
  static NoLeaderMasterFoundException create(String msg, List<Exception> causes) {
    if (causes.isEmpty()) {
      return new NoLeaderMasterFoundException(msg);
    }
    String joinedMsg = msg + ". Exceptions received: " +
        Joiner.on(",").join(Lists.transform(causes, Functions.toStringFunction()));
    return new NoLeaderMasterFoundException(joinedMsg, causes.get(causes.size() - 1));
  }
}

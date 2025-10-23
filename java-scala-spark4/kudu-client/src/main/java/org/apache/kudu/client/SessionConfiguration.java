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

package org.apache.kudu.client;

import org.apache.yetus.audience.InterfaceAudience;
import org.apache.yetus.audience.InterfaceStability;

/**
 * Interface that defines the methods used to configure a session. It also exposes ways to
 * query its state.
 */
@InterfaceAudience.Public
@InterfaceStability.Evolving
public interface SessionConfiguration {

  @InterfaceAudience.Public
  @InterfaceStability.Evolving
  enum FlushMode {
    /**
     * Each {@link KuduSession#apply KuduSession.apply()} call will return only after being
     * flushed to the server automatically. No batching will occur.
     *
     * <p>In this mode, the {@link KuduSession#flush} call never has any effect, since each
     * {@link KuduSession#apply KuduSession.apply()} has already flushed the buffer before
     * returning.
     *
     * <p><strong>This is the default flush mode.</strong>
     */
    AUTO_FLUSH_SYNC,

    /**
     * {@link KuduSession#apply KuduSession.apply()} calls will return immediately, but the writes
     * will be sent in the background, potentially batched together with other writes from
     * the same session. If there is not sufficient buffer space, then
     * {@link KuduSession#apply KuduSession.apply()} may block for buffer space to be available.
     *
     * <p>Because writes are applied in the background, any errors will be stored
     * in a session-local buffer. Call {@link #countPendingErrors() countPendingErrors()} or
     * {@link #getPendingErrors() getPendingErrors()} to retrieve them.
     *
     * <p><strong>Note:</strong> The {@code AUTO_FLUSH_BACKGROUND} mode may result in
     * out-of-order writes to Kudu. This is because in this mode multiple write
     * operations may be sent to the server in parallel.
     * See <a href="https://issues.apache.org/jira/browse/KUDU-1767">KUDU-1767</a> for more
     * information.
     *
     * <p>The {@link KuduSession#flush()} call can be used to block until the buffer is empty.
     */
    AUTO_FLUSH_BACKGROUND,

    /**
     * {@link KuduSession#apply KuduSession.apply()} calls will return immediately, but the writes
     * will not be sent until the user calls {@link KuduSession#flush()}. If the buffer runs past
     * the configured space limit, then {@link KuduSession#apply KuduSession.apply()} will return
     * an error.
     */
    MANUAL_FLUSH
  }

  /**
   * Get the current flush mode.
   * @return flush mode, {@link FlushMode#AUTO_FLUSH_SYNC AUTO_FLUSH_SYNC} by default
   */
  FlushMode getFlushMode();

  /**
   * Set the new flush mode for this session.
   * @param flushMode new flush mode, can be the same as the previous one.
   * @throws IllegalArgumentException if the buffer isn't empty.
   */
  void setFlushMode(FlushMode flushMode);

  /**
   * Set the number of operations that can be buffered.
   * @param size number of ops.
   * @throws IllegalArgumentException if the buffer isn't empty.
   */
  default void setMutationBufferSpace(int size) {
    setMutationBufferSpace(size, -1);
  }

  /**
   * Set the number and the maximum byte size of operations that can be buffered.
   * @param numOps number of ops.
   * @param maxSize max byte size of ops.
   * @throws IllegalArgumentException if the buffer isn't empty.
   */
  void setMutationBufferSpace(int numOps, long maxSize);

  /**
   * Set the low watermark for this session. The default is set to half the mutation buffer space.
   * For example, a buffer space of 1000 with a low watermark set to 50% (0.5) will start randomly
   * sending PleaseRetryExceptions once there's an outstanding flush and the buffer is over 500.
   * As the buffer gets fuller, it becomes likelier to hit the exception.
   * @param mutationBufferLowWatermarkPercentage a new low watermark as a percentage,
   *                             has to be between 0  and 1 (inclusive). A value of 1 disables
   *                             the low watermark since it's the same as the high one
   * @throws IllegalArgumentException if the buffer isn't empty or if the watermark isn't between
   * 0 and 1
   * @deprecated The low watermark no longer has any effect.
   */
  @Deprecated
  void setMutationBufferLowWatermark(float mutationBufferLowWatermarkPercentage);

  /**
   * Set the flush interval, which will be used for the next scheduling decision.
   * @param interval interval in milliseconds.
   */
  void setFlushInterval(int interval);

  /**
   * Get the current timeout.
   * @return operation timeout in milliseconds, 0 if none was configured.
   */
  long getTimeoutMillis();

  /**
   * Sets the timeout for the next applied operations.
   * The default timeout is 0, which disables the timeout functionality.
   * @param timeout Timeout in milliseconds.
   */
  void setTimeoutMillis(long timeout);

  /**
   * Returns true if this session has already been closed.
   */
  boolean isClosed();

  /**
   * Check if there are operations that haven't been completely applied.
   * @return true if operations are pending, else false.
   */
  boolean hasPendingOperations();

  /**
   * Set the new external consistency mode for this session.
   * @param consistencyMode new external consistency mode, can the same as the previous one.
   * @throws IllegalArgumentException if the buffer isn't empty.
   */
  void setExternalConsistencyMode(ExternalConsistencyMode consistencyMode);

  /**
   * Tells if the session is currently ignoring row errors when the whole list returned by a tablet
   * server is of the AlreadyPresent type.
   * @return true if the session is enforcing this, else false
   */
  boolean isIgnoreAllDuplicateRows();

  /**
   * Configures the option to ignore all the row errors if they are all of the AlreadyPresent type.
   * This can be useful when it is possible for INSERT operations to be retried and fail.
   * The effect of enabling this is that operation responses that match this pattern will be
   * cleared of their row errors, meaning that we consider them successful.
   *
   * TODO(KUDU-1563): Implement server side ignore capabilities to improve performance and
   *  reliability of INSERT ignore operations.
   *
   * <p>Disabled by default.
   * @param ignoreAllDuplicateRows true if this session should enforce this, else false
   */
  void setIgnoreAllDuplicateRows(boolean ignoreAllDuplicateRows);

  /**
   * Tells if the session is currently ignoring row errors when the whole list returned by a tablet
   * server is of the NotFound type.
   * @return true if the session is enforcing this, else false
   */
  boolean isIgnoreAllNotFoundRows();

  /**
   * Configures the option to ignore all the row errors if they are all of the NotFound type.
   * This can be useful when it is possible for DELETE operations to be retried and fail.
   * The effect of enabling this is that operation responses that match this pattern will be
   * cleared of their row errors, meaning that we consider them successful.
   *
   * TODO(KUDU-1563): Implement server side ignore capabilities to improve performance and
   *  reliability of DELETE ignore operations.
   *
   * <p>Disabled by default.
   * @param ignoreAllNotFoundRows true if this session should enforce this, else false
   */
  void setIgnoreAllNotFoundRows(boolean ignoreAllNotFoundRows);

  /**
   * Set the number of errors that can be collected.
   * @param size number of errors.
   */
  void setErrorCollectorSpace(int size);

  /**
   * Return the number of errors which are pending. Errors may accumulate when
   * using {@link FlushMode#AUTO_FLUSH_BACKGROUND AUTO_FLUSH_BACKGROUND} mode.
   * @return a count of errors
   */
  int countPendingErrors();

  /**
   * Return any errors from previous calls. If there were more errors
   * than could be held in the session's error storage, the overflow state is set to true.
   *
   * <p>Clears the pending errors.
   * @return an object that contains the errors and the overflow status
   */
  RowErrorsAndOverflowStatus getPendingErrors();

  /**
   * Return cumulative write operation metrics since the beginning of the session.
   * @return cumulative write operation metrics since the beginning of the session.
   */
  ResourceMetrics getWriteOpMetrics();
}

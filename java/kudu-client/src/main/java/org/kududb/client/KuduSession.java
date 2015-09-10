// Copyright (c) 2014, Cloudera, inc.
// Confidential Cloudera Information: Covered by NDA.
package org.kududb.client;

import org.kududb.annotations.*;

import com.stumbleupon.async.Deferred;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Synchronous version of {@link AsyncKuduSession}. Offers the same API but with blocking methods.
 *
 * A major difference with {@link AsyncKuduSession} is that the time spent waiting on operations is
 * defined by {@link #setTimeoutMillis(long)} which defaults to getting it from
 * {@link KuduClient#getDefaultOperationTimeoutMs()}.
 */
@InterfaceAudience.Public
@InterfaceStability.Evolving
public class KuduSession implements SessionConfiguration {

  public static final Logger LOG = LoggerFactory.getLogger(KuduSession.class);

  private final AsyncKuduSession session;

  KuduSession(AsyncKuduSession session) {
    this.session = session;
  }

  /**
   * Blocking call with a different behavior based on the flush mode. PleaseThrottleException is
   * managed by this method and will not be thrown, unlike {@link AsyncKuduSession#apply}.
   * <p>
   * <ul>
   * <li>AUTO_FLUSH_SYNC: the call returns when the operation is persisted,
   * else it throws an exception.
   * <li>AUTO_FLUSH_BACKGROUND: the call returns when the operation has been added to the buffer.
   * The operation's state is then unreachable, meaning that there's no way to know if the
   * operation is persisted. This call should normally perform only fast in-memory operations but
   * it may have to wait when the buffer is full and there's another buffer being flushed.
   * <li>MANUAL_FLUSH: the call returns when the operation has been added to the buffer,
   * else it throws an exception such as a NonRecoverableException if the buffer is full.
   * </ul>
   *
   * @param operation operation to apply
   * @return an OperationResponse for the applied Operation
   * @throws Exception if anything went wrong
   */
  public OperationResponse apply(Operation operation) throws Exception {
    while (true) {
      try {
        Deferred<OperationResponse> d = session.apply(operation);
        if (getFlushMode() == FlushMode.AUTO_FLUSH_SYNC) {
          return d.join(getTimeoutMillis());
        }
        break;
      } catch (PleaseThrottleException ex) {
        try {
          ex.getDeferred().join(getTimeoutMillis());
        } catch (Exception e) {
          // This is the error response from the buffer that was flushing,
          // we can't do much with it at this point.
          LOG.error("Previous batch had this exception", e);
        }
      } catch (Exception e) {
        throw e;
      }
    }
    return null;
  }

  /**
   * Blocking call that force flushes this session's buffers. Data is persisted when this call
   * returns, else it will throw an exception.
   * @return a list of OperationResponse, one per operation that was flushed
   * @throws Exception if anything went wrong. If it's an issue with some or all batches,
   * it will be of type DeferredGroupException.
   */
  public List<OperationResponse> flush() throws Exception {
    return session.flush().join(getTimeoutMillis());
  }

  /**
   * Blocking call that flushes the buffers (see {@link #flush()} and closes the sessions.
   * @return List of OperationResponse, one per operation that was flushed
   * @throws Exception if anything went wrong. If it's an issue with some or all batches,
   * it will be of type DeferredGroupException.
   */
  public List<OperationResponse> close() throws Exception {
    return session.close().join(getTimeoutMillis());
  }

  @Override
  public FlushMode getFlushMode() {
    return session.getFlushMode();
  }

  @Override
  public void setFlushMode(AsyncKuduSession.FlushMode flushMode) {
    session.setFlushMode(flushMode);
  }

  @Override
  public void setMutationBufferSpace(int size) {
    session.setMutationBufferSpace(size);
  }

  @Override
  public void setMutationBufferLowWatermark(float mutationBufferLowWatermarkPercentage) {
    session.setMutationBufferLowWatermark(mutationBufferLowWatermarkPercentage);
  }

  @Override
  public void setFlushInterval(int interval) {
    session.setFlushInterval(interval);
  }

  @Override
  public long getTimeoutMillis() {
    return session.getTimeoutMillis();
  }

  @Override
  public void setTimeoutMillis(long timeout) {
    session.setTimeoutMillis(timeout);
  }

  @Override
  public boolean isClosed() {
    return session.isClosed();
  }

  @Override
  public boolean hasPendingOperations() {
    return session.hasPendingOperations();
  }

  @Override
  public void setExternalConsistencyMode(ExternalConsistencyMode consistencyMode) {
    session.setExternalConsistencyMode(consistencyMode);
  }

  @Override
  public boolean isIgnoreAllDuplicateRows() {
    return session.isIgnoreAllDuplicateRows();
  }

  @Override
  public void setIgnoreAllDuplicateRows(boolean ignoreAllDuplicateRows) {
    session.setIgnoreAllDuplicateRows(ignoreAllDuplicateRows);
  }
}

// Copyright (c) 2015, Cloudera, inc.
// Confidential Cloudera Information: Covered by NDA.
package org.kududb.util;

import com.stumbleupon.async.Callback;
import com.stumbleupon.async.Deferred;
import org.kududb.annotations.InterfaceAudience;

/**
 * Utility methods for various parts of async, such as Deferred.
 * TODO (KUDU-602): Some of these methods could eventually be contributed back to async or to a
 * custom fork/derivative of async.
 */
@InterfaceAudience.Private
public class AsyncUtil {

  /**
   * Register a callback and an "errback".
   * <p>
   * This has the exact same effect as {@link Deferred#addCallbacks(Callback, Callback)}
   * keeps the type information "correct" when the callback and errback return a
   * {@code Deferred}.
   * @param d The {@code Deferred} we want to add the callback and errback to.
   * @param cb The callback to register.
   * @param eb The errback to register.
   * @return {@code d} with an "updated" type.
   */
  @SuppressWarnings("unchecked")
  public static <T, R, D extends Deferred<R>, E>
  Deferred<R> addCallbacksDeferring(final Deferred<T> d,
                                    final Callback<D, T> cb,
                                    final Callback<D, E> eb) {
    return d.addCallbacks((Callback<R, T>) ((Object) cb),
                          (Callback<R, E>) ((Object) eb));
  }
}

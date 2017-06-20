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

package org.apache.kudu.util;

import com.stumbleupon.async.Callback;
import com.stumbleupon.async.Deferred;
import org.apache.yetus.audience.InterfaceAudience;

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
    return d.addCallbacks((Callback<R, T>) cb, eb);
  }

  /**
   * Workaround for {@link Deferred#addBoth}'s failure to use generics correctly. Allows callers
   * to provide a {@link Callback} which takes an {@link Object} instead of the type of the deferred
   * it is applied to, which avoids a runtime {@link ClassCastException} when the deferred fails.
   */
  @SuppressWarnings("unchecked")
  public static <T, U> Deferred<U> addBoth(final Deferred<T> deferred,
                                           final Callback<? extends U, Object> callback) {
    return ((Deferred) deferred).addBoth(callback);
  }

  /**
   * Workaround for {@link Deferred#addBothDeferring}'s failure to use generics correctly. Allows
   * callers to provide a {@link Callback} which takes an {@link Object} instead of the type of the
   * deferred it is applied to, which avoids a runtime {@link ClassCastException} when the deferred
   * fails.
   */
  @SuppressWarnings("unchecked")
  public static <T, U> Deferred<U> addBothDeferring(final Deferred<T> deferred,
                                                    final Callback<Deferred<U>, Object> callback) {
    return ((Deferred) deferred).addBothDeferring(callback);
  }
}

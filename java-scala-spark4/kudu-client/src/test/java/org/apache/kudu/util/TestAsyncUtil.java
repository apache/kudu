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

import static org.junit.Assert.assertEquals;

import com.stumbleupon.async.Callback;
import com.stumbleupon.async.Deferred;
import org.junit.Rule;
import org.junit.Test;

import org.apache.kudu.test.junit.RetryRule;

/**
 * Test for {@link AsyncUtil}.
 */
public class TestAsyncUtil {

  @Rule
  public RetryRule retryRule = new RetryRule();

  @Test(expected = IllegalStateException.class)
  public void testAddCallbacksDeferring() throws Exception {
    Deferred<String> d = new Deferred<>();
    TestCallback cb = new TestCallback();
    TestErrback eb = new TestErrback();

    // Test normal callbacks.
    AsyncUtil.addCallbacksDeferring(d, cb, eb);
    final String testStr = "hello world";
    d.callback(testStr);
    assertEquals(d.join(), "callback: " + testStr);

    d = new Deferred<>();
    AsyncUtil.addCallbacksDeferring(d, cb, eb);
    d.callback(new IllegalArgumentException());
    assertEquals(d.join(), "illegal arg");

    d = new Deferred<>();
    AsyncUtil.addCallbacksDeferring(d, cb, eb);
    d.callback(new IllegalStateException());
    d.join();
  }

  static final class TestCallback implements Callback<Deferred<String>, String> {
    @Override
    public Deferred<String> call(String arg) throws Exception {
      return Deferred.fromResult("callback: " + arg);
    }
  }

  static final class TestErrback implements Callback<Deferred<String>, Exception> {
    @Override
    public Deferred<String> call(Exception arg) {
      if (arg instanceof IllegalArgumentException) {
        return Deferred.fromResult("illegal arg");
      }
      return Deferred.fromError(arg);
    }
  }
}

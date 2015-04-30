// Copyright (c) 2015, Cloudera, inc.
// Confidential Cloudera Information: Covered by NDA.
package org.kududb.util;

import com.stumbleupon.async.Callback;
import com.stumbleupon.async.Deferred;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import static org.junit.Assert.assertEquals;

/**
 * Test for {@link AsyncUtil}.
 */
public class TestAsyncUtil {

  @Rule
  public ExpectedException exception = ExpectedException.none();

  @Test
  public void testAddCallbacksDeferring() throws Exception {
    Deferred<String> d = new Deferred<String>();
    TestCallback cb = new TestCallback();
    TestErrback eb = new TestErrback();

    // Test normal callbacks.
    AsyncUtil.addCallbacksDeferring(d, cb, eb);
    final String testStr = "hello world";
    d.callback(testStr);
    assertEquals(d.join(), "callback: " + testStr);

    d = new Deferred<String>();
    AsyncUtil.addCallbacksDeferring(d, cb, eb);
    d.callback(new IllegalArgumentException());
    assertEquals(d.join(), "illegal arg");

    d = new Deferred<String>();
    AsyncUtil.addCallbacksDeferring(d, cb, eb);
    d.callback(new IllegalStateException());
    exception.expect(IllegalStateException.class);
    d.join();
  }

  final class TestCallback implements Callback<Deferred<String>, String> {
    @Override
    public Deferred<String> call(String arg) throws Exception {
      return Deferred.fromResult("callback: " + arg);
    }
  }

  final class TestErrback implements Callback<Deferred<String>, Exception> {
    @Override
    public Deferred<String> call(Exception arg) {
      if (arg instanceof IllegalArgumentException) {
        return Deferred.fromResult("illegal arg");
      }
      return Deferred.fromError(arg);
    }
  }
}

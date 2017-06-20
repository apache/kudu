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

import static org.hamcrest.CoreMatchers.containsString;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

import javax.security.auth.Subject;

import org.junit.Test;

import org.apache.kudu.util.SecurityUtil;

public class TestMiniKdc {

  @Test
  public void testBasicFunctionality() throws Exception {
    try (MiniKdc kdc = MiniKdc.withDefaults()) {
      kdc.start();

      kdc.createUserPrincipal("alice");
      kdc.kinit("alice");

      kdc.stop();
      kdc.start();

      kdc.createUserPrincipal("bob");
      kdc.kinit("bob");

      kdc.createServiceKeytab("kudu/KRBTEST.COM");

      String klist = kdc.klist();

      assertFalse(klist.contains("alice@KRBTEST.COM"));
      assertTrue(klist.contains("bob@KRBTEST.COM"));
      assertTrue(klist.contains("krbtgt/KRBTEST.COM@KRBTEST.COM"));
    }
  }

  /**
   * Test that we can initialize a JAAS Subject from a user-provided TicketCache.
   */
  @Test
  public void testGetKerberosSubject() throws Exception {
    try (MiniKdc kdc = MiniKdc.withDefaults()) {
      kdc.start();
      kdc.createUserPrincipal("alice");
      kdc.kinit("alice");
      // Typically this would be picked up from the $KRB5CCNAME environment
      // variable, or use a default. However, it's not easy to modify the
      // environment in Java, so instead we override a system property.
      System.setProperty(SecurityUtil.KUDU_TICKETCACHE_PROPERTY, kdc.getTicketCachePath());
      Subject subj = SecurityUtil.getSubjectOrLogin();
      assertThat(subj.toString(), containsString("alice"));
    }
  }

  @Test
  public void testStopClose() throws Exception {
    // Test that closing a stopped KDC does not throw.
    MiniKdc.withDefaults().close();
  }
}

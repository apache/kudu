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

import org.junit.Test;

import static org.junit.Assert.*;

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

      assertTrue(klist.contains("alice@KRBTEST.COM"));
      assertTrue(klist.contains("bob@KRBTEST.COM"));
      assertTrue(klist.contains("krbtgt/KRBTEST.COM@KRBTEST.COM"));
    }
  }
}

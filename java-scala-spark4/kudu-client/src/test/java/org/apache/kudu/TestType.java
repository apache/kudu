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

package org.apache.kudu;

import static org.junit.Assert.assertEquals;

import org.junit.Rule;
import org.junit.Test;

import org.apache.kudu.test.junit.RetryRule;

public class TestType {

  @Rule
  public RetryRule retryRule = new RetryRule();

  @Test
  public void testGetTypeForName() {
    String name = Type.INT64.getName();
    Type newType = Type.getTypeForName(name);

    assertEquals("Get Type from getName()", Type.INT64, newType);

    name = Type.INT64.name();
    newType = Type.getTypeForName(name);

    assertEquals("Get Type from name()", Type.INT64, newType);
  }
}

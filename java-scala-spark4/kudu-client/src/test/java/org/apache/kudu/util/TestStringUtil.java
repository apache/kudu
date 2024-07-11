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

import org.junit.Rule;
import org.junit.Test;

import org.apache.kudu.test.junit.RetryRule;

public class TestStringUtil {

  @Rule
  public RetryRule retryRule = new RetryRule();

  private String escapeSQLString(String s) {
    StringBuilder sb = new StringBuilder();
    StringUtil.appendEscapedSQLString(s, sb);
    return sb.toString();
  }

  @Test
  public void testAppendEscapedSQLString() {
    assertEquals("", escapeSQLString(""));
    assertEquals("a", escapeSQLString("a"));
    assertEquals("\\n", escapeSQLString("\n"));
    assertEquals("the_quick brown\\tfox\\njumps\\rover\\bthe\\0lazy\\\\dog",
                 escapeSQLString("the_quick brown\tfox\njumps\rover\bthe\0lazy\\dog"));
    assertEquals("\\u0012\\0", escapeSQLString("\u0012\u0000"));
  }
}

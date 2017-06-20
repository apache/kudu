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

import org.apache.yetus.audience.InterfaceAudience;

@InterfaceAudience.Private
public class StringUtil {

  /**
   * Escapes the provided string and appends it to the string builder. The
   * escaping is done according to the Hive/Impala escaping rules. Adapted from
   * org.apache.hadoop.hive.ql.parse.BaseSemanticAnalyzer.escapeSQLString, with
   * one difference: '%' and '_' are not escaped, since the resulting escaped
   * string should not be used for a LIKE statement.
   */
  public static void appendEscapedSQLString(String s, StringBuilder sb) {
    for (int i = 0; i < s.length(); i++) {
      char currentChar = s.charAt(i);
      switch (currentChar) {
        case '\0': {
          sb.append("\\0");
          break;
        }
        case '\'': {
          sb.append("\\'");
          break;
        }
        case '\"': {
          sb.append("\\\"");
          break;
        }
        case '\b': {
          sb.append("\\b");
          break;
        }
        case '\n': {
          sb.append("\\n");
          break;
        }
        case '\r': {
          sb.append("\\r");
          break;
        }
        case '\t': {
          sb.append("\\t");
          break;
        }
        case '\\': {
          sb.append("\\\\");
          break;
        }
        case '\u001A': {
          sb.append("\\Z");
          break;
        }
        default: {
          if (currentChar < ' ') {
            sb.append("\\u");
            String hex = Integer.toHexString(currentChar);
            for (int j = 4; j > hex.length(); --j) {
              sb.append('0');
            }
            sb.append(hex);
          } else {
            sb.append(currentChar);
          }
        }
      }
    }
  }

  /** Non-constructable utility class. */
  private StringUtil() { }
}

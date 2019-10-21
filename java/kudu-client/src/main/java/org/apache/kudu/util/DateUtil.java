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

import java.sql.Date;
import java.time.LocalDate;

import org.apache.yetus.audience.InterfaceAudience;

@InterfaceAudience.Private
public class DateUtil {
  public static final int MIN_DATE_VALUE =
      (int)LocalDate.parse("0001-01-01").toEpochDay(); // -719162
  public static final int MAX_DATE_VALUE =
      (int)LocalDate.parse("9999-12-31").toEpochDay(); // 2932896

  /**
   * Check whether the date is within the range '0001-01-01':'9999-12-31'
   *
   * @param the number days since the Unix epoch
   */
  public static void checkDateWithinRange(long days) {
    if (days < MIN_DATE_VALUE || days > MAX_DATE_VALUE) {
      throw new IllegalArgumentException(
          "Date value <" + days + ">} is out of range '0001-01-01':'9999-12-31'");
    }
  }

  /**
   * Converts a {@link java.sql.Date} to the number of days since the Unix epoch
   * (1970-01-01T00:00:00Z).
   *
   * @param date the date to convert to days
   * @return the number days since the Unix epoch
   */
  public static int sqlDateToEpochDays(Date date) {
    long days = date.toLocalDate().toEpochDay();
    checkDateWithinRange(days);
    return (int)days;
  }

  /**
   * Converts a number of days since the Unix epoch to a {@link java.sql.Date}.
   *
   * @param the number of days since the Unix epoch
   * @return the corresponding Date
   */
  public static Date epochDaysToSqlDate(int days) {
    checkDateWithinRange(days);
    return Date.valueOf(LocalDate.ofEpochDay(days));
  }

  /**
   * Transforms a number of days since the Unix epoch into a string according the ISO-8601 format.
   *
   * @param the number of days since the Unix epoch
   * @return a string, in the format: YYYY-MM-DD
   */
  public static String epochDaysToDateString(int days) {
    return LocalDate.ofEpochDay(days).toString();
  }
}

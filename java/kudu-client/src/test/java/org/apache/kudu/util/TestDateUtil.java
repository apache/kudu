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

import java.sql.Date;
import java.time.LocalDate;

import org.junit.Test;

public class TestDateUtil {

  @Test(expected = IllegalArgumentException.class)
  public void testDateOutOfRange() {
    DateUtil.checkDateWithinRange(LocalDate.of(10000, 1, 1).toEpochDay());
  }

  @Test
  public void testSqlDateToEpochDays() {
    Date b = Date.valueOf(LocalDate.of(1, 1, 1));
    Date e = Date.valueOf(LocalDate.of(9999, 12, 31));
    assertEquals(DateUtil.sqlDateToEpochDays(b), DateUtil.MIN_DATE_VALUE);
    assertEquals(DateUtil.sqlDateToEpochDays(e), DateUtil.MAX_DATE_VALUE);
  }

  @Test
  public void testEpochDaysToSqlDate() {
    Date b = Date.valueOf(LocalDate.of(1, 1, 1));
    Date e = Date.valueOf(LocalDate.of(9999, 12, 31));
    assertEquals(DateUtil.epochDaysToSqlDate(DateUtil.MIN_DATE_VALUE), b);
    assertEquals(DateUtil.epochDaysToSqlDate(DateUtil.MAX_DATE_VALUE), e);
  }

  @Test
  public void testEpochDaysToDateString() {
    assertEquals(DateUtil.epochDaysToDateString(DateUtil.MIN_DATE_VALUE), "0001-01-01");
    assertEquals(DateUtil.epochDaysToDateString(DateUtil.MAX_DATE_VALUE), "9999-12-31");
    assertEquals(DateUtil.epochDaysToDateString(0), "1970-01-01");
    assertEquals(DateUtil.epochDaysToDateString(-10000), "1942-08-16");
    assertEquals(DateUtil.epochDaysToDateString(10000), "1997-05-19");
  }

  @Test(expected = IllegalArgumentException.class)
  public void testMinInt() {
    DateUtil.checkDateWithinRange(Integer.MIN_VALUE);
  }

  @Test(expected = IllegalArgumentException.class)
  public void testMaxInt() {
    DateUtil.checkDateWithinRange(Integer.MAX_VALUE);
  }

  @Test(expected = IllegalArgumentException.class)
  public void testBeforeMinDate() {
    DateUtil.checkDateWithinRange(DateUtil.MIN_DATE_VALUE - 1);
  }

  @Test(expected = IllegalArgumentException.class)
  public void testAfterMaxDate() {
    DateUtil.checkDateWithinRange(DateUtil.MAX_DATE_VALUE + 1);
  }
}

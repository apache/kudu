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

import org.junit.Test;

import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.util.TimeZone;

import static org.junit.Assert.assertEquals;

public class TestTimestampUtil {

  @Test
  public void testTimestampConversion() throws Exception {
    Timestamp epoch = new Timestamp(0);
    assertEquals(0, TimestampUtil.timestampToMicros(epoch));
    assertEquals(epoch, TimestampUtil.microsToTimestamp(0));

    Timestamp t1 = new Timestamp(0);
    t1.setNanos(123456000);
    assertEquals(123456, TimestampUtil.timestampToMicros(t1));
    assertEquals(t1, TimestampUtil.microsToTimestamp(123456));

    SimpleDateFormat iso8601 = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS");
    iso8601.setTimeZone(TimeZone.getTimeZone("UTC"));

    Timestamp t3 = new Timestamp(iso8601.parse("1923-12-01T00:44:36.876").getTime());
    t3.setNanos(876544000);
    assertEquals(-1454368523123456L, TimestampUtil.timestampToMicros(t3));
    assertEquals(t3, TimestampUtil.microsToTimestamp(-1454368523123456L));
  }

  @Test
  public void testNonZuluTimestampConversion() throws Exception {
    SimpleDateFormat cst = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS");
    cst.setTimeZone(TimeZone.getTimeZone("America/Chicago"));

    String timeString = "2016-08-19T12:12:12.121";
    Timestamp timestamp = new Timestamp(cst.parse(timeString).getTime());

    long toMicros = TimestampUtil.timestampToMicros(timestamp);
    Timestamp fromMicros = TimestampUtil.microsToTimestamp(toMicros);
    String formattedCST = cst.format(fromMicros);

    assertEquals(1471626732121000L, toMicros);
    assertEquals(timestamp, fromMicros);
    assertEquals(timeString, formattedCST);

    SimpleDateFormat pst = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS");
    pst.setTimeZone(TimeZone.getTimeZone("America/Los_Angeles"));
    String formattedPST = pst.format(fromMicros);
    assertEquals("2016-08-19T10:12:12.121", formattedPST);

    SimpleDateFormat utc = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS");
    utc.setTimeZone(TimeZone.getTimeZone("UTC"));
    String formattedUTC = utc.format(fromMicros);
    assertEquals("2016-08-19T17:12:12.121", formattedUTC);
  }
}

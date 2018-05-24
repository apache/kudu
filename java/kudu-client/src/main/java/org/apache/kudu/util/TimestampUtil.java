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

import java.sql.Timestamp;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.TimeZone;

@InterfaceAudience.Private
public class TimestampUtil {

  // Thread local DateFormat since they're not thread-safe.
  private static final ThreadLocal<DateFormat> DATE_FORMAT = new ThreadLocal<DateFormat>() {
    @Override
    protected DateFormat initialValue() {
      SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss");
      sdf.setTimeZone(TimeZone.getTimeZone("UTC"));
      return sdf;
    }
  };

  /**
   * Converts a {@link Timestamp} to microseconds since the Unix epoch (1970-01-01T00:00:00Z).
   *
   * Note: Timestamp instances with nanosecond precision are truncated to microseconds.
   *
   * @param timestamp the timestamp to convert to microseconds
   * @return the microseconds since the Unix epoch
   */
  public static long timestampToMicros(Timestamp timestamp) {
    // Number of whole milliseconds since the Unix epoch, in microseconds.
    long millis = timestamp.getTime() * 1000L;
    // Sub millisecond time since the Unix epoch, in microseconds.
    long micros = (timestamp.getNanos() % 1000000L) / 1000L;
    if (micros >= 0) {
      return millis + micros;
    } else {
      return millis + 1000000L + micros;
    }
  }

  /**
   * Converts a microsecond offset from the Unix epoch (1970-01-01T00:00:00Z)
   * to a {@link Timestamp}.
   *
   * @param micros the offset in microseconds since the Unix epoch
   * @return the corresponding timestamp
   */
  public static Timestamp microsToTimestamp(long micros) {
    long millis = micros / 1000L;
    long nanos = (micros % 1000000L) * 1000L;
    if (nanos < 0) {
      millis -= 1L;
      nanos += 1000000000L;
    }
    Timestamp timestamp = new Timestamp(millis);
    timestamp.setNanos((int) nanos);
    return timestamp;
  }

  /**
   * Transforms a timestamp into a string, whose formatting and timezone is consistent
   * across Kudu.
   * @param micros the timestamp, in microseconds
   * @return a string, in the format: YYYY-MM-DDTHH:MM:SS.ssssssZ
   */
  public static String timestampToString(long micros) {
    long tsMillis = micros / 1000L;
    long tsMicros = micros % 1000000L;
    String tsStr = DATE_FORMAT.get().format(new Date(tsMillis));
    return String.format("%s.%06dZ", tsStr, tsMicros);
  }
}

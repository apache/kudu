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

import java.io.Closeable;
import java.io.IOException;

import org.apache.log4j.AppenderSkeleton;
import org.apache.log4j.Layout;
import org.apache.log4j.Logger;
import org.apache.log4j.SimpleLayout;
import org.apache.log4j.spi.LoggingEvent;

import com.google.common.base.Throwables;

/**
 * Test utility which wraps Log4j and captures all messages logged
 * while it is attached. This can be useful for asserting that a particular
 * message is (or is not) logged.
 */
public class CapturingLogAppender extends AppenderSkeleton {
  private StringBuilder appended = new StringBuilder();
  private static final Layout layout = new SimpleLayout();

  @Override
  public void close() {
  }

  @Override
  public boolean requiresLayout() {
    return false;
  }

  @Override
  protected void append(LoggingEvent event) {
    appended.append(layout.format(event));
    if (event.getThrowableInformation() != null) {
      appended.append(Throwables.getStackTraceAsString(
          event.getThrowableInformation().getThrowable())).append("\n");
    }
  }

  public String getAppendedText() {
    return appended.toString();
  }

  /**
   * Temporarily attach the capturing appender to the Log4j root logger.
   * This can be used in a 'try-with-resources' block:
   * <code>
   *   try (Closeable c = capturer.attach()) {
   *     ...
   *   }
   * </code>
   */
  public Closeable attach() {
    Logger.getRootLogger().addAppender(this);
    return new Closeable() {
      @Override
      public void close() throws IOException {
        Logger.getRootLogger().removeAppender(CapturingLogAppender.this);
      }
    };
  }
}

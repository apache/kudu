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

import java.time.Instant;

import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;

/**
 * This class suppresses messages by not allowing more than one message per a number of seconds
 * provided at the call-site of the logging functions. Each instance of this class is designed to
 * throttle regardless of the message.
 * TODO(mreddy): If functionality is ever expanded, use ConcurrentHashMap to store multiple messages
 * and the last time it was logged, only one instance of LogThrottler will be needed per class
 * as this would be used at multiple call-sites to throttle different messages
 * TODO(mreddy): Use integer as hashing key rather than string for performance costs, store integers
 * on file with call-sites, put onus on devs to provide integers for each unique message to throttle
 * TODO(mreddy): Add count to keep track of how many messages have been suppressed
 */
@InterfaceAudience.Private
public class LogThrottler {

  private final Logger log;
  private long lastLoggedTimeSecs = -1;

  public LogThrottler(Logger log) {
    this.log = log;
  }

  /**
   * Throttles the log trace message 'msg' if the last message was logged less than 'seconds' ago
   * @param seconds number of seconds between each desired log message
   * @param msg string message to be logged
   */
  public void trace(long seconds, String msg) {
    if (shouldLog(seconds)) {
      log.trace(msg);
    }
  }

  /**
   * Throttles the log trace message according to specified format and argument if the last
   * message was logged less than 'seconds' ago
   * @param seconds number of seconds between each desired log message
   * @param format format string
   * @param arg argument for format string
   */
  public void trace(long seconds, String format, Object arg) {
    if (shouldLog(seconds)) {
      log.trace(format, arg);
    }
  }

  /**
   * Throttles the log trace message according to specified format and arguments if the last
   * message was logged less than 'seconds' ago
   * @param seconds number of seconds between each desired log message
   * @param format format string
   * @param arg1 first argument for format string
   * @param arg2 second argument for format string
   */
  public void trace(long seconds, String format, Object arg1, Object arg2) {
    if (shouldLog(seconds)) {
      log.trace(format, arg1, arg2);
    }
  }

  /**
   * Throttles the log trace message according to specified format and arguments if the last
   * message was logged less than 'seconds' ago
   * @param seconds number of seconds between each desired log message
   * @param format format string
   * @param arguments list of 3 or more arguments for format string
   */
  public void trace(long seconds, String format, Object... arguments) {
    if (shouldLog(seconds)) {
      log.trace(format, arguments);
    }
  }

  /**
   * Throttles the log warn message 'msg' if the last message was logged less than 'seconds' ago
   * @param seconds number of seconds between each desired log message
   * @param msg string message to be logged
   */
  public void warn(long seconds, String msg) {
    if (shouldLog(seconds)) {
      log.warn(msg);
    }
  }

  /**
   * Throttles the log warn message according to specified format and argument if the last
   * message was logged less than 'seconds' ago
   * @param seconds number of seconds between each desired log message
   * @param format format string
   * @param arg argument for format string
   */
  public void warn(long seconds, String format, Object arg) {
    if (shouldLog(seconds)) {
      log.warn(format, arg);
    }
  }

  /**
   * Throttles the log warn message according to specified format and arguments if the last
   * message was logged less than 'seconds' ago
   * @param seconds number of seconds between each desired log message
   * @param format format string
   * @param arg1 first argument for format string
   * @param arg2 second argument for format string
   */
  public void warn(long seconds, String format, Object arg1, Object arg2) {
    if (shouldLog(seconds)) {
      log.warn(format, arg1, arg2);
    }
  }

  /**
   * Throttles the log warn message according to specified format and arguments if the last
   * message was logged less than 'seconds' ago
   * @param seconds number of seconds between each desired log message
   * @param format format string
   * @param arguments list of 3 or more arguments for format string
   */
  public void warn(long seconds, String format, Object... arguments) {
    if (shouldLog(seconds)) {
      log.warn(format, arguments);
    }
  }

  /**
   * Throttles the log error message 'msg' if the last message was logged less than 'seconds' ago
   * @param seconds number of seconds between each desired log message
   * @param msg string message to be logged
   */
  public void error(long seconds, String msg) {
    if (shouldLog(seconds)) {
      log.error(msg);
    }
  }

  /**
   * Throttles the log error message according to specified format and argument if the last
   * message was logged less than 'seconds' ago
   * @param seconds number of seconds between each desired log message
   * @param format format string
   * @param arg argument for format string
   */
  public void error(long seconds, String format, Object arg) {
    if (shouldLog(seconds)) {
      log.error(format, arg);
    }
  }

  /**
   * Throttles the log error message according to specified format and arguments if the last
   * message was logged less than 'seconds' ago
   * @param seconds number of seconds between each desired log message
   * @param format format string
   * @param arg1 first argument for format string
   * @param arg2 second argument for format string
   */
  public void error(long seconds, String format, Object arg1, Object arg2) {
    if (shouldLog(seconds)) {
      log.error(format, arg1, arg2);
    }
  }

  /**
   * Throttles the log error message according to specified format and arguments if the last
   * message was logged less than 'seconds' ago
   * @param seconds number of seconds between each desired log message
   * @param format format string
   * @param arguments list of 3 or more arguments for format string
   */
  public void error(long seconds, String format, Object... arguments) {
    if (shouldLog(seconds)) {
      log.error(format, arguments);
    }
  }

  /**
   * Throttles the log info message 'msg' if the last message was logged less than 'seconds' ago
   * @param seconds number of seconds between each desired log message
   * @param msg string message to be logged
   */
  public void info(long seconds, String msg) {
    if (shouldLog(seconds)) {
      log.info(msg);
    }
  }

  /**
   * Throttles the log info message according to specified format and argument if the last
   * message was logged less than 'seconds' ago
   * @param seconds number of seconds between each desired log message
   * @param format format string
   * @param arg argument for format string
   */
  public void info(long seconds, String format, Object arg) {
    if (shouldLog(seconds)) {
      log.info(format, arg);
    }
  }

  /**
   * Throttles the log info message according to specified format and arguments if the last
   * message was logged less than 'seconds' ago
   * @param seconds number of seconds between each desired log message
   * @param format format string
   * @param arg1 first argument for format string
   * @param arg2 second argument for format string
   */
  public void info(long seconds, String format, Object arg1, Object arg2) {
    if (shouldLog(seconds)) {
      log.info(format, arg1, arg2);
    }
  }

  /**
   * Throttles the log info message according to specified format and arguments if the last
   * message was logged less than 'seconds' ago
   * @param seconds number of seconds between each desired log message
   * @param format format string
   * @param arguments list of 3 or more arguments for format string
   */
  public void info(long seconds, String format, Object... arguments) {
    if (shouldLog(seconds)) {
      log.info(format, arguments);
    }
  }

  /**
   * Throttles the log debug message 'msg' if the last message was logged less than 'seconds' ago
   * @param seconds number of seconds between each desired log message
   * @param msg string message to be logged
   */
  public void debug(long seconds, String msg) {
    if (shouldLog(seconds)) {
      log.debug(msg);
    }
  }

  /**
   * Throttles the log debug message according to specified format and argument if the last
   * message was logged less than 'seconds' ago
   * @param seconds number of seconds between each desired log message
   * @param format format string
   * @param arg argument for format string
   */
  public void debug(long seconds, String format, Object arg) {
    if (shouldLog(seconds)) {
      log.debug(format, arg);
    }
  }

  /**
   * Throttles the log debug message according to specified format and arguments if the last
   * message was logged less than 'seconds' ago
   * @param seconds number of seconds between each desired log message
   * @param format format string
   * @param arg1 first argument for format string
   * @param arg2 second argument for format string
   */
  public void debug(long seconds, String format, Object arg1, Object arg2) {
    if (shouldLog(seconds)) {
      log.debug(format, arg1, arg2);
    }
  }

  /**
   * Throttles the log debug message according to specified format and arguments if the last
   * message was logged less than 'seconds' ago
   * @param seconds number of seconds between each desired log message
   * @param format format string
   * @param arguments list of 3 or more arguments for format string
   */
  public void debug(long seconds, String format, Object... arguments) {
    if (shouldLog(seconds)) {
      log.debug(format, arguments);
    }
  }

  /**
   * Returns true if first time logging message or it's been more than longer than the parameter
   * duration in seconds indicating to call-site to log the message, returns false to let call-site
   * know not to log the message
   * @param throttlingIntervalSecs number of seconds between each desired log message
   * @return boolean indicating whether or not to log
   */
  private synchronized boolean shouldLog(long throttlingIntervalSecs) {
    long nowSecs = Instant.now().getEpochSecond();
    if (lastLoggedTimeSecs == -1 || lastLoggedTimeSecs + throttlingIntervalSecs < nowSecs) {
      lastLoggedTimeSecs = nowSecs;
      return true;
    }
    return false;
  }
}

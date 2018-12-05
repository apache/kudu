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
package org.apache.kudu.test;

import java.io.BufferedWriter;
import java.io.Closeable;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.util.zip.GZIPOutputStream;

import com.google.common.base.Throwables;
import org.apache.commons.io.IOUtils;
import org.apache.log4j.AppenderSkeleton;
import org.apache.log4j.Layout;
import org.apache.log4j.Logger;
import org.apache.log4j.PatternLayout;
import org.apache.log4j.spi.LoggingEvent;
import org.apache.yetus.audience.InterfaceAudience;
import org.apache.yetus.audience.InterfaceStability;

import static java.nio.charset.StandardCharsets.UTF_8;

/**
 * Test utility which wraps Log4j and captures all messages logged while
 * attached, storing them in an (optionally gzipped) temporary file.
 *
 * The typical lifecycle is as follows:
 *
 * constructor: temporary file is created and opened.
 * append():    a new log event is captured. It may or may not be flushed to disk.
 * finish():    all events previously captured in append() are now guaranteed to
 *              be on disk and visible to readers. No more events may be appended.
 * close():     the temporary file is deleted.
 */
@InterfaceAudience.Private
@InterfaceStability.Unstable
public class CapturingToFileLogAppender extends AppenderSkeleton implements AutoCloseable {
  // This is the standard layout used in Kudu tests.
  private static final Layout LAYOUT = new PatternLayout(
      "%d{HH:mm:ss.SSS} [%p - %t] (%F:%L) %m%n");

  private File outputFile;
  private Writer outputFileWriter;

  /**
   * Creates a new appender. The temporary file is created immediately; it may
   * be obtained via getOutputFile().
   *
   * Appended messages are buffered; they must be flushed to disk via finish().
   *
   * @param useGzip whether to gzip-compress messages when appended
   */
  public CapturingToFileLogAppender(boolean useGzip) throws IOException {
    outputFile = File.createTempFile("captured_output", ".txt.gz");
    try {
      OutputStream os = new FileOutputStream(outputFile.getPath());
      try {
        if (useGzip) {
          os = new GZIPOutputStream(os);
        }

        // As per the recommendation in OutputStreamWriter's Javadoc, we wrap in a
        // BufferedWriter to buffer up character conversions.
        outputFileWriter = new BufferedWriter(new OutputStreamWriter(os, UTF_8));
      } catch (Throwable t) {
        IOUtils.closeQuietly(os);
        throw t;
      }
    } catch (Throwable t) {
      outputFile.delete();
      throw t;
    }
  }

  @Override
  public void close() {
    // Just do the cleanup; we don't care about exceptions/logging.

    if (outputFileWriter != null) {
      IOUtils.closeQuietly(outputFileWriter);
      outputFileWriter = null;
    }
    if (outputFile != null) {
      outputFile.delete();
      outputFile = null;
    }
  }

  @Override
  public boolean requiresLayout() {
    return false;
  }

  @Override
  protected void append(LoggingEvent event) {
    assert outputFileWriter != null;
    try {
      outputFileWriter.write(LAYOUT.format(event));
      if (event.getThrowableInformation() != null) {
        outputFileWriter.write(Throwables.getStackTraceAsString(
            event.getThrowableInformation().getThrowable()));
        outputFileWriter.write("\n");
      }
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  /**
   * Flushes any buffered appended events to the on-disk temporary file and
   * closes it.
   *
   * After calling this function, all appended events will be visible to new
   * readers.
   *
   * @throws IOException if close failed
   */
  public void finish() throws IOException {
    // As per the Writer contract, this will also flush the output stream as
    // well as the compressor (if gzip-compression is used).
    //
    // Why close() and not flush()? It turns out to be remarkably hard to
    // flush a GZIPOutputStream [1]. At the very least it also requires calling
    // finish(), which is not a generic OutputStream method. But for our use
    // case (multiple append() calls followed by a single file access) it's
    // easier to just close() when we're done appending.
    //
    // 1. https://stackoverflow.com/questions/3640080/force-flush-on-a-gzipoutputstream-in-java
    //
    outputFileWriter.close();
    outputFileWriter = null;
  }

  /**
   * @return the temporary file opened in the appender's constructor
   */
  public File getOutputFile() {
    return outputFile;
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
        Logger.getRootLogger().removeAppender(CapturingToFileLogAppender.this);
      }
    };
  }
}

/**
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License. See accompanying LICENSE file.
 */
package org.apache.kudu.test;

import static java.nio.charset.StandardCharsets.UTF_8;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import com.google.common.base.Joiner;

import org.apache.kudu.test.junit.RetryRule;
import org.junit.Rule;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.Closeable;
import java.io.File;
import java.io.FileInputStream;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.IOException;
import java.io.Reader;
import java.util.ArrayList;
import java.util.List;
import java.util.zip.GZIPInputStream;

public class TestCapturingToFileLogAppender {
  private static final Logger LOG =
      LoggerFactory.getLogger(TestCapturingToFileLogAppender.class);

  private static final String MAGIC_STRING = "hello world!";

  @Rule
  public RetryRule retryRule = new RetryRule();

  private String readAllFromBufferedReader(BufferedReader br) throws IOException {
      List<String> output = new ArrayList<>();
      for (String line = br.readLine(); line != null; line = br.readLine()) {
        output.add(line);
      }
      return Joiner.on("\n").join(output);
  }

  private String readAllFromFile(File fileName) throws IOException {
    try (InputStream fis = new FileInputStream(fileName);
         Reader isr = new InputStreamReader(fis, UTF_8);
         BufferedReader br = new BufferedReader(isr)) {
      return readAllFromBufferedReader(br);
    }
  }

  private String readAllFromGzippedFile(File fileName) throws IOException {
    try (InputStream fis = new FileInputStream(fileName);
         InputStream gzis = new GZIPInputStream(fis);
         Reader isr = new InputStreamReader(gzis, UTF_8);
         BufferedReader br = new BufferedReader(isr)) {
      return readAllFromBufferedReader(br);
    }
  }

  @Test
  public void testLog() throws IOException {
    File outputFile;
    try (CapturingToFileLogAppender capturer =
         new CapturingToFileLogAppender(/*useGzip=*/ false)) {
      outputFile = capturer.getOutputFile();
      assertTrue(outputFile.exists());

      // Log a magic string and flush the output file.
      try (Closeable c = capturer.attach()) {
        LOG.info(MAGIC_STRING);
      }
      capturer.finish();

      // Read the magic string out of the output file.
      String captured = readAllFromFile(outputFile);
      assertNotNull(captured);
      assertTrue(captured.contains(MAGIC_STRING));
    }
    assertFalse(outputFile.exists());
  }

  @Test
  public void testLogGzipped() throws IOException {
    File outputFile;
    try (CapturingToFileLogAppender capturer =
         new CapturingToFileLogAppender(/*useGzip=*/ true)) {
      outputFile = capturer.getOutputFile();
      assertTrue(outputFile.exists());

      // Log a magic string and flush the output file.
      try (Closeable c = capturer.attach()) {
        LOG.info(MAGIC_STRING);
      }
      capturer.finish();

      // Read the magic string out of the output file.
      String captured = readAllFromGzippedFile(outputFile);
      assertNotNull(captured);
      assertTrue(captured.contains(MAGIC_STRING));
    }
    assertFalse(outputFile.exists());
  }

  @Test
  public void testLogException() throws IOException {
    File outputFile;
    try (CapturingToFileLogAppender capturer =
         new CapturingToFileLogAppender(/*useGzip=*/ false)) {
      outputFile = capturer.getOutputFile();
      assertTrue(outputFile.exists());

      // Log a magic string and flush the output file.
      try (Closeable c = capturer.attach()) {
        LOG.error("Saw exception", new Exception(MAGIC_STRING));
      }
      capturer.finish();

      // Read the magic string out of the output file.
      String captured = readAllFromFile(outputFile);
      assertNotNull(captured);
      assertTrue(captured.contains("java.lang.Exception: " + MAGIC_STRING));
    }
    assertFalse(outputFile.exists());
  }
}

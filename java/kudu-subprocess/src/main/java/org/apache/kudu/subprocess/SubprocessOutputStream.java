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

package org.apache.kudu.subprocess;

import java.io.IOException;
import java.io.OutputStream;
import java.io.PrintStream;

import com.google.common.annotations.VisibleForTesting;

/**
 * Wrapper around {@link java.io.PrintStream} that throws an
 * <code>IOException</code> instead of relying on explicit
 * <code>checkError</code> calls when writing or flushing to
 * the stream. This makes its error-throwing behavior more
 * similar to most {@link java.io.OutputStream}.
 */
public class SubprocessOutputStream extends OutputStream {
  private final PrintStream out;

  @VisibleForTesting
  public static final String WRITE_ERR = "Unable to write to print stream";
  private static final String FLUSH_ERR = "Unable to flush to print stream";

  public SubprocessOutputStream(PrintStream out) {
    this.out = out;
  }

  @Override
  public void write(int b) throws IOException {
    out.write(b);
    if (out.checkError()) {
      throw new IOException(WRITE_ERR);
    }
  }

  @Override
  public void write(byte buf[], int off, int len) throws IOException {
    out.write(buf, off, len);
    if (out.checkError()) {
      throw new IOException(WRITE_ERR);
    }
  }

  @Override
  public void write(byte b[]) throws IOException {
    out.write(b);
    if (out.checkError()) {
      throw new IOException(WRITE_ERR);
    }
  }

  @Override
  public void flush() throws IOException {
    if (out.checkError()) {
      throw new IOException(FLUSH_ERR);
    }
  }
}

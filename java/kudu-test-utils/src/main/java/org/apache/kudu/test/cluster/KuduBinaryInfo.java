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

package org.apache.kudu.test.cluster;

import static java.nio.charset.StandardCharsets.UTF_8;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;

import com.google.common.base.Joiner;
import com.google.common.collect.Lists;
import org.apache.yetus.audience.InterfaceAudience;
import org.apache.yetus.audience.InterfaceStability;

/**
 * Simple struct to provide various properties of a binary artifact to callers.
 */
@InterfaceAudience.Private
@InterfaceStability.Unstable
public class KuduBinaryInfo {
  private final String binDir;
  private final String saslDir;

  public KuduBinaryInfo(String binDir, String saslDir) {
    this.binDir = binDir;
    this.saslDir = saslDir;
  }

  public KuduBinaryInfo(String binDir) {
    this(binDir, null);
  }

  /**
   * Return the binary directory of an extracted artifact.
   */
  public String getBinDir() {
    return binDir;
  }

  /**
   * Return the SASL module directory of an extracted artifact.
   * May be {@code null} if unknown.
   */
  public String getSaslDir() {
    return saslDir;
  }

  /**
   * The C++ sanitizer type enabled for the kudu CLI binary.
   */
  public enum SanitizerType {
    NONE,
    ASAN,
    TSAN,
  }

  /**
   * @return sanitizer type for the kudu CLI binary.
   */
  public static SanitizerType getSanitizerType() {
    List<String> vs = getBinaryVersionStrings();
    if (vs.size() < 1 || !vs.get(0).startsWith("kudu ")) {
      throw new RuntimeException(String.format(
          "unexpected version output from kudu binary: %s",
          Joiner.on("\n").join(vs)));
    }
    for (String s : vs) {
      if (s.equals("ASAN enabled")) {
        return SanitizerType.ASAN;
      } else if (s.equals("TSAN enabled")) {
        return SanitizerType.TSAN;
      }
    }
    return SanitizerType.NONE;
  }

  /**
   * @return sequence of strings output by 'kudu --version'
   */
  private static List<String> getBinaryVersionStrings() {
    try {
      KuduBinaryLocator.ExecutableInfo exeInfo =
          KuduBinaryLocator.findBinary("kudu");
      ProcessBuilder pb = new ProcessBuilder(
          Lists.newArrayList(exeInfo.exePath(), "--version"));
      pb.environment().putAll(exeInfo.environment());
      pb.redirectError(ProcessBuilder.Redirect.INHERIT);
      final Process p = pb.start();
      List<String> result = new ArrayList<>();
      try (InputStreamReader isr = new InputStreamReader(p.getInputStream(), UTF_8);
           BufferedReader br = new BufferedReader(isr)) {
        while (true) {
          String line = br.readLine();
          if (line == null) {
            break;
          }
          result.add(line);
        }
      }
      final int exitCode = p.waitFor();
      if (exitCode != 0) {
        // Don't bother reporting the contents of stderr: it should be in the
        // log of the parent process due to the stderr redirection.
        throw new RuntimeException(String.format(
            "unexpected exit code from kudu binary: %d", exitCode));
      }
      return result;
    } catch (IOException e) {
      throw new RuntimeException(
          "unexpected exception while trying to run kudu binary", e);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new RuntimeException(
          "unexpected exception while trying to run kudu binary", e);
    }
  }
}

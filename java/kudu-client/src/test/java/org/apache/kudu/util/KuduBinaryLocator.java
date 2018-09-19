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

import com.google.common.io.CharStreams;
import org.apache.yetus.audience.InterfaceAudience;
import org.apache.yetus.audience.InterfaceStability;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.Reader;

import static java.nio.charset.StandardCharsets.UTF_8;

@InterfaceAudience.Private
@InterfaceStability.Unstable
public class KuduBinaryLocator {
  private static final Logger LOG = LoggerFactory.getLogger(KuduBinaryLocator.class);

  private static final String KUDU_BIN_DIR_PROP = "kuduBinDir";

  /**
   * Find the binary directory within the build tree.
   *
   * Uses the following priority:
   *    - If kuduBinDir system property is set, use that.
   *    - If the `kudu` binary is found on the PATH using `which kudu`,
   *      use its parent directory.
   */
  private static String findBinaryDir() {
    // If kuduBinDir system property is set, use that.
    String kuduBinDirProp = System.getProperty(KUDU_BIN_DIR_PROP);
    if (kuduBinDirProp != null) {
      LOG.info("Using Kudu binary directory specified by system property '{}': {}",
          KUDU_BIN_DIR_PROP, kuduBinDirProp);
      return kuduBinDirProp;
    }

    // If the `kudu` binary is found on the PATH using `which kudu`, use its parent directory.
    try {
      Runtime runtime = Runtime.getRuntime();
      Process process = runtime.exec("which kudu");
      int errorCode = process.waitFor();
      if (errorCode == 0) {
        try(Reader reader = new InputStreamReader(process.getInputStream(), UTF_8)) {
          String kuduBinary = CharStreams.toString(reader);
          String kuduBinDir = new File(kuduBinary).getParent();
          LOG.info("Using Kudu binary directory found on path with 'which kudu': {}", kuduBinDir);
          return kuduBinDir;
        }
      }
    } catch (IOException | InterruptedException ex) {
      throw new RuntimeException("Error while locating kudu binary", ex);
    }

    throw new RuntimeException("Could not locate the kudu binary directory. " +
        "Set the system variable " + KUDU_BIN_DIR_PROP +
        " or ensure the `kudu` binary is on your path.");
  }

  /**
   * @param binName the binary to look for (eg 'kudu-tserver')
   * @return the absolute path of that binary
   * @throws FileNotFoundException if no such binary is found
   */
  public static String findBinary(String binName) throws FileNotFoundException {
    String binDir = findBinaryDir();

    File candidate = new File(binDir, binName);
    if (candidate.canExecute()) {
      return candidate.getAbsolutePath();
    }
    throw new FileNotFoundException("Cannot find binary " + binName +
        " in binary directory " + binDir);
  }
}

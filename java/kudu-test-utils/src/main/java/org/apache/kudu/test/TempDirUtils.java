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

import org.apache.commons.io.FileUtils;
import org.apache.yetus.audience.InterfaceAudience;
import org.apache.yetus.audience.InterfaceStability;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

/**
 * Utilities for retrieving and creating temp directories.
 */
@InterfaceAudience.Private
@InterfaceStability.Unstable
public class TempDirUtils {

  /**
   * An enum to control whether a temporary directory created by
   * {@link #makeTempDirectory(String, DeleteOnExit)} is recursively deleted on JVM exit,
   * including the contents of the directory.
   */
  public enum DeleteOnExit {
    /** Do not delete the directory on exit. */
    NO_DELETE_ON_EXIT,
    /** Recursively delete the directory and its contents on exit. */
    DELETE_RECURSIVELY_ON_EXIT,
  }

  private static final Logger LOG = LoggerFactory.getLogger(TempDirUtils.class);

  /**
   * Match the C++ MiniCluster test functionality for overriding the tmp directory used.
   * See MakeClusterRoot in src/kudu/tools/tool_action_test.cc.
   * If the TEST_TMPDIR environment variable is defined that directory will be used
   * instead of the default temp directory.
   *
   * @param prefix a directory name to be created, in environment variable TEST_TMPDIR if defined,
   *               else within the java.io.tmpdir system property
   * @param deleteRecursivelyOnExit whether to recursively delete the directory and all its
   *                                contents on JVM exit
   * @return temp directory as a file
   * @throws IOException if a temp directory cannot be created
   */
  public static File makeTempDirectory(String prefix, DeleteOnExit deleteRecursivelyOnExit)
      throws IOException {
    String testTmpdir = System.getenv("TEST_TMPDIR");
    File newDir;
    if (testTmpdir != null) {
      LOG.info("Using the temp directory defined by TEST_TMPDIR: " + testTmpdir);
      newDir = Files.createTempDirectory(Paths.get(testTmpdir), prefix).toFile();
    } else {
      newDir = Files.createTempDirectory(prefix).toFile();
    }
    if (deleteRecursivelyOnExit == DeleteOnExit.DELETE_RECURSIVELY_ON_EXIT) {
      registerToRecursivelyDeleteOnShutdown(newDir.toPath());
    }
    return newDir;
  }

  /**
   * Register a JVM shutdown hook to recursively delete the specified directory on JVM shutdown.
   * @param path directory to delete on shutdown
   */
  private static void registerToRecursivelyDeleteOnShutdown(Path path) {
    final Path absPath = path.toAbsolutePath();
    Runtime.getRuntime().addShutdownHook(new Thread() {
      @Override
      public void run() {
        File dir = absPath.toFile();
        if (!dir.exists()) return;
        try {
          FileUtils.deleteDirectory(dir);
        } catch (IOException exc) {
          LOG.warn("Unable to remove directory tree " + absPath.toString(), exc);
        }
      }
    });
  }
}

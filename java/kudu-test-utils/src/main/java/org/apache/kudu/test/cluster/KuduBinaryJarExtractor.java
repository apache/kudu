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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.file.FileSystem;
import java.nio.file.FileSystems;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.StandardCopyOption;
import java.nio.file.attribute.BasicFileAttributes;
import java.nio.file.attribute.PosixFilePermission;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.Properties;
import java.util.Set;

/**
 * Class to find and extract Kudu binary jars from the classpath
 */
public class KuduBinaryJarExtractor {

  private static final Logger LOG = LoggerFactory.getLogger(KuduBinaryJarExtractor.class);
  private static final String KUDU_TEST_BIN_PROPS_PATH =
      "META-INF/apache-kudu-test-binary.properties";

  protected Properties binaryProps;

  public KuduBinaryJarExtractor() throws IOException {
    this.binaryProps = getBinaryProps();
  }

  private static Properties getBinaryProps() throws IOException {
    Enumeration<URL> resources =
        KuduBinaryJarExtractor.class.getClassLoader().getResources(KUDU_TEST_BIN_PROPS_PATH);
    //TODO: normalize osName
    //TODO: check for matching architecture
    String osName = System.getProperties().getProperty("os.name").toLowerCase().replace(" ", "");
    while (resources.hasMoreElements()) {
      URL url = resources.nextElement();
      try {
        Properties props = loadBinaryProps(url);
        if (osName.startsWith(props.getProperty("artifact.os"))) {
          return props;
        }
      } catch (IOException ex) {
        LOG.warn("Unable to parse properties file from Kudu binary artifact", ex);
      }
    }
    return null;
  }

  private static Properties loadBinaryProps(URL url) throws IOException {
    Properties props = new Properties();
    props.load(url.openStream());
    return props;
  }

  /**
   * Determine if the classpath has a Kudu binary jar that matches the system's OS and CPU
   * architecture
   *
   * @return true if an appropriate Kudu binary jar is available, false otherwise
   */
  public boolean isKuduBinaryJarOnClasspath() {
    return null != binaryProps;
  }

  /**
   * Extract the Kudu binary jar found on the classpath.
   *
   * @param destDir path to a destination
   * @return the absolute path to the directory containing extracted executables.
   * eg. "/tmp/apache-kudu-1.9.0/bin"
   * @throws IOException if any of the JAR extraction process throws an exception
   */
  public String extractKuduBinary(String destDir) throws IOException {
    if (binaryProps == null) {
      throw new IllegalStateException("Could not locate the Kudu binary jar");
    }

    try {
      String prefix = binaryProps.getProperty("artifact.prefix");
      URL kuduBinDir = KuduBinaryJarExtractor.class.getClassLoader().getResource(prefix);
      if (null == kuduBinDir) {
        throw new FileNotFoundException("Cannot find Kudu binary dir: " + prefix);
      }

      final Path target = Paths.get(destDir);
      return extractJar(Paths.get(kuduBinDir.toURI()), target, prefix).toString();
    } catch (URISyntaxException e) {
      throw new IOException("Cannot unpack Kudu binary jar", e);
    }
  }

  /**
   * Accessible for testing only.
   */
  static Path extractJar(Path src, final Path target, String prefix) throws IOException {
    if (Files.notExists(target)) {
      Files.createDirectory(target);
    }
    URI srcJar = URI.create("jar:" + src.toUri().toString());
    try (FileSystem zipFileSystem =
             FileSystems.newFileSystem(srcJar, new HashMap<String, String>())) {

      Path root = zipFileSystem.getPath(prefix);
      Files.walkFileTree(root, new SimpleFileVisitor<Path>() {

        @Override
        public FileVisitResult preVisitDirectory(Path dir, BasicFileAttributes attributes)
            throws IOException {
          Path newDir = Paths.get(target.toString(), dir.toString());
          Files.copy(dir, newDir, StandardCopyOption.REPLACE_EXISTING);
          return FileVisitResult.CONTINUE;
        }

        @Override
        public FileVisitResult visitFile(Path file, BasicFileAttributes attributes)
            throws IOException {
          Path newFile = Paths.get(target.toString(), file.toString());
          Files.copy(file, newFile, StandardCopyOption.REPLACE_EXISTING);

          if (file.getParent().endsWith("bin")) {
            Set<PosixFilePermission> perms = Files.getPosixFilePermissions(newFile);
            perms.add(PosixFilePermission.OWNER_EXECUTE);
            Files.setPosixFilePermissions(newFile, perms);
          }
          return FileVisitResult.CONTINUE;
        }
      });
    }
    return Paths.get(target.toString(), prefix, "bin");
  }
}

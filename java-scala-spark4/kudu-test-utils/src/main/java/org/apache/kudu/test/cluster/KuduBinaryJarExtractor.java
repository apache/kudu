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
import java.util.Map;
import java.util.Properties;
import java.util.Set;

import com.google.common.base.Preconditions;
import com.google.gradle.osdetector.OsDetector;
import org.apache.yetus.audience.InterfaceAudience;
import org.apache.yetus.audience.InterfaceStability;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Class to find and extract Kudu binary jars from the classpath
 */
@InterfaceAudience.Private
@InterfaceStability.Unstable
public class KuduBinaryJarExtractor {

  private static final Logger LOG = LoggerFactory.getLogger(KuduBinaryJarExtractor.class);
  private static final String KUDU_TEST_BIN_PROPS_PATH =
      "META-INF/apache-kudu-test-binary.properties";
  private static final OsDetector DETECTOR = new OsDetector();

  /** Return the thread context classloader or the parent classloader for this class. */
  private static ClassLoader getCurrentClassLoader() {
    ClassLoader loader = Thread.currentThread().getContextClassLoader();
    if (loader != null) {
      return loader;
    }
    return KuduBinaryJarExtractor.class.getClassLoader();
  }

  private static Properties getBinaryProps() throws IOException {
    Enumeration<URL> resources = getCurrentClassLoader().getResources(KUDU_TEST_BIN_PROPS_PATH);
    while (resources.hasMoreElements()) {
      URL url = resources.nextElement();
      try {
        Properties props = loadBinaryProps(url);
        if (DETECTOR.getOs().equals(props.getProperty("artifact.os")) &&
              DETECTOR.getArch().equals(props.getProperty("artifact.arch"))) {
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
   * Determine if the classpath has a Kudu binary test jar compatible with the system architecture
   * and operating system.
   * If a Thread context ClassLoader is set, then that ClassLoader is searched.
   * Otherwise, the ClassLoader that loaded this class is searched.
   *
   * <p>TODO: at the time of writing, OS and architecture checks are not yet implemented.
   *
   * @return {@code true} if an appropriate Kudu binary jar is available, {@code false} otherwise
   */
  public boolean isKuduBinaryJarOnClasspath() throws IOException {
    Properties binaryProps = getBinaryProps();
    return binaryProps != null;
  }

  /**
   * Extract the Kudu binary test jar found on the classpath to the specified location.
   * If a Thread context ClassLoader is set, then that ClassLoader is searched.
   * Otherwise, the ClassLoader that loaded this class is searched.
   *
   * <p>It is expected that
   * {@link #isKuduBinaryJarOnClasspath()} should return {@code true} before this method is invoked.
   *
   * @param destDir path to a destination
   * @return information about the extracted artifact
   * @throws FileNotFoundException if the binary JAR cannot not be located
   * @throws IOException if the JAR extraction process fails
   */
  public KuduBinaryInfo extractKuduBinaryArtifact(String destDir) throws IOException {
    Properties binaryProps = getBinaryProps();
    if (binaryProps == null) {
      throw new FileNotFoundException("Could not locate the Kudu binary test jar");
    }

    String prefix = binaryProps.getProperty("artifact.prefix");
    URL artifactPrefix = getCurrentClassLoader().getResource(prefix);
    if (artifactPrefix == null) {
      throw new FileNotFoundException("Cannot find Kudu artifact prefix dir: " + prefix);
    }

    try {
      Path artifactRoot = extractJar(artifactPrefix.toURI(), prefix, Paths.get(destDir));
      Path binDir = Paths.get(artifactRoot.toString(), "bin");
      if (!binDir.toFile().exists()) {
        throw new FileNotFoundException("Cannot find Kudu artifact bin dir: " + binDir.toString());
      }

      // Only set the saslDir property if we find it in the artifact, since that affects whether
      // the caller needs to set SASL_PATH when executing the binaries.
      Path saslDir = Paths.get(artifactRoot.toString(), "lib", "sasl2");
      String saslDirString = null;
      if (saslDir.toFile().exists()) {
        saslDirString = saslDir.toAbsolutePath().toString();
      }

      return new KuduBinaryInfo(binDir.toString(), saslDirString);
    } catch (URISyntaxException e) {
      throw new IOException("Cannot unpack Kudu binary jar", e);
    }
  }

  /**
   * Extracts the given prefix of the given jar into the target directory.
   * Accessible for testing only.
   * @param src URI of the source jar
   * @param prefix prefix of the jar to extract into the destination directory
   * @param target destination directory
   * @return an absolute path to the extracted artifact, including the prefix portion
   */
  static Path extractJar(URI src, String prefix, final Path target) throws IOException {
    Preconditions.checkArgument("jar".equals(src.getScheme()), "src URI must use a 'jar' scheme");
    if (Files.notExists(target)) {
      Files.createDirectory(target);
    }

    Map<String, String> env = new HashMap<>();
    try (FileSystem zipFileSystem = FileSystems.newFileSystem(src, env)) {

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

          Path parent = file.getParent();
          if (parent != null && parent.endsWith("bin")) {
            Set<PosixFilePermission> perms = Files.getPosixFilePermissions(newFile);
            perms.add(PosixFilePermission.OWNER_EXECUTE);
            Files.setPosixFilePermissions(newFile, perms);
          }
          return FileVisitResult.CONTINUE;
        }
      });
    }
    return Paths.get(target.toString(), prefix).toAbsolutePath();
  }
}

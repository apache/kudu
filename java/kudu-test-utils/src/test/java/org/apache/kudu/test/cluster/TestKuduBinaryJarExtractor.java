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

import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.net.URLClassLoader;
import java.nio.file.FileSystem;
import java.nio.file.FileSystems;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.StandardCopyOption;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

public class TestKuduBinaryJarExtractor {

  private static final Logger LOG = LoggerFactory.getLogger(TestKuduBinaryJarExtractor.class);

  private Path createKuduBinaryJar() throws IOException, URISyntaxException {
    Path tempDir = Files.createTempDirectory("fake-kudu-binary-jar");

    // convert the filename to a URI
    final Path path = Paths.get(tempDir.toString(), "fake-kudu-binary.jar");
    LOG.info("Creating fake kudu binary jar at {}", path.toString());
    final URI uri = URI.create("jar:file:" + path.toUri().getPath());

    final Map<String, String> env = new HashMap<>();
    env.put("create", "true");
    final FileSystem zipFs = FileSystems.newFileSystem(uri, env);

    final Path root = zipFs.getPath("/");
    final Path src =
        Paths.get(TestKuduBinaryJarExtractor.class.getResource("/fake-kudu-binary").toURI());

    Files.walkFileTree(src, new SimpleFileVisitor<Path>() {
      @Override
      public FileVisitResult visitFile(Path file,
                                       BasicFileAttributes attrs) throws IOException {
        final Path dest = zipFs.getPath(root.toString(),
            src.relativize(file).toString());
        Files.copy(file, dest, StandardCopyOption.REPLACE_EXISTING);
        return FileVisitResult.CONTINUE;
      }

      @Override
      public FileVisitResult preVisitDirectory(Path dir,
                                               BasicFileAttributes attrs) throws IOException {
        final Path dirToCreate = zipFs.getPath(root.toString(),
            src.relativize(dir).toString());
        if (Files.notExists(dirToCreate)) {
          System.out.printf("Creating directory %s\n", dirToCreate);
          Files.createDirectories(dirToCreate);
        }
        return FileVisitResult.CONTINUE;
      }
    });
    zipFs.close();
    return path;
  }

  /**
   * Create a ClassLoader. The parent of the ClassLoader will be the current thread context
   * ClassLoader, if not set, or the ClassLoader that loaded this test class if not.
   * @param jars an array of jars to include in the child class loader.
   */
  private ClassLoader createChildClassLoader(URL[] jars) {
    ClassLoader parent = Thread.currentThread().getContextClassLoader();
    if (parent == null) {
      parent = TestKuduBinaryJarExtractor.class.getClassLoader();
    }
    assertNotNull(parent);
    return URLClassLoader.newInstance(jars, parent);
  }

  @Test
  public void testExtractJar() throws IOException, URISyntaxException {
    Path binaryJar = createKuduBinaryJar();

    Path extractedBinDir =
        KuduBinaryJarExtractor.extractJar(binaryJar,
            Files.createTempDirectory("kudu-test"),
            "apache-kudu-1.9.0-SNAPSHOT");
    assertNotNull(extractedBinDir);

    Path kuduTserver = Paths.get(extractedBinDir.toString(), "kudu-tserver");
    assertTrue(Files.exists(kuduTserver));
  }

  @Test
  public void testIsKuduBinaryJarOnClasspath() throws IOException, URISyntaxException {
    KuduBinaryJarExtractor extractor = new KuduBinaryJarExtractor();
    assertFalse(extractor.isKuduBinaryJarOnClasspath());
    Path binaryJar = createKuduBinaryJar();
    ClassLoader childLoader = createChildClassLoader(new URL[] { binaryJar.toUri().toURL() });
    Thread.currentThread().setContextClassLoader(childLoader);
    assertTrue(extractor.isKuduBinaryJarOnClasspath());
  }
}

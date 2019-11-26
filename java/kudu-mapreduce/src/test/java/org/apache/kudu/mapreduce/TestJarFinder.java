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

package org.apache.kudu.mapreduce;

import static java.nio.charset.StandardCharsets.UTF_8;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.OutputStream;
import java.io.Writer;
import java.nio.file.Files;
import java.util.Properties;
import java.util.jar.JarInputStream;
import java.util.jar.JarOutputStream;
import java.util.jar.Manifest;

import org.apache.commons.io.FileUtils;
import org.apache.commons.logging.LogFactory;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import org.apache.kudu.test.junit.RetryRule;

/**
 * This file was forked from hbase/branches/master@4ce6f48.
 */
public class TestJarFinder {

  private File testDir;

  @Rule
  public RetryRule retryRule = new RetryRule();

  @Before
  public void setUp() throws Exception {
    testDir = Files.createTempDirectory("test-dir").toFile();
    System.setProperty(JarFinder.FILE_DIR_PROPERTY, testDir.getAbsolutePath());
  }

  @After
  public void tearDown() throws Exception {
    if (testDir != null) {
      FileUtils.deleteDirectory(testDir);
    }
  }

  @Test
  public void testJar() throws Exception {
    // Picking a class that is for sure in a JAR in the classpath
    String jar = JarFinder.getJar(LogFactory.class);
    Assert.assertTrue(new File(jar).exists());
  }

  @Test
  public void testExpandedClasspath() throws Exception {
    // Picking a class that is for sure in a directory in the classpath
    // In this case, the JAR is created on the fly
    String jar = JarFinder.getJar(TestJarFinder.class);
    Assert.assertTrue(new File(jar).exists());
  }

  @Test
  public void testExistingManifest() throws Exception {
    File dir = new File(testDir, TestJarFinder.class.getName() + "-testExistingManifest");
    Assert.assertTrue(dir.mkdirs());
    writeManifest(dir);
    Assert.assertNotNull(getManifest(dir));
  }

  @Test
  public void testNoManifest() throws Exception {
    File dir = new File(testDir, TestJarFinder.class.getName() + "-testNoManifest");
    Assert.assertTrue(dir.mkdirs());
    Assert.assertNotNull(getManifest(dir));
  }

  private void writeManifest(File dir) throws Exception {
    File metaInfDir = new File(dir, "META-INF");
    Assert.assertTrue(metaInfDir.mkdirs());
    File manifestFile = new File(metaInfDir, "MANIFEST.MF");
    Manifest manifest = new Manifest();
    try (OutputStream os = new FileOutputStream(manifestFile)) {
      manifest.write(os);
    }
  }

  private Manifest getManifest(File dir) throws Exception {
    File propsFile = new File(dir, "props.properties");
    try (Writer writer = Files.newBufferedWriter(propsFile.toPath(), UTF_8)) {
      new Properties().store(writer, "");
    }
    try (ByteArrayOutputStream baos = new ByteArrayOutputStream();
         JarOutputStream zos = new JarOutputStream(baos)) {
      JarFinder.jarDir(dir, "", zos);
      try (JarInputStream jis = new JarInputStream(new ByteArrayInputStream(baos.toByteArray()))) {
        return jis.getManifest();
      }
    }
  }
}

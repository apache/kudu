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
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * This file was forked from hbase/branches/master@4ce6f48.
 */
public class TestJarFinder {

  private static File testDir;

  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    testDir = Files.createTempDirectory("test-dir").toFile();
    System.setProperty(JarFinder.FILE_DIR_PROPERTY, testDir.getAbsolutePath());
  }

  @AfterClass
  public static void tearDownAfterClass() throws Exception {
    FileUtils.deleteDirectory(testDir);
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
    File dir = new File(testDir,
      TestJarFinder.class.getName() + "-testExistingManifest");
    File metaInfDir = new File(dir, "META-INF");
    metaInfDir.mkdirs();
    File manifestFile = new File(metaInfDir, "MANIFEST.MF");
    Manifest manifest = new Manifest();
    OutputStream os = new FileOutputStream(manifestFile);
    manifest.write(os);
    os.close();

    File propsFile = new File(dir, "props.properties");
    Writer writer = Files.newBufferedWriter(propsFile.toPath(), UTF_8);
    new Properties().store(writer, "");
    writer.close();
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    JarOutputStream zos = new JarOutputStream(baos);
    JarFinder.jarDir(dir, "", zos);
    JarInputStream jis =
      new JarInputStream(new ByteArrayInputStream(baos.toByteArray()));
    Assert.assertNotNull(jis.getManifest());
    jis.close();
  }

  @Test
  public void testNoManifest() throws Exception {
    File dir = new File(testDir,
      TestJarFinder.class.getName() + "-testNoManifest");
    dir.mkdirs();
    File propsFile = new File(dir, "props.properties");
    Writer writer = Files.newBufferedWriter(propsFile.toPath(), UTF_8);
    new Properties().store(writer, "");
    writer.close();
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    JarOutputStream zos = new JarOutputStream(baos);
    JarFinder.jarDir(dir, "", zos);
    JarInputStream jis =
      new JarInputStream(new ByteArrayInputStream(baos.toByteArray()));
    Assert.assertNotNull(jis.getManifest());
    jis.close();
  }
}
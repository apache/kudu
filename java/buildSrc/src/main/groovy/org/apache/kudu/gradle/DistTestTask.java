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
package org.apache.kudu.gradle;

import org.gradle.api.DefaultTask;
import org.gradle.api.file.FileCollection;
import org.gradle.api.file.FileTree;
import org.gradle.api.internal.tasks.testing.TestClassProcessor;
import org.gradle.api.internal.tasks.testing.TestClassRunInfo;
import org.gradle.api.internal.tasks.testing.TestResultProcessor;
import org.gradle.api.internal.tasks.testing.detection.DefaultTestClassScanner;
import org.gradle.api.internal.tasks.testing.detection.TestFrameworkDetector;
import org.gradle.api.logging.Logger;
import org.gradle.api.logging.Logging;
import org.gradle.api.tasks.Input;
import org.gradle.api.tasks.InputFiles;
import org.gradle.api.tasks.OutputDirectory;
import org.gradle.api.tasks.TaskAction;
import org.gradle.api.tasks.options.Option;
import org.gradle.api.tasks.testing.Test;

import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.io.Files;
import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import com.google.gson.GsonBuilder;

import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.InputStream;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static java.nio.charset.StandardCharsets.UTF_8;

/**
 * This task is used in our top build.gradle file. It is called
 * by dist_test.py to generate the needed .isolate and .gen.json
 * files needed to run the distributed tests.
 */
public class DistTestTask extends DefaultTask {
  private static final Logger LOGGER = Logging.getLogger(DistTestTask.class);

  private static final Gson GSON = new GsonBuilder()
      .setPrettyPrinting()
      .create();

  String distTestBin = getProject().getRootDir() + "/../build-support/dist_test.py";

  @OutputDirectory
  File outputDir = new File(getProject().getBuildDir(), "dist-test");

  private List<Test> testTasks = Lists.newArrayList();

  private boolean collectTmpDir = false;

  /**
   * Called by build.gradle to add test tasks to be considered for dist-tests.
   */
  public void addTestTask(Test t) {
    testTasks.add(t);
  }

  @Option(option = "classes",
          description = "Sets test class to be included, '*' is supported.")
  public DistTestTask setClassPattern(List<String> classPattern) {
    for (Test t : testTasks) {
      // TODO: this requires a glob like **/*Foo* instead of just *Foo*
      t.setIncludes(classPattern);
    }
    return this;
  }

  /**
   * Not actually used, but gradle mandates that the @Input annotation be placed
   * on a getter, and we need @Input so that the task is rerun if the value of
   * the 'collect-tmpdir' option changes.
   */
  @Input
  public boolean getCollectTmpDir() {
    return collectTmpDir;
  }

  @Option(option = "collect-tmpdir",
          description = "Archives the test's temp directory as an artifact if the test fails.")
  public DistTestTask setCollectTmpdir() {
    collectTmpDir = true;
    return this;
  }

  @InputFiles
  public FileCollection getInputClasses() {
    FileCollection fc = getProject().files(); // Create an empty FileCollection.
    for (Test t : testTasks) {
      fc = fc.plus(t.getCandidateClassFiles());
    }
    return fc;
  }

  @TaskAction
  public void doStuff() throws IOException {
    getProject().delete(outputDir);
    getProject().mkdir(outputDir);
    List<String> baseDeps = getBaseDeps();
    for (Test t : testTasks) {
      List<String> testClassNames = collectTestNames(t);
      for (String c : testClassNames) {
        File isolateFile = new File(outputDir, c + ".isolate");
        File isolatedFile = new File(outputDir, c + ".isolated");
        File genJsonFile = new File(outputDir, c + ".gen.json");

        Files.write(genIsolate(outputDir.toPath(), t, c, baseDeps), isolateFile, UTF_8);

        // Write the gen.json
        GenJson gen = new GenJson();
        gen.args = ImmutableList.of(
            "-i", isolateFile.toString(),
            "-s", isolatedFile.toString());
        gen.dir = outputDir.toString();
        gen.name = c;
        Files.write(GSON.toJson(gen), genJsonFile, UTF_8);
      }
    }
  }

  /**
   * Calls dist_test.py to get the c++ "base" dependencies so that we can
   * include them in the .isolate files.
   *
   * Note: This currently fails OSX because dump_base_deps use ldd.
   */
  private List<String> getBaseDeps() throws IOException {
    Process proc = new ProcessBuilder(distTestBin,
        "internal",
        "dump_base_deps")
        .redirectError(ProcessBuilder.Redirect.INHERIT)
        .start();

    try (InputStream is = proc.getInputStream()) {
      return new Gson().fromJson(new InputStreamReader(is, UTF_8),
          new TypeToken<List<String>>(){}.getType());
    }
  }

  /**
   * @return all test result reporting environment variables and their values,
   *         in a format suitable for consumption by run_dist_test.py.
   */
  private List<String> getTestResultReportingEnvironmentVariables() {
    ImmutableList.Builder<String> args = new ImmutableList.Builder<>();
    String enabled = System.getenv("KUDU_REPORT_TEST_RESULTS");
    if (enabled != null && Integer.parseInt(enabled) > 0) {
      for (String ev : ImmutableList.of("KUDU_REPORT_TEST_RESULTS",
                                        "BUILD_CONFIG",
                                        "BUILD_TAG",
                                        "GIT_REVISION",
                                        "TEST_RESULT_SERVER")) {
        String evValue = System.getenv(ev);
        if (evValue == null || evValue.isEmpty()) {
          if (ev.equals("TEST_RESULT_SERVER")) {
            // This one is optional.
            continue;
          }
          throw new RuntimeException(
              String.format("Required env variable %s is missing", ev));
        }
        args.add("-e");
        args.add(String.format("%s=%s", ev, evValue));
      }
    }
    return args.build();
  }

  private String genIsolate(Path isolateFileDir, Test test, String testClass,
                            List<String> baseDeps) throws IOException {
    Path rootDir = test.getProject().getRootDir().toPath();
    Path binDir = rootDir.resolve("../build/latest/bin").toRealPath();
    Path buildSupportDir = rootDir.resolve("../build-support").toRealPath();
    Path buildDir = rootDir.resolve("build");
    File jarDir = buildDir.resolve("jars").toFile();

    // Build classpath with relative paths.
    List<String> classpath = Lists.newArrayList();
    for (File f : test.getClasspath().getFiles()) {
      File projectFile = f;
      // This hack changes the path to dependent jars from the gradle cache
      // in ~/.gradle/caches/... to a path to the jars copied under the project
      // build directory. See the copyDistTestJars task in build.gradle to see
      // the copy details.
      if (projectFile.getAbsolutePath().contains(".gradle/caches/")) {
        projectFile = new File(jarDir, projectFile.getName());
      }

      String s = isolateFileDir.relativize(projectFile.toPath().toAbsolutePath()).toString();
      // Isolate requires that directories be listed with a trailing '/'.
      if (projectFile.isDirectory()) {
        s += "/";
      }
      // Gradle puts resources directories into the classpath even if they don't exist.
      // isolate is unhappy with non-existent paths, though.
      if (projectFile.exists()) {
        classpath.add(s);
      }
    }

    // Build up the actual Java command line to run the test.
    ImmutableList.Builder<String> cmd = new ImmutableList.Builder<>();
    cmd.add(isolateFileDir.relativize(buildSupportDir.resolve("run_dist_test.py")).toString());
    if (collectTmpDir) {
      cmd.add("--collect-tmpdir");
    }
    cmd.add("--test-language=java");
    cmd.addAll(getTestResultReportingEnvironmentVariables());
    cmd.add("--",
            "-ea",
            "-cp",
            Joiner.on(":").join(classpath));
    for (Map.Entry<String, Object> e : test.getSystemProperties().entrySet()) {
      cmd.add("-D" + e.getKey() + "=" + e.getValue());
    }
    cmd.add("-DkuduBinDir=" + isolateFileDir.relativize(binDir),
            "org.junit.runner.JUnitCore",
            testClass);

    // Output the actual JSON.
    IsolateFileJson isolate = new IsolateFileJson();
    isolate.variables.command = cmd.build();
    isolate.variables.files.addAll(classpath);
    for (String s : baseDeps) {
      File f = new File(s);
      String path = isolateFileDir.relativize(f.toPath().toAbsolutePath()).toString();
      if (f.isDirectory()) {
        path += "/";
      }
      isolate.variables.files.add(path);
    }

    String json = isolate.toJson();

    // '.isolate' files are actually Python syntax, rather than true JSON.
    // However, the two are close enough that just doing this replacement
    // tends to work (we're assuming that no one has a quote character in a
    // file path or system property.
    return json.replace('"', '\'');
  }

  // This is internal API but required to get the filtered list of test classes and process them.
  // See the gradle code here which was used for reference:
  // https://github.com/gradle/gradle/blob/c2067eaa129af4c9c29ad08da39d1c853eec4c59/subprojects/testing-jvm/src/main/java/org/gradle/api/internal/tasks/testing/detection/DefaultTestExecuter.java#L104-L112
  private List<String> collectTestNames(Test testTask) {
    ClassNameCollectingProcessor processor = new ClassNameCollectingProcessor();
    Runnable detector;
    final FileTree testClassFiles = testTask.getCandidateClassFiles();
    if (testTask.isScanForTestClasses()) {
      TestFrameworkDetector testFrameworkDetector = testTask.getTestFramework().getDetector();
      testFrameworkDetector.setTestClasses(testTask.getTestClassesDirs().getFiles());
      testFrameworkDetector.setTestClasspath(testTask.getClasspath().getFiles());
      detector = new DefaultTestClassScanner(testClassFiles, testFrameworkDetector, processor);
    } else {
      detector = new DefaultTestClassScanner(testClassFiles, null, processor);
    }
    detector.run();
    LOGGER.debug("collected test class names: {}", processor.classNames);
    return processor.classNames;
  }

  private static class ClassNameCollectingProcessor implements TestClassProcessor {
    public List<String> classNames = new ArrayList<String>();

    @Override
    public void startProcessing(TestResultProcessor testResultProcessor) {
      // no-op
    }

    @Override
    public void processTestClass(TestClassRunInfo testClassRunInfo) {
      classNames.add(testClassRunInfo.getTestClassName());
    }

    @Override
    public void stop() {
      // no-op
    }

    @Override
    public void stopNow() {
      // no-op
    }
  }

  /**
   * Structured to generate Json that matches the expected .isolate format.
   * See here for a description of the .isolate format:
   *   https://github.com/cloudera/dist_test/blob/master/grind/python/disttest/isolate.py
   */
  private static class IsolateFileJson {
    private static class Variables {
      public List<String> files = new ArrayList<>();
      public List<String> command;
    };
    Variables variables = new Variables();

    public String toJson() {
      return GSON.toJson(this);
    }
  }

  /**
   * Structured to generate Json that matches the expected .gen.json contents.
   * See here for a description of the .gen.json contents:
   *   https://github.com/cloudera/dist_test/blob/master/grind/python/disttest/isolate.py
   */
  private static class GenJson {
    int version = 1;
    String dir;
    List<String> args;
    String name;
  }
}

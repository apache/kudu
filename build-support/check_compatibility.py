#!/usr/bin/env python
#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

# Script which checks Java API compatibility between two revisions of the
# Java client.
#
# Compare current commit against the previous commit:
#   ./build-support/check_compatibility.py HEAD~1..HEAD
#
# Compare current commit against a release tag:
#   ./build-support/check_compatibility.py 1.13.0..HEAD
#
# Compare current commit against a release tag:
#   ./build-support/check_compatibility.py 1.13.0..HEAD
#
# Compare two releases:
#   ./build-support/check_compatibility.py 1.12.0..1.13.0
#
# NOTE: You can only compare against releases which support a Gradle build (1.5+)

import argparse
import logging
import os
import re
import shutil
import subprocess
import sys

from kudu_util import check_output, init_logging

JAPICMP_VERSION="0.15.1"
JAPICMP_JAR="japicmp-" + JAPICMP_VERSION + "-jar-with-dependencies.jar"
JAPICMP_URL="https://repo1.maven.org/maven2/com/github/siom79/japicmp/japicmp/" \
             + JAPICMP_VERSION + "/" + JAPICMP_JAR

# Paths are ".../kudu/java/<artifact-name>/build/..."
ARTIFACT_NAME_PATTERN=re.compile("java/(.*)/build")

# Various relative paths
PATH_TO_REPO_DIR = "../"
PATH_TO_BUILD_DIR = "../build/compat-check"


def get_repo_dir():
  """ Return the path to the top of the repo. """
  dirname, _ = os.path.split(os.path.abspath(__file__))
  return os.path.abspath(os.path.join(dirname, PATH_TO_REPO_DIR))


def get_scratch_dir():
  """ Return the path to the scratch dir that we build within. """
  dirname, _ = os.path.split(os.path.abspath(__file__))
  return os.path.abspath(os.path.join(dirname, PATH_TO_BUILD_DIR))


def clean_scratch_dir(scratch_dir):
  """ Clean up and re-create the scratch directory. """
  if os.path.exists(scratch_dir):
    logging.info("Removing scratch dir %s...", scratch_dir)
    shutil.rmtree(scratch_dir)
  logging.info("Creating empty scratch dir %s...", scratch_dir)
  os.makedirs(scratch_dir)


def checkout_source_tree(rev, path):
  """ Check out the Java source tree for the given revision into the given path. """
  logging.info("Checking out %s in %s", rev, path)
  os.makedirs(path)
  # Extract source.
  subprocess.check_call(["bash", '-o', 'pipefail', "-c",
                         ("git archive --format=tar %s | " +
                          "tar -C \"%s\" -xf -") % (rev, path)],
                        cwd=get_repo_dir())

def get_git_hash(revname):
  """ Convert 'revname' to its SHA-1 hash. """
  return check_output(["git", "rev-parse", revname],
                      cwd=get_repo_dir()).decode('utf-8').strip()


def build_tree(path):
  """ Run the Java build within 'path'. """
  java_path = os.path.join(path, "java")
  logging.info("Building in %s...", java_path)
  subprocess.check_call(["./gradlew", "assemble"], cwd=java_path)


def get_japicmp_path():
  """ Return the path where we download the japicmp jar. """
  return os.path.join(get_repo_dir(), "thirdparty/src/" + JAPICMP_JAR)


def download_japicmp(force):
  """ Download the japicmp jar. """
  if os.path.exists(get_japicmp_path()):
    logging.info("japicmp is already downloaded.")
    if not force:
      return
    logging.info("Forcing re-download.")
    os.remove(get_japicmp_path())
  subprocess.check_call(["curl", "--retry", "3", "-L", "-o",
                         get_japicmp_path(), JAPICMP_URL], cwd=get_repo_dir())


def find_client_jars(path):
  """ Return a list of jars within 'path' to be checked for compatibility. """
  all_jars = set(check_output(["find", path, "-name", "*.jar"]).decode('utf-8').splitlines())
  return [j for j in all_jars if (
      "-javadoc" not in j and
      "-sources" not in j and
      "-test-sources" not in j and
      "-tests" not in j and
      "-unshaded" not in j and
      "buildSrc" not in j and
      "gradle-wrapper" not in j and
      "kudu-backup" not in j and
      "kudu-hive" not in j and
      "kudu-jepsen" not in j and
      "kudu-proto" not in j and
      "kudu-subprocess" not in j)]


def get_artifact_name(jar_path):
  """ Return the artifact name given a full jar path. """
  return ARTIFACT_NAME_PATTERN.search(jar_path).group(1)


def run_japicmp(src, dst, opts):
  """ Run the compliance checker to compare 'src' and 'dst'. """
  src_jars = find_client_jars(src)
  dst_jars = find_client_jars(dst)
  logging.info("Will check compatibility between original jars:\n%s\n" +
               "and new jars:\n%s",
               "\n".join(src_jars),
               "\n".join(dst_jars))
  excludes = [
    # Exclude shaded/relocated packages.
    "org.apache.kudu.shaded.*",
    # Exclude inner Protobuf classes since the annotation filter doesn't handle these.
    "org.apache.kudu.*PB*",
    # Exclude generated Protobuf code.
    "@javax.annotation.Generated",
    # Exclude unstable code.
    "@org.apache.yetus.audience.InterfaceStability$Unstable",
    # Exclude private code.
    "@org.apache.yetus.audience.InterfaceAudience$Private",
    # Exclude limited private code.
    "@org.apache.yetus.audience.InterfaceAudience$LimitedPrivate",
    # Exclude Scala Generated code.
    "@scala.reflect.ScalaSignature",
    "*$$anon$*",
    "*$$anonfun$*"
  ]

  reports = []
  for src_jar in src_jars:
    src_name = get_artifact_name(src_jar)
    for dst_jar in dst_jars:
      dst_name = get_artifact_name(dst_jar)
      if src_name == dst_name:
          reports.append((src_name, src_jar, dst_jar))

  # CLI tool documentation: https://siom79.github.io/japicmp/CliTool.html
  for (name, src_jar, dst_jar) in reports:
    out_path = os.path.join(get_scratch_dir(), name + "-report.html")

    # Add the extra flags.
    cmd = ["java", "-jar", get_japicmp_path(),
            "--old", src_jar,
            "--new", dst_jar,
            "--include", "org.apache.kudu.*",
            "--exclude", ";".join(excludes),
            "--html-file", out_path,
            "--only-modified",
            "--ignore-missing-classes",
            "--report-only-filename"]
    if (opts.only_incompatible):
      cmd.append("--only-incompatible")
    if (opts.error_on_binary_incompatibility):
      cmd.append("--error-on-binary-incompatibility")
    if (opts.error_on_source_incompatibility):
      cmd.append("--error-on-source-incompatibility")

    subprocess.check_call(cmd)

def parse_args():
  """ Parse command-line arguments """
  parser = argparse.ArgumentParser(description='Check and report on Kudu API compatibility',
                                   formatter_class=argparse.ArgumentDefaultsHelpFormatter)
  parser.add_argument('source',
                      help="The source revision to compare against. Likely the previous release tag.")
  parser.add_argument('destination',
                      help="The destination revision to compare against the source.")
  parser.add_argument('--only-incompatible', action='store_true',
                      help='Outputs only classes/methods that are binary incompatible. '
                           'If not given, all classes and methods are printed.')
  parser.add_argument('--error-on-binary-incompatibility', action='store_true',
                      help='Exit with an error if a binary incompatibility is detected.')
  parser.add_argument('--error-on-source-incompatibility', action='store_true',
                      help='Exit with an error if a source incompatibility is detected.')
  parser.add_argument("--force-download-deps", action='store_true',
                      help="Download dependencies (i.e. japicmp) even if they are already present")
  return parser.parse_args()

def main():
  opts = parse_args()

  src_rev = get_git_hash(opts.source)
  dst_rev = get_git_hash(opts.destination)

  logging.info("Source revision: %s", src_rev)
  logging.info("Destination revision: %s", dst_rev)

  # Set up the build.
  scratch_dir = get_scratch_dir()
  clean_scratch_dir(scratch_dir)

  # Download japicmp.
  download_japicmp(opts.force_download_deps)

  # Check out the src and dst source trees.
  src_dir = os.path.join(scratch_dir, "src")
  dst_dir = os.path.join(scratch_dir, "dst")
  checkout_source_tree(src_rev, src_dir)
  checkout_source_tree(dst_rev, dst_dir)

  # Run the build in each tree.
  build_tree(src_dir)
  build_tree(dst_dir)

  run_japicmp(src_dir, dst_dir, opts)


if __name__ == "__main__":
  init_logging()
  main()

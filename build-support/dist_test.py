#!/usr/bin/env python2
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
#
# This tool allows tests to be submitted to a distributed testing
# service running on shared infrastructure.
#
# See dist_test.py --help for usage information.

import argparse
from collections import deque
import glob
try:
  import simplejson as json
except:
  import json
import logging
import os
import pprint
import re
import sys
import shlex
import shutil
import subprocess
import time

from dep_extract import DependencyExtractor
from kudu_util import init_logging

TEST_TIMEOUT_SECS = int(os.environ.get('TEST_TIMEOUT_SECS', '900'))
ARTIFACT_ARCHIVE_GLOBS = ["build/*/test-logs/**/*"]
ISOLATE_SERVER = os.environ.get('ISOLATE_SERVER',
                                "http://isolate.cloudera.org:4242/")
DIST_TEST_HOME = os.environ.get('DIST_TEST_HOME',
                                os.path.expanduser("~/dist_test"))

# Put some limit so someone doesn't accidentally try to loop all of the
# tests 10,000 times and cost a bunch of money. If someone really has a good
# reason to do this, they are can always edit this constant locally.
MAX_TASKS_PER_JOB=10000

# The number of times that flaky tests will be retried.
# Our non-distributed implementation sets a number of _attempts_, not a number
# of retries, so we have to subtract 1.
FLAKY_TEST_RETRIES = int(os.environ.get('KUDU_FLAKY_TEST_ATTEMPTS', 1)) - 1

# Whether to retry all failed C++ tests, rather than just known flaky tests.
# Since Java flaky tests are not reported by the test server, Java tests are
# always retried, regardless of this value.
RETRY_ALL_TESTS = int(os.environ.get('KUDU_RETRY_ALL_FAILED_TESTS', 0))

# Flags to include when running Gradle tasks
GRADLE_FLAGS = os.environ.get('EXTRA_GRADLE_FLAGS', "")

PATH_TO_REPO = "../"

# Matches the command line listings in 'ctest -V -N'. For example:
#  262: Test command: /src/kudu/build-support/run-test.sh "/src/kudu/build/debug/bin/jsonwriter-test"
TEST_COMMAND_RE = re.compile('Test command: (.+)$')

# Matches the environment variable listings in 'ctest -V -N'. For example:
#  262:  GTEST_TOTAL_SHARDS=1
TEST_ENV_RE = re.compile('^\d+:  (\S+)=(.+)')

# Matches test names that have a shard suffix. For example:
#  master-stress-test.8
TEST_SHARD_RE = re.compile("\.\d+$")

DEPS_FOR_ALL = \
    ["build-support/stacktrace_addr2line.pl",
     "build-support/report-test.sh",
     "build-support/run-test.sh",
     "build-support/run_dist_test.py",
     "build-support/java-home-candidates.txt",

     # The LLVM symbolizer is necessary for suppressions to work
     "thirdparty/installed/uninstrumented/bin/llvm-symbolizer",

     # Tests that use the external minicluster require these.
     # TODO: declare these dependencies per-test.
     "build/latest/bin/kudu-tserver",
     "build/latest/bin/kudu-master",

     # Tests that require tooling require this.
     "build/latest/bin/kudu",

     # The HMS and Sentry tests require the Hadoop, Hive, and Sentry libraries.
     # These files are just symlinks, but dist-test will copy the entire
     # directories they point to.  The symlinks themselves won't be recreated,
     # so we point to them with environment variables in run_dist_test.py.
     "build/latest/bin/hive-home",
     "build/latest/bin/hadoop-home",
     "build/latest/bin/sentry-home",

     # Add the Kudu HMS plugin.
     "build/latest/bin/hms-plugin.jar",
     ]

class StagingDir(object):
  @staticmethod
  def new():
    dir = rel_to_abs("build/isolate")
    if os.path.isdir(dir):
      shutil.rmtree(dir)
    os.makedirs(dir)
    return StagingDir(dir)

  def __init__(self, dir):
    self.dir = dir

  def archive_dump_path(self):
    return os.path.join(self.dir, "dump.json")

  def gen_json_paths(self):
    return glob.glob(os.path.join(self.dir, "*.gen.json"))

  def tasks_json_path(self):
    return os.path.join(self.dir, "tasks.json")

class TestExecution(object):
  """
  An individual test execution that will be run.

  One instance exists for each shard of a test case.
  """
  def __init__(self, argv=None, env=None):
    self.argv = argv or []
    self.env = env or {}

  @property
  def test_name(self):
    return "%s.%d" % (os.path.basename(self.argv[1]), self.shard())

  def shard(self):
    return int(self.env.get("GTEST_SHARD_INDEX", "0"))

def rel_to_abs(rel_path):
  dirname, _ = os.path.split(os.path.abspath(__file__))
  abs = os.path.abspath(os.path.join(dirname, PATH_TO_REPO, rel_path))
  if rel_path.endswith('/') and not abs.endswith('/'):
    abs += '/'
  return abs


def abs_to_rel(abs_path, staging):
  rel = os.path.relpath(abs_path, staging.dir)
  if abs_path.endswith('/') and not rel.endswith('/'):
    rel += '/'
  return rel


def get_test_executions(tests_regex, extra_args=None):
  """
  Return an array of TestExecution objects.

  :param tests_regex: Regexp for ctest's -R option to select particular tests
  :param extra_args: Extra arguments to run the test binary with
  """
  ctest_bin = os.path.join(rel_to_abs("thirdparty/installed/common/bin/ctest"))
  ctest_argv = [ctest_bin, "-V", "-N", "-LE", "no_dist_test"]
  if tests_regex:
    ctest_argv.extend(['-R', tests_regex])
  p = subprocess.Popen(ctest_argv,
                       stdout=subprocess.PIPE,
                       cwd=rel_to_abs("build/latest"))
  out, err = p.communicate()
  if p.returncode != 0:
    logging.error("Unable to list tests with ctest")
    sys.exit(1)
  lines = deque(out.splitlines())
  execs = []
  # Output looks like:
  # 262: Test command: /src/kudu/build-support/run-test.sh "/src/kudu/build/debug/bin/jsonwriter-test"
  # 262: Environment variables:
  # 262:  KUDU_TEST_TIMEOUT=900
  # 262:  GTEST_TOTAL_SHARDS=1
  # 262:  GTEST_SHARD_INDEX=0
  #   Test #262: jsonwriter-test.0
  #
  # 263: Test command ...
  # ...

  while lines:
    # Advance to the beginning of the next test block.
    m = None
    while lines and not m:
      m = TEST_COMMAND_RE.search(lines.popleft())
    if not m:
      break
    argv = shlex.split(m.group(1))
    if extra_args:
      argv.extend(extra_args)
    # Next line should b the 'Environment variables' heading
    l = lines.popleft()
    if "Environment variables:" not in l:
      raise Exception("Unexpected line in ctest -V output: %s" % l)
    # Following lines should be environment variable pairs.
    env = {}
    while lines:
      m = TEST_ENV_RE.match(lines[0])
      if not m:
        break
      lines.popleft()
      env[m.group(1)] = m.group(2)
    execs.append(TestExecution(argv=argv, env=env))
  return execs


def is_lib_whitelisted(lib):
  # No need to ship things like libc, libstdcxx, etc.
  if lib.startswith("/lib") or lib.startswith("/usr"):
    return False
  return True


def create_dependency_extractor():
  dep_extractor = DependencyExtractor()
  dep_extractor.set_library_filter(is_lib_whitelisted)
  dep_extractor.set_expand_symlinks(True)
  return dep_extractor


def get_base_deps(dep_extractor):
  deps = []
  for d in DEPS_FOR_ALL:
    d = os.path.realpath(rel_to_abs(d))
    if os.path.isdir(d):
      d += "/"
    deps.append(d)
    # DEPS_FOR_ALL may include binaries whose dependencies are not dependencies
    # of the test executable. We must include those dependencies in the archive
    # for the binaries to be usable.
    deps.extend(dep_extractor.extract_deps(d))
  return deps


def is_outside_of_tree(path):
  repo_dir = rel_to_abs("./")
  rel = os.path.relpath(path, repo_dir)
  return rel.startswith("../")

def copy_system_library(lib):
  """
  For most system libraries, we expect them to be installed on the test
  machines. However, a couple are shipped from the submitter machine
  to the cluster by putting them in a special directory inside the
  isolated build tree.

  This function copies such libraries into that directory.
  """
  sys_lib_dir = rel_to_abs("build/dist-test-system-libs")
  if not os.path.exists(sys_lib_dir):
    os.makedirs(sys_lib_dir)
  dst = os.path.join(sys_lib_dir, os.path.basename(lib))
  # Copy if it doesn't exist, or the mtimes don't match.
  # Using shutil.copy2 preserves the mtime after the copy (like cp -p)
  if not os.path.exists(dst) or os.stat(dst).st_mtime != os.stat(lib).st_mtime:
    logging.info("Copying system library %s to %s...", lib, dst)
    shutil.copy2(rel_to_abs(lib), dst)
  return dst

def forward_env_var(command_list, var_name, is_required=True):
  """
  Extends 'command_list' with the name and value of the environment variable
  given by 'var_name'.

  Does nothing if the environment variable isn't set or is empty, unless
  'is_required' is True, in which case an exception is raised.
  """
  if not var_name in os.environ or not os.environ.get(var_name):
    if is_required:
      raise Exception("required env variable %s is missing" % (var_name,))
    return
  command_list.extend(["-e", "%s=%s" % (var_name, os.environ.get(var_name))])

def create_archive_input(staging, execution, dep_extractor,
                         collect_tmpdir=False):
  """
  Generates .gen.json and .isolate files corresponding to the
  test 'execution', which must be a TestExecution instance.
  The outputs are placed in the specified staging directory.
  """
  argv = execution.argv
  if not argv[0].endswith('run-test.sh') or len(argv) < 2:
    logging.warning("Unable to handle test: %s", argv)
    return
  abs_test_exe = os.path.realpath(argv[1])
  rel_test_exe = abs_to_rel(abs_test_exe, staging)
  argv[1] = rel_test_exe
  files = []
  files.append(rel_test_exe)
  deps = dep_extractor.extract_deps(abs_test_exe)
  deps.extend(get_base_deps(dep_extractor))

  # Deduplicate dependencies included via DEPS_FOR_ALL.
  for d in set(deps):
    # System libraries will end up being relative paths out
    # of the build tree. We need to copy those into the build
    # tree somewhere.
    if is_outside_of_tree(d):
      d = copy_system_library(d)
    files.append(abs_to_rel(d, staging))

  # Add data file dependencies.
  if 'KUDU_DATA_FILES' in execution.env:
    for data_file in execution.env['KUDU_DATA_FILES'].split(","):
      # Paths are relative to the test binary.
      path = os.path.join(os.path.dirname(abs_test_exe), data_file)
      files.append(abs_to_rel(path, staging))

  out_archive = os.path.join(staging.dir, '%s.gen.json' % (execution.test_name))
  out_isolate = os.path.join(staging.dir, '%s.isolate' % (execution.test_name))

  command = ['../../build-support/run_dist_test.py',
             '-e', 'KUDU_TEST_TIMEOUT=%d' % (TEST_TIMEOUT_SECS - 30),
             '-e', 'KUDU_ALLOW_SLOW_TESTS=%s' % os.environ.get('KUDU_ALLOW_SLOW_TESTS', 1),
             '-e', 'KUDU_COMPRESS_TEST_OUTPUT=%s' % \
                    os.environ.get('KUDU_COMPRESS_TEST_OUTPUT', 0)]
  for k, v in execution.env.iteritems():
    if k == 'KUDU_TEST_TIMEOUT':
      # Currently we don't respect the test timeouts specified in ctest, since
      # we want to make sure that the dist-test task timeout and the
      # underlying test timeout are coordinated.
      continue
    command.extend(['-e', '%s=%s' % (k, v)])

  # If test result reporting was requested, forward all relevant environment
  # variables into the test process so as to enable reporting.
  if os.environ.get('KUDU_REPORT_TEST_RESULTS', 0):
    forward_env_var(command, 'KUDU_REPORT_TEST_RESULTS')
    forward_env_var(command, 'BUILD_CONFIG')
    forward_env_var(command, 'BUILD_TAG')
    forward_env_var(command, 'GIT_REVISION')
    forward_env_var(command, 'TEST_RESULT_SERVER', is_required=False)

  if collect_tmpdir:
    command += ["--collect-tmpdir"]
  command.append('--')
  command += argv[1:]

  archive_json = dict(args=["-i", out_isolate,
                            "-s", out_isolate + "d"],
                      dir=rel_to_abs("."),
                      version=1)
  isolate_dict = dict(variables=dict(command=command,
                                     files=files))
  with open(out_archive, "w") as f:
    json.dump(archive_json, f)
  with open(out_isolate, "w") as f:
    pprint.pprint(isolate_dict, f)


def create_task_json(staging,
                     replicate_tasks=1,
                     flaky_test_set=set(),
                     retry_all_tests=False):
  """
  Create a task JSON file suitable for submitting to the distributed
  test execution service.

  If 'replicate_tasks' is higher than one, each .isolate file will be
  submitted multiple times. This can be useful for looping tests.

  The test name is compared with the contents of 'flaky_test_set' to decide
  how many times the execution service should retry the test on failure.
  Alternatively, if 'retry_all_tests' is True, all tests will be retried.
  """
  tasks = []
  with file(staging.archive_dump_path(), "r") as isolate_dump:
    inmap = json.load(isolate_dump)

  # Some versions of 'isolate batcharchive' directly list the items in
  # the dumped JSON. Others list it in an 'items' dictionary.
  items = inmap.get('items', inmap)
  for k, v in items.iteritems():
    # The key may be 'foo-test.<shard>'. So, chop off the last component
    # to get the test name.
    test_name = ".".join(k.split(".")[:-1]) if TEST_SHARD_RE.search(k) else k
    max_retries = 0
    if test_name in flaky_test_set or retry_all_tests:
      max_retries = FLAKY_TEST_RETRIES

    tasks += [{"isolate_hash": str(v),
               "description": str(k),
               "artifact_archive_globs": ARTIFACT_ARCHIVE_GLOBS,
               "timeout": TEST_TIMEOUT_SECS + 30,
               "max_retries": max_retries
               }] * replicate_tasks

  if len(tasks) > MAX_TASKS_PER_JOB:
    logging.error("Job contains %d tasks which is more than the maximum %d",
                  len(tasks), MAX_TASKS_PER_JOB)
    sys.exit(1)
  outmap = {"tasks": tasks}

  with file(staging.tasks_json_path(), "wt") as f:
    json.dump(outmap, f)


def run_isolate(staging):
  """
  Runs 'isolate batcharchive' to archive all of the .gen.json files in
  the provided staging directory.

  Throws an exception if the call fails.
  """
  isolate_path = "isolate"
  try:
    subprocess.check_call([isolate_path,
                           'batcharchive',
                           '-isolate-server=' + ISOLATE_SERVER,
                           '-dump-json=' + staging.archive_dump_path(),
                           '--'] + staging.gen_json_paths())
  except:
    logging.error("Failed to run %s", isolate_path)
    raise

def submit_tasks(staging, options):
  """
  Runs the distributed testing tool to submit the tasks in the
  provided staging directory.

  This requires that the tasks JSON file has already been generated
  by 'create_task_json()'.
  """
  if not os.path.exists(DIST_TEST_HOME):
    logging.error("Cannot find dist_test tools at path %s " \
                  "Set the DIST_TEST_HOME environment variable to the path to the dist_test directory. ",
                  DIST_TEST_HOME)
    raise OSError("Cannot find path to dist_test tools")
  client_py_path = os.path.join(DIST_TEST_HOME, "bin", "client")
  try:
    cmd = [client_py_path, "submit"]
    if options.no_wait:
      cmd.append('--no-wait')
    cmd.append(staging.tasks_json_path())
    subprocess.check_call(cmd)
  except:
    logging.error("Failed to run %s", client_py_path)
    raise

def get_flakies():
  path = os.getenv('KUDU_FLAKY_TEST_LIST')
  if not path:
    return set()

  # dist-test can only retry tests on a per-class basis, but
  # KUDU_FLAKY_TEST_LIST lists Java flakes on a per-method basis.
  flaky_classes = []
  with open(path) as f:
    for l in f:
      l = l.strip()
      if '.' in l:
        # "o.a.k.client.TestKuduClient.testFoo" -> "o.a.k.client.TestKuduClient"
        flaky_classes.append('.'.join(l.split('.')[:-1]))
      else:
        flaky_classes.append(l)

  return set(flaky_classes)

def run_tests(parser, options):
  """
  Gets all of the test command lines from 'ctest', isolates them,
  creates a task list, and submits the tasks to the testing service.
  """
  executions = get_test_executions(options.tests_regex)
  if not executions:
    raise Exception("No matching tests found for pattern %s" % options.tests_regex)
  if options.extra_args:
    if options.extra_args[0] == '--':
      del options.extra_args[0]
    for e in executions:
      e.argv.extend(options.extra_args)
  staging = StagingDir.new()
  dep_extractor = create_dependency_extractor()
  for execution in executions:
    create_archive_input(staging, execution, dep_extractor,
                         collect_tmpdir=options.collect_tmpdir)
  run_isolate(staging)
  retry_all = RETRY_ALL_TESTS > 0
  create_task_json(staging,
                   flaky_test_set=get_flakies(),
                   replicate_tasks=options.num_instances,
                   retry_all_tests=retry_all)
  submit_tasks(staging, options)

def add_run_subparser(subparsers):
  p = subparsers.add_parser('run', help='Run the dist-test-enabled tests')

  p.add_argument("--tests-regex", "-R", dest="tests_regex", type=str,
                 metavar="REGEX",
                 help="Only run tests matching regular expression. For example, " +
                 "'run -R consensus' will run any tests with the word consensus in " +
                 "their names.")
  p.add_argument("--num-instances", "-n", dest="num_instances", type=int,
                 default=1, metavar="NUM",
                 help="Number of times to submit each matching test. This can be used to " +
                 "loop a suite of tests to test for flakiness. Typically this should be used " +
                 "in conjunction with the --tests-regex option above to select a small number " +
                 "of tests.")
  p.add_argument("extra_args", nargs=argparse.REMAINDER,
                 help=("Optional arguments to append to the command line for all " +
                       "submitted tasks. Passing a '--' argument before the list of " +
                       "arguments to pass may be helpful."))
  p.set_defaults(func=run_tests)

def loop_test(parser, options):
  """
  Runs many instances of a user-provided test case on the testing service.

  To run the test, this function first builds corresponding list of
  TestExecution objects using get_test_executions() to properly populate all
  the necessary ctest environment variables for TestExecution instances.
  Those environment variables are necessary for building proper dependencies
  for test binaries: e.g., including all DATA_FILES.
  """
  if options.num_instances < 1:
    parser.error("--num-instances must be >= 1")
  # A regex to match ctest's name of the test suite provided by the binary,
  # sharded and non-sharded. For example, sharded are 'client-test.0',
  # 'client-test.1', ..., 'client-test.7'; non-sharded is
  # 'ts_location_assignment-itest'.
  tests_regex = "^" + os.path.basename(options.cmd) + "(\.[0-9]+)?$"
  executions = get_test_executions(tests_regex, options.args)
  if not executions:
    raise Exception("No matching tests found for pattern %s" % tests_regex)
  staging = StagingDir.new()
  # The presence of the --gtest_filter flag means the user is interested in a
  # particular subset of tests provided by the binary. In that case it doesn't
  # make sense to shard the execution since only the shards containing the
  # matching tests would produce interesting results, while the rest would
  # yield noise. To avoid this, we disable sharding in this case.
  filter_flags = [e for e in options.args
      if e.find("--gtest_filter") == 0 or e.find("--gtest-filter") == 0]
  if filter_flags and len(executions) > 1:
    # If the test is sharded, avoid sharded execution by using only one
    # TestExecution from the generated list (doesn't matter which one)
    # and update shard-related environment variables.
    executions = executions[:1]
    e = executions[0]
    e.env["GTEST_TOTAL_SHARDS"] = 1
    e.env["GTEST_SHARD_INDEX"] = 0
  dep_extractor = create_dependency_extractor()
  for execution in executions:
    create_archive_input(staging, execution, dep_extractor,
                         collect_tmpdir=options.collect_tmpdir)
  run_isolate(staging)
  create_task_json(staging, options.num_instances)
  submit_tasks(staging, options)

def add_loop_test_subparser(subparsers):
  p = subparsers.add_parser("loop",
      help="Run many instances of the same test, specified by its full path",
      epilog="NOTE: unless --gtest_filter=... flag is specified for the test "
      "binary, the test scenarios are automatically sharded in accordance with "
      "the shard count defined in CMakeLists.txt. Another way to loop an "
      "entire suite of tests is to use the 'run' command instead. The 'run' "
      "command will unconditionally shard bigger test suites into more "
      "granular tasks based on the shard count configured in CMakeLists.txt. "
      "For example: dist_test.py run -R '^raft_consensus-itest' -n 1000")
  p.add_argument("--num-instances", "-n",
      dest="num_instances", type=int, metavar="NUM", default=100,
      help="number of test instances to start. If passing arguments to the "
      "test, you may want to use a '--' argument before <test-path>. "
      "e.g: loop -- build/latest/bin/foo-test --gtest_opt=123")
  p.add_argument("cmd", help="the path to the test binary (e.g. build/latest/bin/foo-test)")
  p.add_argument("args", nargs=argparse.REMAINDER, help="test arguments")
  p.set_defaults(func=loop_test)

def get_gradle_cmd_line(options):
  cmd = [rel_to_abs("java/gradlew")]
  cmd.extend(GRADLE_FLAGS.split())
  cmd.append("distTest")
  if options.collect_tmpdir:
    cmd.append("--collect-tmpdir")
  return cmd

def run_java_tests(parser, options):
  subprocess.check_call(get_gradle_cmd_line(options),
                        cwd=rel_to_abs("java"))
  staging = StagingDir(rel_to_abs("java/build/dist-test"))
  run_isolate(staging)
  retry_all = RETRY_ALL_TESTS > 0
  create_task_json(staging,
                   flaky_test_set=get_flakies(),
                   replicate_tasks=1,
                   retry_all_tests=retry_all)
  submit_tasks(staging, options)

def loop_java_test(parser, options):
  """
  Runs many instances of a user-provided Java test class on the testing service.
  """
  if options.num_instances < 1:
    parser.error("--num-instances must be >= 1")
  cmd = get_gradle_cmd_line(options)
  cmd.extend([ "--classes", "**/%s" % options.pattern ])
  subprocess.check_call(cmd, cwd=rel_to_abs("java"))
  staging = StagingDir(rel_to_abs("java/build/dist-test"))
  run_isolate(staging)
  create_task_json(staging, options.num_instances)
  submit_tasks(staging, options)

def add_java_subparser(subparsers):
  p = subparsers.add_parser('java', help='Run java tests via dist-test')
  sp = p.add_subparsers()
  run_all = sp.add_parser("run-all",
      help="Run all of the Java tests via dist-test")
  run_all.set_defaults(func=run_java_tests)

  loop = sp.add_parser("loop", help="Loop a single Java test")
  loop.add_argument("--num-instances", "-n", dest="num_instances", type=int,
                 help="number of test instances to start", metavar="NUM",
                 default=100)
  loop.add_argument("pattern", help="Pattern matching a Java test class to run")
  loop.set_defaults(func=loop_java_test)

def dump_base_deps(parser, options):
  print json.dumps(get_base_deps(create_dependency_extractor()))

def add_internal_commands(subparsers):
  p = subparsers.add_parser('internal', help="[Internal commands not for users]")
  p.add_subparsers().add_parser('dump_base_deps').set_defaults(func=dump_base_deps)

def main(argv):
  p = argparse.ArgumentParser()
  p.add_argument("--collect-tmpdir", dest="collect_tmpdir", action="store_true",
                 help="Collect the test tmpdir of failed tasks as test artifacts", default=False)
  p.add_argument("--no-wait", dest="no_wait", action="store_true",
                 help="Return without waiting for the job to complete", default=False)
  sp = p.add_subparsers()
  add_loop_test_subparser(sp)
  add_run_subparser(sp)
  add_java_subparser(sp)
  add_internal_commands(sp)
  args = p.parse_args(argv)
  args.func(p, args)

if __name__ == "__main__":
  init_logging()
  main(sys.argv[1:])

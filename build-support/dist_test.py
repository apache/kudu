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

PATH_TO_REPO = "../"

# Matches the command line listings in 'ctest -V -N'. For example:
#   262: Test command: /src/kudu/build-support/run-test.sh "/src/kudu/build/debug/bin/jsonwriter-test"
TEST_COMMAND_RE = re.compile('Test command: (.+)$')

# Matches the environment variable listings in 'ctest -V -N'. For example:
#  262:  GTEST_TOTAL_SHARDS=1
TEST_ENV_RE = re.compile('^\d+:  (\S+)=(.+)')

# Matches the output lines of 'ldd'. For example:
#   libcrypto.so.10 => /path/to/usr/lib64/libcrypto.so.10 (0x00007fb0cb0a5000)
LDD_RE = re.compile(r'^\s+.+? => (\S+) \(0x.+\)')

DEPS_FOR_ALL = \
    ["build-support/stacktrace_addr2line.pl",
     "build-support/run-test.sh",
     "build-support/run_dist_test.py",
     "build-support/tsan-suppressions.txt",
     "build-support/lsan-suppressions.txt",

     # The LLVM symbolizer is necessary for suppressions to work
     "thirdparty/installed/uninstrumented/bin/llvm-symbolizer",

     # Tests that use the external minicluster require these.
     # TODO: declare these dependencies per-test.
     "build/latest/bin/kudu-tserver",
     "build/latest/bin/kudu-master",

     # Tests that require tooling require this.
     "build/latest/bin/kudu",

     # The HMS tests require the Hadoop and Hive libraries. These files are just
     # symlinks, but dist-test will copy the entire directories they point to.
     # The symlinks themselves won't be recreated, so we point to them with
     # environment variables in run_dist_test.py.
     "build/latest/bin/hive-home",
     "build/latest/bin/hadoop-home",

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


def get_test_executions(options):
  """
  Return an array of TestExecution objects.
  """
  ctest_bin = os.path.join(rel_to_abs("thirdparty/installed/common/bin/ctest"))
  ctest_argv = [ctest_bin, "-V", "-N", "-LE", "no_dist_test"]
  if options.tests_regex:
    ctest_argv.extend(['-R', options.tests_regex])
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


def is_lib_blacklisted(lib):
  # No need to ship things like libc, libstdcxx, etc.
  if lib.startswith("/lib") or lib.startswith("/usr"):
    return True
  return False


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

LDD_CACHE={}
def ldd_deps(exe):
  """
  Runs 'ldd' on the provided 'exe' path, returning a list of
  any libraries it depends on. Blacklisted libraries are
  removed from this list.

  If the provided 'exe' is not a binary executable, returns
  an empty list.
  """
  if (exe.endswith(".jar") or
      exe.endswith(".pl") or
      exe.endswith(".py") or
      exe.endswith(".sh") or
      exe.endswith(".txt") or
      os.path.isdir(exe)):
    return []
  if exe not in LDD_CACHE:
    p = subprocess.Popen(["ldd", exe], stdout=subprocess.PIPE)
    out, err = p.communicate()
    LDD_CACHE[exe] = (out, err, p.returncode)
  out, err, rc = LDD_CACHE[exe]
  if rc != 0:
    logging.warning("failed to run ldd on %s", exe)
    return []
  ret = []
  for l in out.splitlines():
    m = LDD_RE.match(l)
    if not m:
      continue
    lib = m.group(1)
    if is_lib_blacklisted(lib):
      continue
    path = m.group(1)
    ret.append(m.group(1))

    # ldd will often point to symlinks. We need to upload the symlink
    # as well as whatever it's pointing to, recursively.
    while os.path.islink(path):
      path = os.path.join(os.path.dirname(path), os.readlink(path))
      ret.append(path)
  return ret


def create_archive_input(staging, execution,
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
  deps = ldd_deps(abs_test_exe)
  for d in DEPS_FOR_ALL:
    d = os.path.realpath(rel_to_abs(d))
    if os.path.isdir(d):
      d += "/"
    deps.append(d)
    # DEPS_FOR_ALL may include binaries whose dependencies are not dependencies
    # of the test executable. We must include those dependencies in the archive
    # for the binaries to be usable.
    deps.extend(ldd_deps(d))

  # Deduplicate dependencies included via DEPS_FOR_ALL.
  for d in set(deps):
    # System libraries will end up being relative paths out
    # of the build tree. We need to copy those into the build
    # tree somewhere.
    if is_outside_of_tree(d):
      d = copy_system_library(d)
    files.append(abs_to_rel(d, staging))

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
                     flaky_test_set=set()):
  """
  Create a task JSON file suitable for submitting to the distributed
  test execution service.

  If 'replicate_tasks' is higher than one, each .isolate file will be
  submitted multiple times. This can be useful for looping tests.
  """
  tasks = []
  with file(staging.archive_dump_path(), "r") as isolate_dump:
    inmap = json.load(isolate_dump)

  # Some versions of 'isolate batcharchive' directly list the items in
  # the dumped JSON. Others list it in an 'items' dictionary.
  items = inmap.get('items', inmap)
  for k, v in items.iteritems():
    # The key is 'foo-test.<shard>'. So, chop off the last component
    # to get the test name
    test_name = ".".join(k.split(".")[:-1])
    max_retries = 0
    if test_name in flaky_test_set:
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
  return set(l.strip() for l in file(path))

def run_tests(parser, options):
  """
  Gets all of the test command lines from 'ctest', isolates them,
  creates a task list, and submits the tasks to the testing service.
  """
  executions = get_test_executions(options)
  if options.extra_args:
    if options.extra_args[0] == '--':
      del options.extra_args[0]
    for e in executions:
      e.argv.extend(options.extra_args)
  staging = StagingDir.new()
  for execution in executions:
    create_archive_input(staging, execution,
                         collect_tmpdir=options.collect_tmpdir)
  run_isolate(staging)
  create_task_json(staging,
                   flaky_test_set=get_flakies(),
                   replicate_tasks=options.num_instances)
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
  """
  if options.num_instances < 1:
    parser.error("--num-instances must be >= 1")
  execution = TestExecution(["run-test.sh", options.cmd] + options.args)
  staging = StagingDir.new()
  create_archive_input(staging, execution,
                       collect_tmpdir=options.collect_tmpdir)
  run_isolate(staging)
  create_task_json(staging, options.num_instances)
  submit_tasks(staging, options)

def add_loop_test_subparser(subparsers):
  p = subparsers.add_parser('loop',
                            help='Run many instances of the same test, specified by its full path',
                            epilog="NOTE: if you would like to loop an entire suite of tests, you may " +
                            "prefer to use the 'run' command instead. The 'run' command will automatically " +
                            "shard bigger test suites into more granular tasks based on the shard count " +
                            "configured in CMakeLists.txt. For example: " +
                            "dist_test.py run -R '^raft_consensus-itest' -n 1000")
  p.add_argument("--num-instances", "-n", dest="num_instances", type=int,
                 metavar="NUM",
                 help="number of test instances to start. If passing arguments to the " +
                 "test, you may want to use a '--' argument before <test-path>. " +
                 "e.g: loop -- build/latest/bin/foo-test --gtest_opt=123",
                 default=100)
  p.add_argument("cmd", help="the path to the test binary (e.g. build/latest/bin/foo-test)")
  p.add_argument("args", nargs=argparse.REMAINDER, help="test arguments")
  p.set_defaults(func=loop_test)


def main(argv):
  p = argparse.ArgumentParser()
  p.add_argument("--collect-tmpdir", dest="collect_tmpdir", action="store_true",
                 help="Collect the test tmpdir of failed tasks as test artifacts", default=False)
  p.add_argument("--no-wait", dest="no_wait", action="store_true",
                 help="Return without waiting for the job to complete", default=False)
  sp = p.add_subparsers()
  add_loop_test_subparser(sp)
  add_run_subparser(sp)
  args = p.parse_args(argv)
  args.func(p, args)


if __name__ == "__main__":
  init_logging()
  main(sys.argv[1:])

#!/usr/bin/env python2
# Copyright (c) 2015, Cloudera, inc.
#
# This script runs on the distributed-test slave and acts
# as a wrapper around run-test.sh.
#
# The distributed testing system can't pass in environment variables
# to commands, so this takes some parameters, turns them into environment
# variables, and then executes the test wrapper.
#
# We also 'cat' the test log upon completion so that the test logs are
# uploaded by the test slave back.

import optparse
import os
import shutil
import subprocess
import sys

ME = os.path.abspath(__file__)
ROOT = os.path.abspath(os.path.join(os.path.dirname(ME), ".."))


def main():
  p = optparse.OptionParser(usage="usage: %prog [options] <test-name>")
  p.add_option("-s", "--shard", dest="shard", type="int",
               help="test shard to run", metavar="SHARD",
               default=0)
  p.add_option("-t", "--total-shards", dest="total_shards", type="int",
               help="total shards", metavar="NUM_SHARDS",
               default=1)
  p.add_option("--timeout", dest="timeout", type="int",
               help="timeout for test", metavar="SECONDS",
               default=300)
  options, args = p.parse_args()
  if options.shard >= options.total_shards:
    print >>sys.stderr, "--shard value must be in range [0, --total-shards - 1]"
    sys.exit(1)
  if len(args) < 1:
    p.print_help(sys.stderr)
    sys.exit(1)
  test_exe = args[0]
  test_name = os.path.basename(test_exe)

  env = os.environ.copy()
  env['GTEST_TOTAL_SHARDS'] = str(options.total_shards)
  env['GTEST_SHARD_INDEX'] = str(options.shard)
  env['KUDU_TEST_TIMEOUT'] = str(options.timeout)
  env['LD_LIBRARY_PATH'] = ":".join(
      [os.path.join(ROOT, "thirdparty/installed/lib"),
       os.path.join(ROOT, "build/dist-test-system-libs/"),
       os.path.abspath(os.path.dirname(test_exe))])
  env['KUDU_ALLOW_SLOW_TESTS'] = '1'
  rc = subprocess.call([os.path.join(ROOT, "build-support/run-test.sh")] + args,
                       env=env)
  # 'cat' the test logs to stdout so that the user can grab them.
  with file(os.path.join(ROOT, "build/test-logs/%s.txt" % test_name), "r") as f:
    shutil.copyfileobj(f, sys.stdout)
  sys.exit(rc)


if __name__ == "__main__":
  main()

#!/usr/bin/env python

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

from __future__ import print_function
from io import StringIO
import glob
import json
import logging
import optparse
import os
import re
import subprocess
import sys

from kudu_util import get_upstream_commit, check_output, ROOT, Colors, init_logging
import iwyu.fix_includes
from iwyu.fix_includes import ParseAndMergeIWYUOutput

_USAGE = """\
%prog [--fix] [--sort-only] [--all | --from-git | <path>...]

%prog is a wrapper around include-what-you-use that passes the appropriate
configuration and filters the output to ignore known issues. In addition,
it can automatically pipe the output back into the IWYU-provided 'fix_includes.py'
script in order to fix any reported issues.
"""

_MAPPINGS_DIR = os.path.join(ROOT, "build-support/iwyu/mappings/")
_TOOLCHAIN_DIR = os.path.join(ROOT, "thirdparty/clang-toolchain/bin")
_IWYU_TOOL = os.path.join(ROOT, "build-support/iwyu/iwyu_tool.py")

# Matches source files that we should run on.
_RE_SOURCE_FILE = re.compile(r'\.(c|cc|h)$')

# Matches compilation errors in the output of IWYU
_RE_CLANG_ERROR = re.compile(r'^.+?:\d+:\d+:\s*'
                             r'(fatal )?error:', re.MULTILINE)

# Files that we don't want to ever run IWYU on, because they aren't clean yet.
_MUTED_FILES = set([
  "src/kudu/cfile/cfile_reader.h",
  "src/kudu/cfile/cfile_writer.h",
  "src/kudu/client/client-internal.h",
  "src/kudu/client/client-test.cc",
  "src/kudu/common/encoded_key-test.cc",
  "src/kudu/common/schema.h",
  "src/kudu/experiments/rwlock-perf.cc",
  "src/kudu/rpc/reactor.cc",
  "src/kudu/rpc/reactor.h",
  "src/kudu/security/ca/cert_management.cc",
  "src/kudu/security/ca/cert_management.h",
  "src/kudu/security/cert-test.cc",
  "src/kudu/security/cert.cc",
  "src/kudu/security/cert.h",
  "src/kudu/security/openssl_util.cc",
  "src/kudu/security/openssl_util.h",
  "src/kudu/security/tls_context.cc",
  "src/kudu/security/tls_handshake.cc",
  "src/kudu/security/tls_socket.h",
  "src/kudu/security/x509_check_host.cc",
  "src/kudu/server/default-path-handlers.cc",
  "src/kudu/server/webserver.cc",
  "src/kudu/util/bit-util-test.cc",
  "src/kudu/util/group_varint-test.cc",
  "src/kudu/util/metrics.h",
  "src/kudu/util/minidump.cc",
  "src/kudu/util/mt-metrics-test.cc",
  "src/kudu/util/process_memory.cc",
  "src/kudu/util/rle-test.cc"
])

# Flags to pass to iwyu/fix_includes.py for Kudu-specific style.
_FIX_INCLUDES_STYLE_FLAGS = [
  '--blank_lines',
  '--blank_line_between_c_and_cxx_includes',
  '--separate_project_includes=kudu/',
  '--reorder'
]

# Directory containin the compilation database
_BUILD_DIR = os.path.join(ROOT, 'build/latest')

def _get_file_list_from_git():
  upstream_commit = get_upstream_commit()
  out = check_output(["git", "diff", "--name-only", upstream_commit]).splitlines()
  return [l.decode('utf-8') for l in out if _RE_SOURCE_FILE.search(l.decode('utf-8'))]

def _get_paths_from_compilation_db():
  db_path = os.path.join(_BUILD_DIR, 'compile_commands.json')
  with open(db_path, 'r') as fileobj:
    compilation_db = json.load(fileobj)
  return [entry['file'] for entry in compilation_db]

def _run_iwyu_tool(paths):
  iwyu_args = ['--max_line_length=256']
  for m in glob.glob(os.path.join(_MAPPINGS_DIR, "*.imp")):
    iwyu_args.append("--mapping_file=%s" % os.path.abspath(m))

  cmdline = [_IWYU_TOOL, '-p', _BUILD_DIR]
  cmdline.extend(paths)
  cmdline.append('--')
  cmdline.extend(iwyu_args)
  # iwyu_tool.py requires include-what-you-use on the path
  env = os.environ.copy()
  env['PATH'] = "%s:%s" % (_TOOLCHAIN_DIR, env['PATH'])
  def crash(output):
    sys.exit((Colors.RED + "Failed to run IWYU tool.\n\n" + Colors.RESET +
              Colors.YELLOW + "Command line:\n" + Colors.RESET +
              "%s\n\n" +
              Colors.YELLOW + "Output:\n" + Colors.RESET +
              "%s") % (" ".join(cmdline), output))

  try:
    output = check_output(cmdline, env=env, stderr=subprocess.STDOUT).decode('utf-8')
    if '\nFATAL ERROR: ' in output or \
       'Assertion failed: ' in output or \
       _RE_CLANG_ERROR.search(output):
      crash(output)
    return output
  except subprocess.CalledProcessError as e:
    crash(e.output)


def _is_muted(path):
  assert os.path.isabs(path)
  rel = os.path.relpath(path, ROOT)
  return not rel.startswith('src/') or rel in _MUTED_FILES


def _filter_paths(paths):
  return [p for p in paths if not _is_muted(p)]


def _relativize_paths(paths):
  """ Make paths relative to the build directory. """
  return [os.path.relpath(p, _BUILD_DIR) for p in paths]


def _get_thirdparty_include_dirs():
  return glob.glob(os.path.join(ROOT, "thirdparty", "installed", "*", "include"))


def _get_fixer_flags(flags):
  args = ['--quiet',
          '--nosafe_headers',
          '--source_root=%s' % os.path.join(ROOT, 'src')]
  if flags.dry_run:
    args.append("--dry_run")
  for d in _get_thirdparty_include_dirs():
    args.extend(['--thirdparty_include_dir', d])
  args.extend(_FIX_INCLUDES_STYLE_FLAGS)
  fixer_flags, _ = iwyu.fix_includes.ParseArgs(args)
  return fixer_flags


def _do_iwyu(flags, paths):
  iwyu_output = _run_iwyu_tool(paths)
  if flags.dump_iwyu_output:
    logging.info("Dumping iwyu output to %s", flags.dump_iwyu_output)
    with open(flags.dump_iwyu_output, "w") as f:
      print(iwyu_output, file=f)
  stream = StringIO(iwyu_output)
  fixer_flags = _get_fixer_flags(flags)

  # Passing None as 'fix_paths' tells the fixer script to process
  # all of the IWYU output, instead of just the output corresponding
  # to files in 'paths'. This means that if you run this script on a
  # .cc file, it will also report and fix errors in headers included
  # by that .cc file.
  fix_paths = None
  records = ParseAndMergeIWYUOutput(stream, fix_paths, fixer_flags)
  unfiltered_count = len(records)
  records = [r for r in records if not _is_muted(os.path.abspath(r.filename))]
  if len(records) < unfiltered_count:
    logging.info("Muted IWYU suggestions on %d file(s)", unfiltered_count - len(records))
  return iwyu.fix_includes.FixManyFiles(records, fixer_flags)


def _do_sort_only(flags, paths):
  fixer_flags = _get_fixer_flags(flags)
  iwyu.fix_includes.SortIncludesInFiles(paths, fixer_flags)


def main(argv):
  parser = optparse.OptionParser(usage=_USAGE)
  for i, arg in enumerate(argv):
    if arg.startswith('-'):
      argv[i] = argv[i].replace('_', '-')

  parser.add_option('--all', action='store_true',
                    help=('Process all files listed in the compilation database of the current '
                          'build.'))

  parser.add_option('--from-git', action='store_true',
                    help=('Determine the list of files to run IWYU automatically based on git. '
                          'All files which are modified in the current working tree or in commits '
                          'not yet committed upstream by gerrit are processed.'))

  parser.add_option('--fix', action='store_false', dest="dry_run", default=True,
                    help=('If this is set, fixes IWYU issues in place.'))
  parser.add_option('-s', '--sort-only', action='store_true',
                    help=('Just sort #includes of files listed on cmdline;'
                          ' do not add or remove any #includes'))

  parser.add_option('--dump-iwyu-output', type='str',
                    help=('A path to dump the raw IWYU output to. This can be useful for '
                          'debugging this tool.'))

  (flags, paths) = parser.parse_args(argv[1:])

  if bool(flags.from_git) + bool(flags.all) + (len(paths) > 0) != 1:
    sys.exit('Must specify exactly one of --all, --from-git, or a list of paths')

  do_filtering = True
  if flags.from_git:
    paths = _get_file_list_from_git()
    paths = [os.path.abspath(os.path.join(ROOT, p)) for p in paths]
  elif paths:
    paths = [os.path.abspath(p) for p in paths]
    # If paths are specified explicitly, don't filter them out.
    do_filtering = False
  elif flags.all:
    paths = _filter_paths(_get_paths_from_compilation_db())
  else:
    assert False, "Should not reach here"

  if do_filtering:
    orig_count = len(paths)
    paths = _filter_paths(paths)
    if len(paths) != orig_count:
      logging.info("Filtered %d paths muted by configuration in iwyu.py",
                   orig_count - len(paths))
  else:
    muted_paths = [p for p in paths if _is_muted(p)]
    if muted_paths:
      logging.warning("%d selected path(s) are known to have IWYU issues:" % len(muted_paths))
      for p in muted_paths:
        logging.warning("   %s" % p)

  # If we came up with an empty list (no relevant files changed in the commit)
  # then we should early-exit. Otherwise, we'd end up passing an empty list to
  # IWYU and it will run on every file.
  if flags.from_git and not paths:
    logging.info("No files selected for analysis.")
    sys.exit(0)

  # IWYU output will be relative to the compilation database which is in
  # the build directory. In order for the fixer script to properly find them, we need
  # to treat all paths relative to that directory and chdir into it first.
  paths = _relativize_paths(paths)
  os.chdir(_BUILD_DIR)

  # For correct results, IWYU depends on the generated header files.
  logging.info("Ensuring IWYU dependencies are built...")
  if os.path.exists('Makefile'):
    subprocess.check_call(['make', 'generated-headers'])
  elif os.path.exists('build.ninja'):
    subprocess.check_call(['ninja', 'generated-headers'])
  else:
    logging.error('No Makefile or build.ninja found in build directory %s',
                  _BUILD_DIR)
    sys.exit(1)

  logging.info("Checking %d file(s)...", len(paths))
  if flags.sort_only:
    return _do_sort_only(flags, paths)
  else:
    return _do_iwyu(flags, paths)

if __name__ == "__main__":
  init_logging()
  sys.exit(main(sys.argv))

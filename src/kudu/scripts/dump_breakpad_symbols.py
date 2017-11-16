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
#
# This script can be used to dump symbols using the 'dump_syms' binary, which
# is contained in Google Breakpad. It was forked from Impala's dump_breakpad_symbols.py.
#
# It supports collecting binary files from different sources:
#
#  - Scan a Kudu build dir for ELF files
#  - Read files from stdin
#  - Process a list of one or moreexplicitly specified files
#  - Extract a Kudu rpm/deb, scan for ELF files, and process them together with
#    their respective .debug file.
#
# Dependencies:
#  - dpkg (sudo apt-get -y install dpkg)
#  - rpm2cpio (sudo apt-get -y install rpm2cpio)
#  - cpio (sudo apt-get -y install cpio)
#  - Google Breakpad, either installed via the Kudu thirdparty build or separately
#
# Usage: dump_breakpad_symbols.py -h
#
# Typical usage patterns:
# -----------------------
#
# * Extract symbols from an rpm file:
#   ./dump_breakpad_symbols -d /tmp/syms \
#   -r tmp/kudu-1.4.0+cdh5.12.1+0-1.cdh5.12.1.p0.10.el7.x86_64.rpm \
#   -s tmp/kudu-debuginfo-1.4.0+cdh5.12.1+0-1.cdh5.12.1.p0.10.el7.x86_64.rpm
#
#   Note that this will process all ELF binaries in the rpm, including both debug and
#   release builds. Files are identified by hashes, so you don't need to worry about
#   collisions and you can expect it to 'just work'.
#
# * Scan a Kudu build directory and extract Breakpad symbols from kudu-master
#   and kudu-tserver binaries:
#   ./dump_breakpad_symbols.py -d /tmp/syms -b kudu/build/debug
#
# * After, to process a minidump file, use the 'minidump_stackwalk' tool:
#   $KUDU_HOME/thirdparty/installed/uninstrumented/bin/dump_syms \
#   /tmp/kudu-minidumps/kudu-tserver/03c0ee26-bfd1-cf3e-43fa49ca-1a6aae25.dmp /tmp/syms
#
# For more information, see the getting started docs at
#     http://chromium.googlesource.com/breakpad/breakpad/+/master/docs/getting_started_with_breakpad.md

import errno
import logging
import glob
import magic
import os
import shutil
import subprocess
import sys
import tempfile

from argparse import ArgumentParser
from collections import namedtuple

logging.basicConfig(level=logging.INFO)

BinarySymbolInfo = namedtuple('BinarySymbolInfo', 'path, debug_path')


def die(msg=''):
  """End the process, optionally after printing the passed error message."""
  logging.error('ERROR: %s\n' % msg)
  sys.exit(1)


def find_dump_syms_binary():
  """Locate the 'dump_syms' binary from Breakpad.

  We try to locate the package in the Kudu thirdparty folder.
  TODO: Lookup the binary in the system path. Not urgent, since the user can specify the
  path as a command line switch.
  """
  kudu_home = os.environ.get('KUDU_HOME')
  if kudu_home:
    if not os.path.isdir(kudu_home):
      die('Could not find KUDU_HOME directory')
    # TODO: Use dump_syms_mac if on macOS.
    dump_syms = os.path.join(kudu_home, 'thirdparty', 'installed', 'uninstrumented', 'bin', 'dump_syms')
    if not os.path.isfile(dump_syms):
      die('Could not find dump_syms executable at %s' % dump_syms)
    return dump_syms
  return ''


def parse_args():
  """Parse command line arguments and perform sanity checks."""
  parser = ArgumentParser()
  parser.add_argument('-d', '--dest_dir', required=True, help="""The target directory,
      below which to place extracted symbol files""")
  parser.add_argument('--dump_syms', help='Path to the dump_syms binary from Breakpad')
  # Options controlling how to find input files.
  parser.add_argument('-b', '--build_dir', help="""Path to a directory containing results
      from a Kudu build, e.g. kudu/build/debug""")
  parser.add_argument('-f', '--binary_files', nargs='+', metavar="FILE",
      help='List of binary files to process')
  parser.add_argument('-i', '--stdin_files', action='store_true', help="""Read the list
      of files to process from stdin""")
  parser.add_argument('-r', '--pkg', '--rpm', help="""RPM/DEB file containing the binaries
      to process, use with -s""")
  parser.add_argument('-s', '--symbol_pkg', '--debuginfo_rpm', help="""RPM/DEB file
      containing the debug symbols matching the binaries in -r""")
  args = parser.parse_args()

  # Post processing checks
  # Check that either both pkg and debuginfo_rpm/deb are specified, or none.
  if bool(args.pkg) != bool(args.symbol_pkg):
    parser.print_usage()
    die('Either both -r and -s have to be specified, or none')
  input_flags = [args.build_dir, args.binary_files, args.stdin_files, args.pkg]
  if sum(1 for flag in input_flags if flag) != 1:
    die('Specify exactly one way to locate input files (-b/-f/-i/-r,-s)')

  return args


def ensure_dir_exists(path):
  """Make sure the directory 'path' exists in a thread-safe way."""
  try:
    os.makedirs(path)
  except OSError as e:
    if e.errno != errno.EEXIST or not os.path.isdir(path):
      raise e


def walk_path(path):
  for dirpath, dirnames, filenames in os.walk(path):
    for name in filenames:
      yield os.path.join(dirpath, name)


def is_regular_file(path):
  """Check whether 'path' is a regular file, especially not a symlink."""
  return os.path.isfile(path) and not os.path.islink(path)


def is_elf_file(path):
  """Check whether 'path' is an ELF file."""
  return is_regular_file(path) and 'ELF' in magic.from_file(path)


def find_elf_files(path):
  """Walk 'path' and return a generator over all ELF files below."""
  return (f for f in walk_path(path) if is_elf_file(f))


def extract_rpm(rpm, out_dir):
  """Extract 'rpm' into 'out_dir'."""
  assert os.path.isdir(out_dir)
  cmd = 'rpm2cpio %s | cpio -id' % rpm
  subprocess.check_call(cmd, shell=True, cwd=out_dir)


def extract_deb(deb, out_dir):
  """Extract 'deb' into 'out_dir'."""
  assert os.path.isdir(out_dir)
  cmd = 'dpkg -x %s %s' % (deb, out_dir)
  subprocess.check_call(cmd, shell=True)


def extract_pkg(pkg, out_dir):
  """Autodetect type of 'pkg' and extract it to 'out_dir'."""
  pkg_magic = magic.from_file(pkg)
  if 'RPM' in pkg_magic:
    return extract_rpm(pkg, out_dir)
  elif 'Debian' in pkg_magic:
    return extract_deb(pkg, out_dir)
  else:
    die('Unsupported package type: %s' % pkg_magic)


def assert_file_exists(path):
  if not os.path.isfile(path):
    die('File does not exists: %s' % path)


def enumerate_pkg_files(pkg, symbol_pkg):
  """Return a generator over BinarySymbolInfo tuples for all ELF files in 'pkg'.

  This function extracts both RPM/DEB files, then walks the binary pkg directory to
  enumerate all ELF files, matches them to the location of their respective .debug files
  and yields all tuples thereof. We use a generator here to keep the temporary directory
  and its contents around until the consumer of the generator has finished its processing.
  """
  KUDU_BINARY_BASE = os.path.join('usr', 'lib', 'kudu')
  KUDU_SYMBOL_BASE = os.path.join('usr', 'lib', 'debug', KUDU_BINARY_BASE)
  assert_file_exists(pkg)
  assert_file_exists(symbol_pkg)
  tmp_dir = tempfile.mkdtemp()
  try:
    # Extract pkg
    logging.info('Extracting to %s: %s' % (tmp_dir, pkg))
    extract_pkg(os.path.abspath(pkg), tmp_dir)
    # Extract symbol_pkg
    logging.info('Extracting to %s: %s' % (tmp_dir, symbol_pkg))
    extract_pkg(os.path.abspath(symbol_pkg), tmp_dir)
    # Walk pkg path and find elf files
    binary_base = os.path.join(tmp_dir, KUDU_BINARY_BASE)
    symbol_base = os.path.join(tmp_dir, KUDU_SYMBOL_BASE)
    # Find folder with .debug file in symbol_pkg path
    for binary_path in find_elf_files(binary_base):
      # Add tuple to output
      rel_dir = os.path.relpath(os.path.dirname(binary_path), binary_base)
      debug_dir = os.path.join(symbol_base, rel_dir)
      yield BinarySymbolInfo(binary_path, debug_dir)
  finally:
    shutil.rmtree(tmp_dir)


def enumerate_binaries(args):
  """Enumerate all BinarySymbolInfo tuples, from which symbols should be extracted.

  This function returns iterables, either lists or generators.
  """
  if args.binary_files:
    return (BinarySymbolInfo(f, None) for f in args.binary_files)
  elif args.stdin_files:
    return (BinarySymbolInfo(f, None) for f in sys.stdin.read().splitlines())
  elif args.pkg:
    return enumerate_pkg_files(args.pkg, args.symbol_pkg)
  elif args.build_dir:
    return (BinarySymbolInfo(f, None) for f in find_elf_files(args.build_dir))
  die('No input method provided')


def process_binary(dump_syms, binary, out_dir):
  """Dump symbols of a single binary file and move the result.

  Symbols will be extracted to a temporary file and moved into place afterwards. Required
  directories will be created if necessary.
  """
  logging.info('Processing binary file: %s' % binary.path)
  ensure_dir_exists(out_dir)
  # tmp_fd will be closed when the file object created by os.fdopen() below gets
  # destroyed.
  tmp_fd, tmp_file = tempfile.mkstemp(dir=out_dir, suffix='.sym')
  try:
    # Run dump_syms on the binary.
    args = [dump_syms, binary.path]
    if binary.debug_path:
      args.append(binary.debug_path)
    proc = subprocess.Popen(args, stdout=os.fdopen(tmp_fd, 'wb'), stderr=subprocess.PIPE)
    _, stderr = proc.communicate()
    if proc.returncode != 0:
      sys.stderr.write('Failed to dump symbols from %s, return code %s\n' %
          (binary.path, proc.returncode))
      sys.stderr.write(stderr)
      os.remove(tmp_file)
      return False
    # Parse the temporary file to determine the full target path.
    with open(tmp_file, 'r') as f:
      header = f.readline().strip()
      # Format of header is: MODULE os arch binary_id binary
      _, _, _, binary_id, binary = header.split(' ')
      out_path = os.path.join(out_dir, binary, binary_id)
      ensure_dir_exists(out_path)
    # Move the temporary file to its final destination.
    shutil.move(tmp_file, os.path.join(out_path, '%s.sym' % binary))
  except Exception as e:
    # Only need to clean up in case of errors.
    try:
      os.remove(tmp_file)
    except EnvironmentError:
      pass
    raise e
  return True


def main():
  args = parse_args()
  dump_syms = args.dump_syms or find_dump_syms_binary()
  assert dump_syms
  status = 0
  ensure_dir_exists(args.dest_dir)
  for binary in enumerate_binaries(args):
    if not process_binary(dump_syms, binary, args.dest_dir):
      status = 1
  sys.exit(status)


if __name__ == '__main__':
  main()

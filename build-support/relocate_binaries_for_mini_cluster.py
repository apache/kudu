#!/usr/bin/env python
################################################################################
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
################################################################################
# This script makes Kudu release binaries relocatable for easy use by
# integration tests using a mini cluster. The resulting binaries should never
# be deployed to run an actual Kudu service, whether in production or
# development, because all security dependencies are copied from the build
# system and will not be updated if the operating system on the runtime host is
# patched.
################################################################################

import logging
import optparse
import os
import os.path
import re
import shutil
import subprocess
import sys

from kudu_util import check_output, Colors, init_logging
from dep_extract import DependencyExtractor

# Constants.
LC_RPATH = 'LC_RPATH'
LC_LOAD_DYLIB = 'LC_LOAD_DYLIB'
KEY_CMD = 'cmd'
KEY_NAME = 'name'
KEY_PATH = 'path'

# Exclude libraries that are GPL-licensed and libraries that are not portable
# across Linux kernel versions.
PAT_LINUX_LIB_EXCLUDE = re.compile(r"""(libpthread|
                                        libc|
                                        libstdc\+\+|
                                        librt|
                                        libdl|
                                        libgcc.*
                                       )\.so""", re.VERBOSE)

# We don't want to ship libSystem because it includes kernel and thread
# routines that we assume may not be portable between macOS versions.
# TODO(mpercy): consider excluding libc++ as well.
PAT_MACOS_LIB_EXCLUDE = re.compile(r"libSystem")

# Config keys.
BUILD_ROOT = 'build_root'
BUILD_BIN_DIR = 'build_bin_dir'
ARTIFACT_ROOT = 'artifact_root'
ARTIFACT_BIN_DIR = 'artifact_bin_dir'
ARTIFACT_LIB_DIR = 'artifact_lib_dir'

SOURCE_ROOT = os.path.join(os.path.dirname(__file__), "..")

IS_MACOS = os.uname()[0] == "Darwin"
IS_LINUX = os.uname()[0] == "Linux"

def check_for_command(command):
  """
  Ensure that the specified command is available on the PATH.
  """
  try:
    subprocess.check_call(['which', command])
  except subprocess.CalledProcessError as err:
    logging.error("Unable to find %s command", command)
    raise err

def objdump_private_headers(binary_path):
  """
  Run `objdump -p` on the given binary.
  Returns a list with one line of objdump output per record.
  """

  check_for_command('objdump')
  try:
    output = check_output(["objdump", "-p", binary_path])
  except subprocess.CalledProcessError as err:
    logging.error(err)
    return []
  return output.strip().decode("utf-8").split("\n")

def parse_objdump_macos(cmd_type, dump):
  """
  Parses the output from objdump_private_headers() for macOS.
  'cmd_type' must be one of the following:
  * LC_RPATH: Returns a list containing the rpath search path, with one
    search path per entry.
  * LC_LOAD_DYLIB: Returns a list of shared object library dependencies, with
    one shared object per entry. They are returned as stored in the MachO
    header, without being first resolved to an absolute path, and may look
    like: @rpath/Foo.framework/Versions/A/Foo
  'dump' is the output from objdump_private_headers().
  """
  # Parsing state enum values.
  PARSING_NONE = 0
  PARSING_NEW_RECORD = 1
  PARSING_RPATH = 2
  PARSING_LIB_PATHS = 3

  state = PARSING_NONE
  values = []
  for line in dump:
    if re.match('^Load command', line):
      state = PARSING_NEW_RECORD
      continue
    splits = re.split('\s+', line.strip().decode("utf-8"), maxsplit=2)
    key = splits[0]
    val = splits[1] if len(splits) > 1 else None
    if state == PARSING_NEW_RECORD:
      if key == KEY_CMD and val == LC_RPATH:
        state = PARSING_RPATH
        continue
      if key == KEY_CMD and val == LC_LOAD_DYLIB:
        state = PARSING_LIB_PATHS
        continue

    if state == PARSING_RPATH and cmd_type == LC_RPATH:
      if key == KEY_PATH:
        # Strip trailing metadata from rpath dump line.
        values.append(val)

    if state == PARSING_LIB_PATHS and cmd_type == LC_LOAD_DYLIB:
      if key == KEY_NAME:
        values.append(val)
  return values

def get_rpaths_macos(binary_path):
  """
  Helper function that returns a list of rpaths parsed from the given binary.
  """
  dump = objdump_private_headers(binary_path)
  return parse_objdump_macos(LC_RPATH, dump)

def resolve_library_paths_macos(raw_library_paths, rpaths):
  """
  Resolve the library paths from parse_objdump_macos(LC_LOAD_DYLIB, ...) to
  absolute filesystem paths using the rpath information returned from
  get_rpaths_macos().
  Returns a mapping from original to resolved library paths on success.
  If any libraries cannot be resolved, prints an error to stderr and returns
  an empty map.
  """
  resolved_paths = {}
  for raw_lib_path in raw_library_paths:
    if not raw_lib_path.startswith("@rpath"):
      resolved_paths[raw_lib_path] = raw_lib_path
      continue
    resolved = False
    for rpath in rpaths:
      resolved_path = re.sub('@rpath', rpath, raw_lib_path)
      if os.path.exists(resolved_path):
        resolved_paths[raw_lib_path] = resolved_path
        resolved = True
        break
    if not resolved:
      raise FileNotFoundError("Unable to locate library %s in rpath %s" % (raw_lib_path, rpaths))
  return resolved_paths

def get_dep_library_paths_macos(binary_path):
  """
  Returns a map of symbolic to resolved library dependencies of the given binary.
  See resolve_library_paths_macos().
  """
  dump = objdump_private_headers(binary_path)
  raw_library_paths = parse_objdump_macos(LC_LOAD_DYLIB, dump)
  rpaths = parse_objdump_macos(LC_RPATH, dump)
  return resolve_library_paths_macos(raw_library_paths, rpaths)

def get_artifact_name():
  """
  Read the Kudu version to create an archive with an appropriate name.
  """
  with open(os.path.join(SOURCE_ROOT, "version.txt"), 'r') as version:
    version = version.readline().strip().decode("utf-8")
  artifact_name = "apache-kudu-%s" % (version, )
  return artifact_name

def mkconfig(build_root, artifact_root):
  """
  Build a configuration map for convenient plumbing of path information.
  """
  config = {}
  config[BUILD_ROOT] = build_root
  config[BUILD_BIN_DIR] = os.path.join(build_root, "bin")
  config[ARTIFACT_ROOT] = artifact_root
  config[ARTIFACT_BIN_DIR] = os.path.join(artifact_root, "bin")
  config[ARTIFACT_LIB_DIR] = os.path.join(artifact_root, "lib")
  return config

def prep_artifact_dirs(config):
  """
  Create any required artifact output directories, if needed.
  """

  if not os.path.exists(config[ARTIFACT_ROOT]):
    os.makedirs(config[ARTIFACT_ROOT], mode=0755)
  if not os.path.exists(config[ARTIFACT_BIN_DIR]):
    os.makedirs(config[ARTIFACT_BIN_DIR], mode=0755)
  if not os.path.exists(config[ARTIFACT_LIB_DIR]):
    os.makedirs(config[ARTIFACT_LIB_DIR], mode=0755)

def copy_file(src, dest):
  """
  Copy the file with path 'src' to path 'dest', including the file mode.
  If 'src' is a symlink, the link will be followed and 'dest' will be written
  as a plain file.
  """
  shutil.copyfile(src, dest)
  shutil.copymode(src, dest)

def chrpath(target, new_rpath):
  """
  Change the RPATH or RUNPATH for the specified target. See man chrpath(1).
  """

  # Continue with a warning if no rpath is set on the binary.
  try:
    subprocess.check_call(['chrpath', '-l', target])
  except subprocess.CalledProcessError as err:
    logging.warning("No RPATH or RUNPATH set on target %s, continuing...", target)
    return

  # Update the rpath.
  try:
    subprocess.check_call(['chrpath', '-r', new_rpath, target])
  except subprocess.CalledProcessError as err:
    logging.warning("Failed to chrpath for target %s", target)
    raise err

def relocate_deps_linux(target_src, target_dst, config):
  """
  See relocate_deps(). Linux implementation.
  """
  NEW_RPATH = '$ORIGIN/../lib'

  # Make sure we have the chrpath command available in the Linux build.
  check_for_command('chrpath')

  # Copy the linked libraries.
  dep_extractor = DependencyExtractor()
  dep_extractor.set_library_filter(lambda path: False if PAT_LINUX_LIB_EXCLUDE.search(path) else True)
  libs = dep_extractor.extract_deps(target_src)
  for lib_src in libs:
    lib_dst = os.path.join(config[ARTIFACT_LIB_DIR], os.path.basename(lib_src))
    copy_file(lib_src, lib_dst)
    # We have to set the RUNPATH of the shared objects as well for transitive
    # dependencies to be properly resolved. $ORIGIN is always relative to the
    # running executable.
    chrpath(lib_dst, NEW_RPATH)

  # We must also update the RUNPATH of the executable itself to look for its
  # dependencies in a relative location.
  chrpath(target_dst, NEW_RPATH)

def relocate_deps_macos(target_src, target_dst, config):
  """
  See relocate_deps(). macOS implementation.
  """
  libs = get_dep_library_paths_macos(target_src)

  check_for_command('install_name_tool')

  for (search_name, resolved_path) in libs.iteritems():
    # Filter out libs we don't want to archive.
    if PAT_MACOS_LIB_EXCLUDE.search(resolved_path):
      continue

    # Archive the rest of the runtime dependencies.
    lib_dst = os.path.join(config[ARTIFACT_LIB_DIR], os.path.basename(resolved_path))
    copy_file(resolved_path, lib_dst)

    # Change library search path or name for each archived library.
    modified_search_name = re.sub('^.*/', '@rpath/', search_name)
    subprocess.check_call(['install_name_tool', '-change',
                search_name, modified_search_name, target_dst])
  # Modify the rpath.
  rpaths = get_rpaths_macos(target_src)
  for rpath in rpaths:
    subprocess.check_call(['install_name_tool', '-delete_rpath', rpath, target_dst])
  subprocess.check_call(['install_name_tool', '-add_rpath', '@executable_path/../lib',
                         target_dst])

def relocate_deps(target_src, target_dst, config):
  """
  Make the target relocatable and copy all of its dependencies into the
  artifact directory.
  """
  if IS_LINUX:
    return relocate_deps_linux(target_src, target_dst, config)
  if IS_MACOS:
    return relocate_deps_macos(target_src, target_dst, config)
  raise NotImplementedError("Unsupported platform")

def relocate_target(target, config):
  """
  Copy all dependencies of the executable referenced by 'target' from the
  build directory into the artifact directory, and change the rpath of the
  executable so that the copied dependencies will be found when the executable
  is invoked.
  """

  # Create artifact directories, if needed.
  prep_artifact_dirs(config)

  # Copy the target into the artifact directory.
  target_src = os.path.join(config[BUILD_BIN_DIR], target)
  target_dst = os.path.join(config[ARTIFACT_BIN_DIR], target)
  copy_file(target_src, target_dst)

  # Make the target relocatable and copy all of its dependencies into the
  # artifact directory.
  return relocate_deps(target_src, target_dst, config)

def main():
  if len(sys.argv) < 3:
    print("Usage: %s kudu_build_dir target [target ...]" % (sys.argv[0], ))
    sys.exit(1)

  # Command-line arguments.
  build_root = sys.argv[1]
  targets = sys.argv[2:]

  init_logging()

  if not os.path.exists(build_root):
    logging.error("Build directory %s does not exist", build_root)
    sys.exit(1)

  artifact_name = get_artifact_name()
  artifact_root = os.path.join(build_root, artifact_name)

  logging.info("Including targets and their dependencies in archive...")
  config = mkconfig(build_root, artifact_root)
  for target in targets:
    relocate_target(target, config)

if __name__ == "__main__":
  main()

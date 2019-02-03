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

import errno
import logging
import optparse
import os
import os.path
import re
import shutil
import subprocess
import sys

SOURCE_ROOT = os.path.join(os.path.dirname(__file__), "../..")
# Add the build-support dir to the system path so we can import kudu-util.
sys.path.append(os.path.join(SOURCE_ROOT, "build-support"))

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

IS_MACOS = os.uname()[0] == "Darwin"
IS_LINUX = os.uname()[0] == "Linux"

def check_for_command(command):
  """
  Ensure that the specified command is available on the PATH.
  """
  try:
    _ = subprocess.check_output(['which', command])
  except subprocess.CalledProcessError as err:
    logging.error("Unable to find %s command", command)
    raise err

def dump_load_commands_macos(binary_path):
  """
  Run `otool -l` on the given binary.
  Returns a list with one line of otool output per entry.
  We use 'otool -l' instead of 'objdump -p' because 'otool' supports Universal
  Mach-O binaries.
  """

  check_for_command('otool')
  try:
    output = check_output(["otool", "-l", binary_path])
  except subprocess.CalledProcessError as err:
    logging.error("Failed to run %s", err.cmd)
    raise err
  return output.strip().decode("utf-8").split("\n")

def parse_load_commands_macos(cmd_type, dump):
  """
  Parses the output from dump_load_commands_macos() for macOS.
  'cmd_type' must be one of the following:
  * LC_RPATH: Returns a list containing the rpath search path, with one
    search path per entry.
  * LC_LOAD_DYLIB: Returns a list of shared object library dependencies, with
    one shared object per entry. They are returned as stored in the MachO
    header, without being first resolved to an absolute path, and may look
    like: @rpath/Foo.framework/Versions/A/Foo
  'dump' is the output from dump_load_commands_macos().
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
  dump = dump_load_commands_macos(binary_path)
  return parse_load_commands_macos(LC_RPATH, dump)

def resolve_library_paths_macos(raw_library_paths, rpaths):
  """
  Resolve the library paths from parse_load_commands_macos(LC_LOAD_DYLIB, ...) to
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
      raise IOError(errno.ENOENT, "Unable to locate library %s in rpath %s" % (raw_lib_path, rpaths))
  return resolved_paths

def get_resolved_dep_library_paths_macos(binary_path):
  """
  Returns a map of symbolic to resolved library dependencies of the given binary.
  See resolve_library_paths_macos().
  """
  load_commands = dump_load_commands_macos(binary_path)
  lib_search_paths = parse_load_commands_macos(LC_LOAD_DYLIB, load_commands)
  rpaths = parse_load_commands_macos(LC_RPATH, load_commands)
  return resolve_library_paths_macos(lib_search_paths, rpaths)

def get_artifact_name():
  """
  Create an archive with an appropriate name. Including version, OS, and architecture.
  """
  if IS_LINUX:
    os_str = "linux"
  elif IS_MACOS:
    os_str = "osx"
  else:
    raise NotImplementedError("Unsupported platform")
  arch = os.uname()[4]
  with open(os.path.join(SOURCE_ROOT, "version.txt"), 'r') as version:
    version = version.readline().strip().decode("utf-8")
  artifact_name = "kudu-binary-%s-%s-%s" % (version, os_str, arch)
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
  Copy the file with path 'src' to path 'dest'.
  If 'src' is a symlink, the link will be followed and 'dest' will be written
  as a plain file.
  """
  shutil.copyfile(src, dest)

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

def fix_rpath_macos(target_dst):
  check_for_command('install_name_tool')
  rpaths = get_rpaths_macos(target_dst)
  for rpath in rpaths:
    subprocess.check_call(['install_name_tool', '-delete_rpath', rpath, target_dst])
  subprocess.check_call(['install_name_tool', '-add_rpath', '@executable_path/../lib',
                         target_dst])

def relocate_dep_path_macos(target_dst, dep_search_name):
  """
  Change library search path to @rpath for the specified search named in the
  specified binary.
  """
  modified_search_name = re.sub('^.*/', '@rpath/', dep_search_name)
  subprocess.check_call(['install_name_tool', '-change',
                        dep_search_name, modified_search_name, target_dst])

def relocate_deps_macos(target_src, target_dst, config):
  """
  See relocate_deps(). macOS implementation.
  """
  target_deps = get_resolved_dep_library_paths_macos(target_src)

  check_for_command('install_name_tool')

  # Modify the rpath of the target.
  fix_rpath_macos(target_dst)

  # For each dependency, relocate the path we will search for it and ensure it
  # is shipped with the archive.
  for (dep_search_name, dep_src) in target_deps.iteritems():
    # Filter out libs we don't want to archive.
    if PAT_MACOS_LIB_EXCLUDE.search(dep_search_name):
      continue

    # Change the search path of the specified dep in 'target_dst'.
    relocate_dep_path_macos(target_dst, dep_search_name)

    # Archive the rest of the runtime dependencies.
    dep_dst = os.path.join(config[ARTIFACT_LIB_DIR], os.path.basename(dep_src))
    if not os.path.isfile(dep_dst):
      # Recursively copy and relocate library dependencies as they are found.
      copy_file(dep_src, dep_dst)
      relocate_deps_macos(dep_src, dep_dst, config)

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
  # Clear the artifact root to ensure a clean build.
  if os.path.exists(artifact_root):
    shutil.rmtree(artifact_root)

  logging.info("Including targets and their dependencies in archive...")
  config = mkconfig(build_root, artifact_root)
  for target in targets:
    relocate_target(target, config)

if __name__ == "__main__":
  main()

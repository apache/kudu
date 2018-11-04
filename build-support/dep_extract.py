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

import logging
import os
import re
import subprocess

# Matches the output lines from the 'ldd' tool. For example:
#   libcrypto.so.10 => /path/to/usr/lib64/libcrypto.so.10 (0x00007fb0cb0a5000)
#
# Note: The following pattern will not match the following two types of
# dependencies and so they will not be included in the output from this module:
#
# 1. The dynamic linker:
#    /lib64/ld-linux-x86-64.so.2 (0x00007f6f7ab79000)
# 2. Linux virtual dynamic shared objects:
#    linux-vdso.so.1 (0x00007ffc06cfb000)
#
LDD_RE = re.compile(r'^\s+.+? => (\S+) \(0x.+\)')

class DependencyExtractor(object):
  """
  This class extracts native library dependencies from the given executable.
  """
  def __init__(self):
    self.deps_cache = {}
    self.lib_allowed_filter = lambda path: True
    self.enable_expand_symlinks = False

  def set_library_filter(self, lib_allowed_filter):
    """
    Specify a filter predicate that should return True iff the specified
    library path should be included in the result from extract_deps().
    By default, all libraries are included in the result.
    """
    self.lib_allowed_filter = lib_allowed_filter

  def set_expand_symlinks(self, expand):
    """
    Specify whether symlinks should be expanded in the output from
    extract_deps(). By default, symlinks are not expanded. See
    expand_symlinks().
    """
    self.enable_expand_symlinks = expand

  def expand_symlinks(self, deps):
    """
    ldd will often point to symlinks. Return a list including any symlink in
    the specified dependency list as well as whatever it's pointing to,
    recursively.
    """
    expanded = []
    for path in deps:
      expanded.append(path)
      while os.path.islink(path):
        # TODO(mpercy): os.readlink() can return an absolute path. Should we more carefully handle
        # the path concatenation here?
        path = os.path.join(os.path.dirname(path), os.readlink(path))
        expanded.append(path)
    return expanded

  def extract_deps(self, exe):
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

    if exe not in self.deps_cache:
      p = subprocess.Popen(["ldd", exe], stdout=subprocess.PIPE)
      out, err = p.communicate()
      self.deps_cache[exe] = (out, err, p.returncode)

    out, err, rc = self.deps_cache[exe]
    if rc != 0:
      logging.warning("failed to run ldd on %s", exe)
      return []

    deps = []
    for line in out.splitlines():
      match = LDD_RE.match(line)
      if not match:
        continue
      dep = match.group(1)
      # Apply the provided predicate.
      if not self.lib_allowed_filter(dep):
        continue
      deps.append(dep)

    if self.enable_expand_symlinks:
      deps = self.expand_symlinks(deps)
    return deps

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
"""
Simple script which checks that the build system has the main
dependencies necessary for building thirdparty/.

This is not meant to be exhaustive. For example, it doesn't check specific
version numbers of software. Nor is it meant to be a replacement for
more complicated config tests using something like autoconf or cmake.
But, it's simple enough that it runs with no dependencies and tells
users if they're missing anything obvious.
"""

import os
import platform
import sys
import subprocess

DEV_NULL = open("/dev/null", "wb")
CC = os.environ.get("CC", "cc")
CXX = os.environ.get("CXX", "c++")

REQUIRED_TOOLS = [
  "autoconf",
  "automake",
  "curl",
  "git",
  "make",
  "patch",
  "pkg-config",
  "rsync",
  "unzip",
  "xxd"]

if platform.system() == "Linux":
  # Modern Linux distros split libtool functionality into two packages:
  # libtool-bin (containing 'libtool') and libtool (containing 'libtoolize').
  # Only the latter is needed for a working autoconf.
  REQUIRED_TOOLS.append("libtoolize")
else:
  REQUIRED_TOOLS.append("libtool")

def log_failure_and_exit(error_msg, exc=None):
  print("*** " + error_msg)
  if exc:
    print(exc.message)
    print("")
  print("Please refer to docs/installation.adoc for complete installation ")
  print("instructions for your platform.")
  sys.exit(1)


def try_do(status_msg, error_msg, func):
  print(status_msg + " ...")
  try:
    func()
  except Exception as e:
    log_failure_and_exit(error_msg, e)


def compile(script, flags=None):
  if flags is None:
    flags = []

  p = subprocess.Popen([CXX, '-o', '/dev/null', '-c', '-x', 'c++', '-'] + flags,
      stderr=subprocess.PIPE,
      stdout=subprocess.PIPE,
      stdin=subprocess.PIPE,
      universal_newlines=True)

  stdout, stderr = p.communicate(input=script)
  if p.returncode != 0:
    raise Exception("Compilation failed: " + stderr)


def check_tools():
  """ Check that basic build tools are installed. """
  missing = []
  for tool in REQUIRED_TOOLS:
    print("Checking for " + tool)
    if subprocess.call(["which", tool], stdout=DEV_NULL, stderr=DEV_NULL) != 0:
      missing.append(tool)
  if missing:
    log_failure_and_exit("Missing required tools:\n" +
      "\n".join("  " + tool for tool in missing))


def check_cxx11():
  # Check that the compiler is new enough.
  try_do(
    "Checking for C++11 compiler support",
    ("Unable to compile a simple c++11 program. " +
     "Please use g++ 4.8 or higher. On CentOS 6 or RHEL 6 " +
     "you must use the devtoolset. See docs/installation.adoc " +
     "for more info."),
    lambda: compile("""
      #include <atomic>
      int f() {
        std::atomic<int> i;
        return i.load();
      }""",
      flags=['--std=c++11']))


def check_sasl():
  try_do(
    "Checking for cyrus-sasl headers",
    ("Unable to compile a simple program that uses sasl. " +
     "Please check that cyrus-sasl-devel (RPM) or libsasl2-dev (deb) " +
     "dependencies are installed."),
    lambda: compile("""
      #include <sasl/sasl.h>
      """,
      flags=["-E"]))


def main():
  print("Running pre-flight checks")
  print("-------------------------")
  print("Using C compiler: " + CXX)
  print("Using C++ compiler: " + CXX)
  print("")
  print("  (Set $CC and $CXX to change compiler)")
  print("-------------------------")
  check_tools()
  check_cxx11()
  check_sasl()
  print("-------------")
  print("Pre-flight checks succeeded.")
  return 0

if __name__ == "__main__":
  sys.exit(main())

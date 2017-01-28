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
Script which performs some sanity checks on the built thirdparty/.
"""

import os
import subprocess
import sys

TP_DIR = os.path.dirname(os.path.realpath(__file__))

def shell(status_msg, script):
  to_print = status_msg + " ..."

  p = subprocess.Popen(["/bin/bash"],
      stderr=subprocess.PIPE,
      stdout=subprocess.PIPE,
      stdin=subprocess.PIPE,
      universal_newlines=True)
  stdout, _ = p.communicate(input=script)
  if p.returncode != 0:
    to_print += " FAILED\n"
    for line in stdout.strip().split("\n"):
      to_print += "==> " + line + "\n"
  else:
    to_print += " PASSED"
  print(to_print)

  if p.returncode != 0:
    sys.exit(1)


def check_tsan_dependencies():
  shell(
    "Checking that no TSAN dependency depends on libstdc++",
"""
if [[ "$OSTYPE" == "darwin"* ]]; then
  echo No TSAN support on macOS, skipping test
  exit 0
fi

lib_dir="{tp_dir}/installed/tsan/lib"
echo Looking for TSAN dependencies directory $lib_dir
if [ ! -d "$lib_dir" ]; then
  echo Could not find TSAN dependencies directory $lib_dir, skipping test
  exit 0
fi
echo Found TSAN dependencies directory $lib_dir

echo Looking for ldd
if ! $(which ldd > /dev/null 2>&1); then
  echo Could not find ldd
  exit 1
fi
echo Found ldd

echo Checking dependencies
for so in $lib_dir/*.so; do
  found=$(ldd $so 2>/dev/null | grep -q libstdc++ && echo $so)
  if [ -n "$found" ]; then
    echo Found bad dependency: $found
    exit 1
  fi
done

echo All TSAN dependencies checked
""".format(tp_dir=TP_DIR))

def main():
  print("Running post-flight checks")
  print("-------------------------")
  if "tsan" in sys.argv:
    check_tsan_dependencies()
  print("-------------------------")
  print("Post-flight checks succeeded.")
  return 0

if __name__ == "__main__":
  sys.exit(main())

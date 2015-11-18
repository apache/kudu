#!/usr/bin/env python
# Copyright 2015 Cloudera, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#
# This script generates a header file which contains definitions
# for the current Kudu build (eg timestamp, git hash, etc)

import subprocess

def check_output(cmd, **kwargs):
  """ Simple backport of subprocess.check_output() from python 2.7. """
  p = subprocess.Popen(cmd,
                       stdout=subprocess.PIPE,
                       stderr=subprocess.PIPE,
                       **kwargs)
  out, err = p.communicate()
  if p.returncode != 0:
    raise Exception("%s returned %d: %s" % (cmd, p.returncode, err))
  return out


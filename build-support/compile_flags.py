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

import os
from os.path import join

ROOT = join(os.path.dirname(__file__), "..")

def get_flags():
    """
    Return the set of flags that are used during compilation.

    TODO(todd) it would be nicer to somehow grab these from CMake, but it's
    not clear how to do so.
    """
    return [
        '-x',
        'c++',
        '-DKUDU_HEADERS_NO_STUBS=1',
        '-DKUDU_HEADERS_USE_RICH_SLICE=1',
        '-DKUDU_HEADERS_USE_SHORT_STATUS_MACROS=1',
        '-DKUDU_STATIC_DEFINE',
        '-D__STDC_FORMAT_MACROS',
        '-fno-strict-aliasing',
        '-msse4.2',
        '-Wall',
        '-Wno-sign-compare',
        '-Wno-deprecated',
        '-pthread',
        '-ggdb',
        '-Qunused-arguments',
        '-Wno-ambiguous-member-template',
        '-std=c++11',
        '-g',
        '-fPIC',
        '-I',
        join(ROOT, 'src'),
        '-I',
        join(ROOT, 'build/latest/src'),
        '-isystem',
        join(ROOT, 'thirdparty/installed/common/include'),
        '-isystem',
        join(ROOT, 'thirdparty/installed/uninstrumented/include'),
    ]

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

# Cython is a pre-requirement to be able to run this setup.py script.
# By putting this part before the Cython imports there is no need to
# install Cython manually before the installation.

import sys
import subprocess
import pkg_resources

cython_version = "Cython==0.29.37"

# Function to install a package via pip
def install(package):
    subprocess.check_call([sys.executable, "-m", "pip", "install", package])

# Ensure the correct version of Cython is installed before proceeding
try:
    pkg_resources.require(cython_version)
except (pkg_resources.DistributionNotFound, pkg_resources.VersionConflict):
    sys.stdout.write("Installing Cython...")
    install(cython_version)

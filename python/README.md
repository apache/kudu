<!-- Licensed to the Apache Software Foundation (ASF) under one
or more contributor license agreements.  See the NOTICE file
distributed with this work for additional information
regarding copyright ownership.  The ASF licenses this file
to you under the Apache License, Version 2.0 (the
"License"); you may not use this file except in compliance
with the License.  You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing,
software distributed under the License is distributed on an
"AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
KIND, either express or implied.  See the License for the
specific language governing permissions and limitations
under the License. -->

# kudu-python: Python interface to the Apache Kudu C++ Client API

Using this package requires that you install the Kudu C++ client libraries and
headers. See https://kudu.apache.org for more.

## System Dependencies

Before installing the Python client, you need to install OS-specific system dependencies.

The Kudu Python client requires several packages that vary by operating system (Debian/Ubuntu, CentOS/RHEL, OpenSUSE/SLES). These include build tools like `setuptools` and the Python development package.

For automated installation of these dependencies, you can use the bootstrap script: https://github.com/apache/kudu/blob/master/docker/bootstrap-python-env.sh

## Virtual Environment

**Note: It is recommended to use a Python virtual environment to avoid conflicts with system packages.**

If you don't have virtualenv installed, you can learn more and install it from [the official virtualenv website](https://virtualenv.pypa.io/en/latest/index.html).

## Setting up a Virtual Environment
```bash
# Create a virtual environment
# You can use any Python version supported by kudu-python (check https://pypi.org/project/kudu-python/)
virtualenv venv -p 3.8

# Activate the virtual environment
source venv/bin/activate

# To deactivate later:
deactivate
```

## Installing from PyPI

```bash
pip install kudu-python
```

## Installing from Source

**Note: Make sure you are in the `kudu/python` directory where the requirements files are located.**

```bash
cd /path/to/kudu/python  # Navigate to the python directory if not already there
pip install -r requirements.txt
python setup.py sdist
pip install dist/kudu-python-*.tar.gz
```

## Building for Development

### Setting up the KUDU_HOME Environment Variable

Before building for development, you need to set the `KUDU_HOME` environment variable to point to the root directory of your Kudu git repository:

```bash
export KUDU_HOME=/path/to/kudu
```

This variable is required by various scripts and tools in the project. Make sure it's set in your environment before running any Kudu-related commands.

**Note: Make sure you are in the `kudu/python` directory where the requirements files are located.**

```bash
cd $KUDU_HOME/python  # Navigate to the python directory if not already there
pip install -r requirements.txt
pip install -r requirements_dev.txt
python setup.py build_ext --inplace
```

## Run All Tests

```bash
python setup.py test
```

## Run Single Test

```bash
python -m unittest kudu.tests.test_client.TestClient.test_list_tables
```

## Debugging A Test

To debug a specific test using Python's built-in debugger (pdb):

```bash
python -m pdb -m unittest kudu.tests.test_client.TestClient.test_list_tables
```

This will start the debugger before running the test, allowing you to set breakpoints and step through the code.

## Using the Makefile

A Makefile is provided to simplify common development tasks. It includes targets for building, installing dependencies, running tests, and cleaning the project.

```bash
# Install all dependencies
make requirements

# Build the extension in-place
make build

# Run all tests
make test

# Run a specific test
make test TEST=kudu.tests.test_client.TestClient.test_list_tables

# Clean build artifacts
make clean
```

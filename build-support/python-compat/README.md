<!---
Licensed to the Apache Software Foundation (ASF) under one
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
under the License.
-->

# python-compat

Test infrastructure for verifying the Kudu Python client builds, passes its
test suite, and packages correctly across all supported Python versions.

Useful when:
- A new Python version is added or dropped
- Preparing a release

## Scripts

| Script | What it does |
|---|---|
| `bootstrap-deadsnakes-python.sh` | Installs all supported Python interpreters via the deadsnakes PPA (Ubuntu/Debian) |
| `test-client.sh` | For each version: builds the Cython extension and runs the test suite |
| `test-packaging.sh` | For each version: builds an sdist and installs it in a clean venv to verify packaging |

The list of supported versions is derived from the trove classifiers in
`python/setup.py` — that is the single source of truth.

## Usage

Run once to set up interpreters, then run the test scripts:

```bash
# 1. Install all supported Python interpreters
./bootstrap-deadsnakes-python.sh

# 2. Verify the client builds and tests pass
./test-client.sh

# 3. Verify sdist packaging round-trip
./test-packaging.sh
```

To test a specific version or subset:

```bash
./test-client.sh 3.12
./test-client.sh 3.11 3.12 3.13
```

## Prerequisites

- Ubuntu/Debian with `sudo` and `apt-get`
- `virtualenv` on PATH (`pip install virtualenv`)
- Kudu built with `KUDU_HOME` pointing to the repo root (or inferred automatically)

<!---
Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
-->

# Basic Kudu-Python Example
This is a very basic example of usage for the Kudu Python client.
It demonstrates much of the standard capabilities within the client.

## To install the Python client

### Building from source
_NOTE:_ This example is pointing to the latest build, which is often
times a debug build. If this is not the desired approach, you will
need to point this to the release build.
```
export KUDU_HOME=/path/to/kudu
export LD_LIBRARY_PATH=$KUDU_HOME/build/latest/lib/exported
# For OS X
export DYLD_LIBRARY_PATH=$KUDU_HOME/build/latest/lib/exported
pip install -r requirements.txt
python setup.py build_ext --inplace
python setup.py install
```

### Installing from pypi
_NOTE:_ This example is pointing to the latest build, which is often
times a debug build. If this is not the desired approach, you will
need to point this to the release build.
```
export KUDU_HOME=/path/to/kudu
export LD_LIBRARY_PATH=$KUDU_HOME/build/latest/lib/exported
# For OS X
export DYLD_LIBRARY_PATH=$KUDU_HOME/build/latest/lib/exported
pip install kudu-python
```

## Running the example

```
./basic_example.py --masters master1.address --ports 7051
```
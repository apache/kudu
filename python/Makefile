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

.PHONY: all build requirements test install clean test-examples

build:
	python setup.py build_ext --inplace

all: build

requirements:
	pip install -r requirements.txt
	pip install -r requirements_dev.txt

test:
ifdef TEST
	python -m unittest $(TEST)
else
	python setup.py test
endif

# Run Python client examples against a local Kudu cluster.
# Optional: PYTHON=python2|python3 (default: python3)
# Optional: EXAMPLE=<example_name.py> (default: all examples)
test-examples:
ifndef PYTHON
	$(eval PYTHON := python3)
endif
ifdef EXAMPLE
	./test-python-examples.sh $(PYTHON) $(EXAMPLE)
else
	./test-python-examples.sh $(PYTHON)
endif

clean:
	find . -name "*.pyc" -exec rm -rf {} \;
	find . -name "*.so" -exec rm -rf {} \;
	rm -rf build/
	rm -rf __pycache__/
	rm -f kudu/client.cpp
	rm -f kudu/errors.cpp
	rm -f kudu/schema.cpp
	rm -f kudu/config.pxi
	rm -f kudu/version.py

install:
	python setup.py install
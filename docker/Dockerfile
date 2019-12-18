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

# This docker file defines a multistage build that supports creating
# various docker images for Apache Kudu development.
#
# Note: When editing this file, please follow the best practices laid out here:
#   https://docs.docker.com/develop/develop-images/dockerfile_best-practices
#
# Note: This file uses the shared label namespace for common labels. See:
#   http://label-schema.org/rc1/

#
# ---- Runtime ----
# Builds a base image that has all the runtime libraries for Kudu pre-installed.
#
ARG BASE_OS
FROM $BASE_OS as runtime

COPY ./docker/bootstrap-runtime-env.sh /
RUN ./bootstrap-runtime-env.sh && rm bootstrap-runtime-env.sh

# Common label arguments.
# VCS_REF is not specified to improve docker caching.
ARG DOCKERFILE
ARG MAINTAINER
ARG URL
ARG VCS_TYPE
ARG VCS_URL
ARG VERSION

LABEL org.label-schema.name="Apache Kudu Runtime Base" \
      org.label-schema.description="A base image that has all the runtime \
        libraries for Kudu pre-installed." \
      # Common labels.
      org.label-schema.dockerfile=$DOCKERFILE \
      org.label-schema.maintainer=$MAINTAINER \
      org.label-schema.url=$URL \
      org.label-schema.vcs-type=$VCS_TYPE \
      org.label-schema.vcs-url=$VCS_URL \
      org.label-schema.version=$VERSION

#
# ---- Dev ----
# Builds a base image that has all the development libraries for Kudu pre-installed.
#
ARG BASE_OS
FROM $BASE_OS as dev

COPY ./docker/bootstrap-dev-env.sh /
COPY ./docker/bootstrap-python-env.sh /
RUN ./bootstrap-dev-env.sh \
  && ./bootstrap-python-env.sh \
  && rm bootstrap-dev-env.sh \
  && rm bootstrap-python-env.sh

# Common label arguments.
# VCS_REF is not specified to improve docker caching.
ARG DOCKERFILE
ARG MAINTAINER
ARG URL
ARG VCS_TYPE
ARG VCS_URL
ARG VERSION

LABEL org.label-schema.name="Apache Kudu Development Base" \
      org.label-schema.description="A base image that has all the development \
        libraries for Kudu pre-installed." \
      # Common labels.
      org.label-schema.dockerfile=$DOCKERFILE \
      org.label-schema.maintainer=$MAINTAINER \
      org.label-schema.url=$URL \
      org.label-schema.vcs-type=$VCS_TYPE \
      org.label-schema.vcs-url=$VCS_URL \
      org.label-schema.version=$VERSION

#
# ---- Thirdparty ----
# Builds an image that has Kudu's thirdparty dependencies built.
# This is done in its own stage so that docker can cache it and only
# run it when thirdparty has changes.
#
FROM dev AS thirdparty

WORKDIR /kudu
# We only copy the needed files for thirdparty so docker can handle caching.
COPY ./thirdparty thirdparty
COPY ./build-support/enable_devtoolset.sh \
  ./build-support/enable_devtoolset_inner.sh \
  build-support/
RUN build-support/enable_devtoolset.sh \
  thirdparty/build-if-necessary.sh \
  # Remove the files left behind that we don't need.
  # Remove all the source files except the hive, hadoop, and sentry sources
  # which are pre-built and symlinked into the installed/common/opt directory.
  && find thirdparty/src/* -maxdepth 0 -type d  \
    \( ! -name 'hadoop-*' ! -name 'hive-*' ! -name 'apache-sentry-*' \) \
    -prune -exec rm -rf {} \; \
  # Remove all the build files except the llvm build which is symlinked into
  # the clang-toolchain directory.
  && find thirdparty/build/* -maxdepth 0 -type d ! -name 'llvm-*' -prune -exec rm -rf {} \;

# Common label arguments.
# VCS_REF is not specified to improve docker caching.
ARG DOCKERFILE
ARG MAINTAINER
ARG URL
ARG VCS_REF
ARG VCS_TYPE
ARG VCS_URL
ARG VERSION

LABEL name="Apache Kudu Thirdparty" \
      description="An image that has Kudu's thirdparty dependencies pre-built." \
      # Common labels.
      org.label-schema.dockerfile=$DOCKERFILE \
      org.label-schema.maintainer=$MAINTAINER \
      org.label-schema.url=$URL \
      org.label-schema.vcs-type=$VCS_TYPE \
      org.label-schema.vcs-url=$VCS_URL \
      org.label-schema.version=$VERSION

#
# ---- Build ----
# Builds an image that has the Kudu source code pre-built.
# This is useful for generating a runtime image, but can also be a
# useful base development image.
#
FROM thirdparty AS build

ARG BUILD_TYPE=release
ARG LINK_TYPE=static
ARG STRIP=1
ARG PARALLEL=4
# This is a common label argument, but also used in the build invocation.
ARG VCS_REF

# Use the bash shell for all RUN commands.
SHELL ["/bin/bash", "-c"]

WORKDIR /kudu
# Copy the C++ build source.
# We copy the minimal source to optimize docker cache hits.
COPY ./build-support build-support
COPY ./docs/support docs/support
COPY ./cmake_modules cmake_modules
COPY ./examples/cpp examples/cpp
COPY ./java/kudu-hive/ java/kudu-hive/
COPY ./src src
COPY ./CMakeLists.txt ./version.txt ./
# Build the c++ code.
WORKDIR /kudu/build/$BUILD_TYPE
# Ensure we don't rebuild thirdparty. Instead let docker handle caching.
ENV NO_REBUILD_THIRDPARTY=1
RUN ../../build-support/enable_devtoolset.sh \
  ../../thirdparty/installed/common/bin/cmake \
  -DCMAKE_BUILD_TYPE=$BUILD_TYPE \
  -DKUDU_LINK=$LINK_TYPE \
  -DKUDU_GIT_HASH=$VCS_REF \
  # The release build is massive with tests built.
  -DNO_TESTS=1 \
  ../.. \
  && make -j${PARALLEL} \
  # Install the client libraries for the python build to use.
  # TODO: Use custom install location when the python build can be configured to use it.
  && make install \
  # Strip the binaries to reduce the images size.
  && if [ "$STRIP" == "1" ]; then find "bin" -name "kudu*" -type f -exec strip {} \;; fi \
  # Strip the client libraries to reduce the images size
  && if [[ "$STRIP" == "1" ]]; then find "/usr/local" -name "libkudu*" -type f -exec strip {} \;; fi

# Copy the java build source.
COPY ./java /kudu/java
# Build the java code.
WORKDIR /kudu/java
RUN ./gradlew jar

# Copy the python build source.
COPY ./python /kudu/python
# Build the python code.
WORKDIR /kudu/python
RUN pip install -r requirements.txt \
  && python setup.py sdist

# Copy any remaining source files.
COPY . /kudu

# Common label arguments.
ARG DOCKERFILE
ARG MAINTAINER
ARG URL
ARG VCS_TYPE
ARG VCS_URL
ARG VERSION

LABEL name="Apache Kudu Build" \
      description="An image that has the Kudu source code pre-built." \
      org.apache.kudu.build.type=$BUILD_TYPE \
      org.apache.kudu.build.link=$LINK_TYPE \
      org.apache.kudu.build.stripped=$STRIP \
      # Common labels.
      org.label-schema.dockerfile=$DOCKERFILE \
      org.label-schema.maintainer=$MAINTAINER \
      org.label-schema.url=$URL \
      org.label-schema.vcs-ref=$VCS_REF \
      org.label-schema.vcs-type=$VCS_TYPE \
      org.label-schema.vcs-url=$VCS_URL \
      org.label-schema.version=$VERSION

#
# ---- Kudu Python ----
# Builds a runtime image with the Kudu python client pre-installed.
#
FROM runtime AS kudu-python

COPY ./docker/bootstrap-python-env.sh /
RUN ./bootstrap-python-env.sh \
  && rm bootstrap-python-env.sh

ARG INSTALL_DIR="/opt/kudu"

ENV LD_LIBRARY_PATH=$LD_LIBRARY_PATH:/usr/local/lib64
WORKDIR $INSTALL_DIR/python
# Copy the requirements file.
COPY ./python/requirements.txt requirements.txt
COPY --from=build /usr/local/lib/libkudu_client* /usr/local/lib/
COPY --from=build /usr/local/include/kudu /usr/local/include/kudu
COPY --from=build /kudu/python/dist/kudu-python-*.tar.gz .
RUN pip install -r requirements.txt \
    && rm -rf requirements.txt \
    && pip install kudu-python-*.tar.gz \
    && rm -rf kudu-python-*.tar.gz

ARG DOCKERFILE
ARG MAINTAINER
ARG URL
ARG VCS_TYPE
ARG VCS_URL
ARG VERSION

LABEL org.label-schema.name="Apache Kudu Python Client" \
      org.label-schema.description="An image with the Kudu Python client pre-installed." \
      # Common labels.
      org.label-schema.dockerfile=$DOCKERFILE \
      org.label-schema.maintainer=$MAINTAINER \
      org.label-schema.url=$URL \
      org.label-schema.vcs-type=$VCS_TYPE \
      org.label-schema.vcs-url=$VCS_URL \
      org.label-schema.version=$VERSION

# Entry point to the python.
CMD ["python"]

#
# ---- Kudu ----
# Builds a runtime image with the Kudu binaries pre-installed.
#
FROM runtime AS kudu

ARG UID=1000
ARG GID=1000
ARG INSTALL_DIR="/opt/kudu"
ARG DATA_DIR="/var/lib/kudu"

# Copy the binaries.
WORKDIR $INSTALL_DIR/bin
COPY --from=build /kudu/build/latest/bin/kudu ./
# Add to the binaries to the path.
ENV PATH=$INSTALL_DIR/bin/:$PATH

WORKDIR $INSTALL_DIR
# Copy the web files.
COPY --from=build /kudu/www ./www
COPY ./docker/kudu-entrypoint.sh /

# Setup the kudu user and create the neccessary directories.
RUN groupadd -g ${GID} kudu || groupmod -n kudu $(getent group ${GID} | cut -d: -f1) \
    && useradd --shell /bin/bash -u ${UID} -g ${GID} -m kudu \
    && chown -R kudu:kudu ${INSTALL_DIR} \
    && mkdir -p ${DATA_DIR} && chown -R kudu:kudu ${DATA_DIR}
USER kudu

# Add the entrypoint.
ENTRYPOINT ["/kudu-entrypoint.sh"]
CMD ["help"]

# Common label arguments.
ARG DOCKERFILE
ARG MAINTAINER
ARG URL
ARG VCS_REF
ARG VCS_TYPE
ARG VCS_URL
ARG VERSION

LABEL name="Apache Kudu" \
      description="An image with the Kudu binaries and clients pre-installed." \
      org.apache.kudu.build.type=$BUILD_TYPE \
      # Common labels.
      org.label-schema.dockerfile=$DOCKERFILE \
      org.label-schema.maintainer=$MAINTAINER \
      org.label-schema.url=$URL \
      org.label-schema.vcs-ref=$VCS_REF \
      org.label-schema.vcs-type=$VCS_TYPE \
      org.label-schema.vcs-url=$VCS_URL \
      org.label-schema.version=$VERSION
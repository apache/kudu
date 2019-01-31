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
# ---- Base ----
# Builds a base image that has all prerequisite libraries for
# development and runtime pre-installed.
# TODO: Consider a separate runtime-base and buildtime-base to make
#   runtime images smaller.
#
ARG BASE_OS
FROM $BASE_OS as base

COPY ./docker/bootstrap-env.sh /
RUN ./bootstrap-env.sh && rm bootstrap-env.sh

# Common label arguments.
# VCS_REF is not specified to improve docker caching.
ARG DOCKERFILE
ARG MAINTAINER
ARG URL
ARG VCS_TYPE
ARG VCS_URL
ARG VERSION

LABEL org.label-schema.name="Apache Kudu Base" \
      org.label-schema.description="A base image that has all prerequisite \
        libraries for development and runtime pre-installed." \
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
FROM base AS thirdparty

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

# TODO: Support other buildtypes.
ARG BUILD_TYPE=release
ARG PARALLEL=4

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
  # The release build is massive with tests built.
  -DNO_TESTS=1 \
  ../.. \
  && make -j${PARALLEL} \
  # Install the client libraries for the python build to use.
  && make install

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
ARG VCS_REF
ARG VCS_TYPE
ARG VCS_URL
ARG VERSION

LABEL name="Apache Kudu Build" \
      description="An image that has the Kudu source code pre-built." \
      org.apache.kudu.build.type=$BUILD_TYPE \
      # Common labels.
      org.label-schema.dockerfile=$DOCKERFILE \
      org.label-schema.maintainer=$MAINTAINER \
      org.label-schema.url=$URL \
      org.label-schema.vcs-ref=$VCS_REF \
      org.label-schema.vcs-type=$VCS_TYPE \
      org.label-schema.vcs-url=$VCS_URL \
      org.label-schema.version=$VERSION

#
# ---- Kudu ----
# Builds a runtime image with the Kudu binaries and clients pre-installed.
#
FROM base AS kudu

ARG INSTALL_DIR="/opt/kudu"

# Copy the binaries.
WORKDIR $INSTALL_DIR/bin
COPY --from=build \
  /kudu/build/latest/bin/kudu \
  /kudu/build/latest/bin/kudu-master \
  /kudu/build/latest/bin/kudu-tserver \
  ./
# Add to the binaries to the path.
ENV PATH=$INSTALL_DIR/bin/:$PATH

# Copy the python files and install.
WORKDIR $INSTALL_DIR/python
COPY --from=build /usr/local /usr/local/
ENV LD_LIBRARY_PATH=$LD_LIBRARY_PATH:/usr/local/lib64
COPY --from=build /kudu/python/dist/kudu-python-*.tar.gz .
RUN pip install kudu-python-*.tar.gz

# Copy the Java application jars.
WORKDIR $INSTALL_DIR/java
COPY --from=build \
 /kudu/java/kudu-backup/build/libs/*.jar \
 /kudu/java/kudu-client-tools/build/libs/*.jar \
 /kudu/java/kudu-spark-tools/build/libs/*.jar \
 ./

WORKDIR $INSTALL_DIR
# Copy the lib files.
COPY --from=build /kudu/build/latest/lib ./lib
# Copy the web files.
COPY --from=build /kudu/www ./www
COPY ./docker/kudu-entrypoint.sh /

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
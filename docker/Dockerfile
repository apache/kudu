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
      org.label-schema.description="A base image that has all the runtime libraries for Kudu pre-installed." \
      # Common labels.
      org.label-schema.dockerfile=$DOCKERFILE \
      org.label-schema.maintainer=$MAINTAINER \
      org.label-schema.url=$URL \
      org.label-schema.vcs-type=$VCS_TYPE \
      org.label-schema.vcs-url=$VCS_URL \
      org.label-schema.version=$VERSION

# Entry point to bash.
CMD ["/bin/bash"]

#
# ---- Dev ----
# Builds a base image that has all the development libraries for Kudu pre-installed.
#
ARG BASE_OS
FROM $BASE_OS as dev

COPY ./docker/bootstrap-dev-env.sh /
COPY ./docker/bootstrap-java-env.sh /
COPY ./docker/bootstrap-python-env.sh /
RUN ./bootstrap-dev-env.sh \
  && ./bootstrap-java-env.sh \
  && ./bootstrap-python-env.sh \
  && rm bootstrap-dev-env.sh \
  && rm bootstrap-java-env.sh \
  && rm bootstrap-python-env.sh

ENV PATH /usr/lib/ccache:/usr/lib64/ccache/:$PATH

# Entry point to bash.
CMD ["/bin/bash"]

# Common label arguments.
# VCS_REF is not specified to improve docker caching.
ARG DOCKERFILE
ARG MAINTAINER
ARG URL
ARG VCS_TYPE
ARG VCS_URL
ARG VERSION

LABEL org.label-schema.name="Apache Kudu Development Base" \
      org.label-schema.description="A base image that has all the development libraries for Kudu pre-installed." \
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

ARG UID=1000
ARG GID=1000
ARG BUILD_DIR="/kudu"

# Setup the kudu user and create the neccessary directories.
# We do this before copying any files othwerwise the image size is doubled by the chown change.
RUN groupadd -g ${GID} kudu || groupmod -n kudu $(getent group ${GID} | cut -d: -f1) \
    && useradd --shell /bin/bash -u ${UID} -g ${GID} -m kudu \
    && echo 'kudu ALL=(ALL) NOPASSWD:ALL' >> /etc/sudoers \
    && mkdir -p ${BUILD_DIR} && chown -R kudu:kudu ${BUILD_DIR}
# Run the build as the kudu user.
USER kudu

WORKDIR ${BUILD_DIR}
# We only copy the needed files for thirdparty so docker can handle caching.
COPY --chown=kudu:kudu ./thirdparty thirdparty
COPY --chown=kudu:kudu ./build-support/enable_devtoolset.sh \
  ./build-support/enable_devtoolset_inner.sh \
  build-support/
COPY --chown=kudu:kudu ./build-support/ccache-clang build-support/ccache-clang
COPY --chown=kudu:kudu ./build-support/ccache-devtoolset-3 build-support/ccache-devtoolset-3
RUN build-support/enable_devtoolset.sh \
  thirdparty/build-if-necessary.sh \
  # Remove the files left behind that we don't need.
  # Remove all the source files except the hadoop, hive, postgresql, ranger, and sentry sources
  # which are pre-built and symlinked into the installed/common/opt directory.
  && find thirdparty/src/* -maxdepth 0 -type d  \
    \( ! -name 'hadoop-*' ! -name 'hive-*' ! -name 'postgresql-*' ! -name 'ranger-*' ! -name 'sentry-*' \) \
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

# Entry point to bash.
CMD ["/bin/bash"]

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
# This is useful for generating a small runtime image,
# but can also be a useful base development image.
#
FROM thirdparty AS build

ARG BUILD_DIR="/kudu"
ARG BUILD_TYPE=release
ARG LINK_TYPE=static
ARG STRIP=1
ARG PARALLEL=4
# This is a common label argument, but also used in the build invocation.
ARG VCS_REF

# Use the bash shell for all RUN commands.
SHELL ["/bin/bash", "-c"]
# Run the build as the kudu user.
USER kudu

WORKDIR ${BUILD_DIR}
# Copy the C++ build source.
# We copy the minimal source to optimize docker cache hits.
COPY --chown=kudu:kudu ./build-support build-support
COPY --chown=kudu:kudu ./docs/support docs/support
COPY --chown=kudu:kudu ./cmake_modules cmake_modules
COPY --chown=kudu:kudu ./examples/cpp examples/cpp
COPY --chown=kudu:kudu ./src src
COPY --chown=kudu:kudu ./CMakeLists.txt ./version.txt ./

# Copy the java build source.
# Some parts of the C++ build depend on Java code.
COPY --chown=kudu:kudu ./java ${BUILD_DIR}/java

# Build the c++ code.
WORKDIR ${BUILD_DIR}/build/$BUILD_TYPE
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
  && sudo make install \
  # Strip the binaries to reduce the images size.
  && if [ "$STRIP" == "1" ]; then find "bin" -name "kudu*" -type f -exec strip {} \;; fi \
  # Strip the client libraries to reduce the images size
  && if [[ "$STRIP" == "1" ]]; then find "/usr/local" -name "libkudu*" -type f -exec strip {} \;; fi

# Build the java code.
WORKDIR ${BUILD_DIR}/java
RUN ./gradlew jar

# Copy the python build source.
COPY --chown=kudu:kudu ./python ${BUILD_DIR}/python
# Build the python code.
WORKDIR ${BUILD_DIR}/python
RUN pip install --user -r requirements.txt \
  && python setup.py sdist

# Copy any remaining source files.
WORKDIR ${BUILD_DIR}
COPY --chown=kudu:kudu . ${BUILD_DIR}

# Entry point to bash.
CMD ["/bin/bash"]

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

ARG UID=1000
ARG GID=1000
ARG BUILD_DIR="/kudu"
ARG INSTALL_DIR="/opt/kudu"

# Setup the kudu user and create the neccessary directories.
# We do this before copying any files othwerwise the image size is doubled by the chown change.
RUN groupadd -g ${GID} kudu || groupmod -n kudu $(getent group ${GID} | cut -d: -f1) \
    && useradd --shell /bin/bash -u ${UID} -g ${GID} -m kudu \
    && mkdir -p ${INSTALL_DIR} && chown -R kudu:kudu ${INSTALL_DIR}

COPY ./docker/bootstrap-python-env.sh /
RUN ./bootstrap-python-env.sh \
  && rm bootstrap-python-env.sh

# Install as the kudu user.
USER kudu

ENV LD_LIBRARY_PATH=$LD_LIBRARY_PATH:/usr/local/lib64
WORKDIR $INSTALL_DIR/python
# Copy the requirements file.
COPY ./python/requirements.txt requirements.txt
COPY --chown=kudu:kudu --from=build /usr/local/lib/libkudu_client* /usr/local/lib/
COPY --chown=kudu:kudu --from=build /usr/local/include/kudu /usr/local/include/kudu
COPY --chown=kudu:kudu --from=build ${BUILD_DIR}/python/dist/kudu-python-*.tar.gz .
RUN pip install --user -r requirements.txt \
    && rm -rf requirements.txt \
    && pip install --user kudu-python-*.tar.gz \
    && rm -rf kudu-python-*.tar.gz

# Entry point to Python.
CMD ["python"]

ARG DOCKERFILE
ARG MAINTAINER
ARG URL
ARG VCS_REF
ARG VCS_TYPE
ARG VCS_URL
ARG VERSION

LABEL org.label-schema.name="Apache Kudu Python Client" \
      org.label-schema.description="An image with the Kudu Python client pre-installed." \
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
# Builds a runtime image with the Kudu binaries pre-installed.
#
FROM runtime AS kudu

ARG UID=1000
ARG GID=1000
ARG BUILD_DIR="/kudu"
ARG INSTALL_DIR="/opt/kudu"
ARG DATA_DIR="/var/lib/kudu"

# Setup the kudu user and create the neccessary directories.
# We do this before copying any files othwerwise the image size is doubled by the chown change.
RUN groupadd -g ${GID} kudu || groupmod -n kudu $(getent group ${GID} | cut -d: -f1) \
    && useradd --shell /bin/bash -u ${UID} -g ${GID} -m kudu \
    && mkdir -p ${INSTALL_DIR} && chown -R kudu:kudu ${INSTALL_DIR} \
    && mkdir -p ${DATA_DIR} && chown -R kudu:kudu ${DATA_DIR}

# Copy the binaries.
WORKDIR $INSTALL_DIR/bin
COPY --chown=kudu:kudu --from=build ${BUILD_DIR}/build/latest/bin/kudu ./
# Add to the binaries to the path.
ENV PATH=$INSTALL_DIR/bin/:$PATH

# Copy the web files.
WORKDIR $INSTALL_DIR
COPY --chown=kudu:kudu --from=build ${BUILD_DIR}/www ./www

# Copy the entrypoint script.
COPY --chown=kudu:kudu ./docker/kudu-entrypoint.sh /

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

#
# ---- Impala Build ----
# Builds an image that has the Impala source code pre-built.
# This is useful for generating a small runtime image.
#
FROM dev AS impala-build

ARG IMPALA_VERSION="3.4.0"

# Use the bash shell for all RUN commands.
SHELL ["/bin/bash", "-c"]

# Install Maven.
COPY ./docker/bootstrap-maven-env.sh /
RUN ./bootstrap-maven-env.sh \
  && rm bootstrap-maven-env.sh

WORKDIR /impala

# Download and un-tar an Impala source release.
RUN wget -nv http://archive.apache.org/dist/impala/${IMPALA_VERSION}/apache-impala-${IMPALA_VERSION}.tar.gz -O apache-impala.tar.gz \
  && tar -xzf apache-impala.tar.gz --strip-components=1 \
  && rm apache-impala.tar.gz

# Build Impala
RUN source bin/impala-config.sh \
  && ./buildall.sh -release -noclean -notests \
  && docker/setup_build_context.py

# Copy to the expected install location in the runtime image.
# This helps avoid issues where docker won't follow symbolic links on copy.
RUN mkdir /opt/impala \
  && cp -Lr /impala/docker/build_context/release/bin /opt/impala/bin \
  && cp -Lr /impala/docker/build_context/release/lib /opt/impala/lib \
  && cp -Lr /impala/docker/build_context/release/www /opt/impala/www \
  && cp -Lr /impala/toolchain/cdh_components-*/hive-* /opt/hive \
  && cp -Lr /impala/toolchain/cdh_components-*/hadoop-* /opt/hadoop

# Common label arguments.
ARG DOCKERFILE
ARG MAINTAINER
ARG URL
ARG VCS_REF
ARG VCS_TYPE
ARG VCS_URL
ARG VERSION

LABEL name="Apache Impala Build" \
      description="An image that has the Impala source code pre-built." \
      org.apache.kudu.impala.version=$IMPALA_VERSION \
      # Common labels.
      org.label-schema.dockerfile=$DOCKERFILE \
      org.label-schema.maintainer=$MAINTAINER \
      org.label-schema.url=$URL \
      org.label-schema.vcs-ref=$VCS_REF \
      org.label-schema.vcs-type=$VCS_TYPE \
      org.label-schema.vcs-url=$VCS_URL \
      org.label-schema.version=$VERSION

#
# ---- Kudu Impala ----
# Builds a runtime image with the Impala binaries pre-installed.
# This image is only for use with the Kudu Quickstart.
#
FROM runtime AS impala

ARG UID=1000
ARG GID=1000
ARG DATA_DIR="/var/lib/impala"
ARG IMPALA_VERSION="3.3.0"

ENV IMPALA_HOME="/opt/impala"
ENV HIVE_HOME="/opt/hive"
ENV HIVE_CONF_DIR="/etc/hive/conf"
ENV HADOOP_HOME="/opt/hadoop"
ENV HADOOP_CONF_DIR="/etc/hadoop/conf"

# Setup the impala user and create the neccessary directories.
# We do this before copying any files othwerwise the image size is doubled by the chown change.
RUN groupadd -g ${GID} impala || groupmod -n impala $(getent group ${GID} | cut -d: -f1) \
    && useradd --shell /bin/bash -u ${UID} -g ${GID} -m impala \
    && mkdir -p ${IMPALA_HOME} && chown -R impala:impala ${IMPALA_HOME} \
    && mkdir -p ${HIVE_HOME} && chown -R impala:impala ${HIVE_HOME} \
    && mkdir -p ${HADOOP_HOME} && chown -R impala:impala ${HADOOP_HOME} \
    && mkdir -p ${DATA_DIR} && chown -R impala:impala ${DATA_DIR}

# Copy the Impala install.
WORKDIR $IMPALA_HOME
COPY --chown=impala:impala --from=impala-build /opt/impala ./
# Symlink here instead of in setup_build_context to avoid duplicate binaries.
WORKDIR $IMPALA_HOME/bin
RUN ln -s impalad statestored && ln -s impalad catalogd

# Copy the Hive install.
WORKDIR $HIVE_HOME
COPY --chown=impala:impala --from=impala-build /opt/hive ./

# Copy the Hadoop install.
WORKDIR $HADOOP_HOME
COPY --chown=impala:impala --from=impala-build /opt/hadoop ./

# Add to the binaries to the path.
ENV PATH=$IMPALA_HOME/bin/:$PATH
ENV PATH=$HIVE_HOME/bin/:$PATH
ENV PATH=$HADOOP_HOME/bin/:$PATH

# Copy the impala config files.
COPY ./docker/impala/etc /etc

WORKDIR /
# Install Java.
COPY ./docker/bootstrap-java-env.sh /
RUN ./bootstrap-java-env.sh \
  && rm bootstrap-java-env.sh
# Install Python.
COPY ./docker/bootstrap-python-env.sh /
RUN ./bootstrap-python-env.sh \
  && rm bootstrap-python-env.sh
# Install the impala-shell.
# TODO(ghenke): Install from the impala-build image.
RUN pip install impala-shell

# Copy the entrypoint script.
WORKDIR $IMPALA_HOME/bin
COPY --chown=impala:impala ./docker/impala-entrypoint.sh /

USER impala
# Add the entrypoint.
ENTRYPOINT ["/impala-entrypoint.sh"]
CMD ["help"]

# Common label arguments.
ARG DOCKERFILE
ARG MAINTAINER
ARG URL
ARG VCS_REF
ARG VCS_TYPE
ARG VCS_URL
ARG VERSION

LABEL name="Apache Impala" \
      description="An image with the Impala binaries pre-installed. This image is only for use with the Kudu Quickstart." \
      org.apache.kudu.impala.version=$IMPALA_VERSION \
      # Common labels.
      org.label-schema.dockerfile=$DOCKERFILE \
      org.label-schema.maintainer=$MAINTAINER \
      org.label-schema.url=$URL \
      org.label-schema.vcs-ref=$VCS_REF \
      org.label-schema.vcs-type=$VCS_TYPE \
      org.label-schema.vcs-url=$VCS_URL \
      org.label-schema.version=$VERSION

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
#
# An image with everything needed to run docs/support/scripts/make_site.sh
# on Linux. It is meant to be driven by
# docs/support/scripts/make_site_docker.sh, which uses the docker/ directory
# of the source tree as the build context.
#
# Ubuntu 22.04 is used as it ships OpenJDK 8 (required for the Javadoc).
# Ruby is built from source since some gems in
# docs/support/scripts/Gemfile.lock require Ruby 2.x, which is not available
# as a package. Doxygen is built from source as well to pin the version used
# for the C++ client API docs: warnings are treated as errors, and newer
# versions report warnings that older ones don't.

ARG BASE_OS=ubuntu:jammy
FROM ${BASE_OS}

ARG RUBY_VERSION=2.7.8
ARG DOXYGEN_VERSION=1.9.0

COPY ./bootstrap-dev-env.sh /
COPY ./bootstrap-java-env.sh /
RUN ./bootstrap-dev-env.sh \
  && ./bootstrap-java-env.sh \
  && rm bootstrap-dev-env.sh \
  && rm bootstrap-java-env.sh

# Install the docs build dependencies.
RUN apt-get update -y \
  && DEBIAN_FRONTEND=noninteractive apt-get install -y --no-install-recommends \
    bison \
    bzip2 \
    ca-certificates \
    graphviz \
    libffi-dev \
    libreadline-dev \
    libyaml-dev \
    xsltproc \
    zip \
    zlib1g-dev \
  && apt-get clean \
  && rm -rf /var/lib/apt/lists/* /tmp/* /var/tmp/*

# Build Ruby. ruby-build takes care of building a compatible OpenSSL version
# if necessary.
RUN git clone --depth 1 https://github.com/rbenv/ruby-build.git /tmp/ruby-build \
  && /tmp/ruby-build/bin/ruby-build ${RUBY_VERSION} /usr/local \
  && rm -rf /tmp/*

# Build Doxygen.
RUN DOXYGEN_TAG=Release_$(echo ${DOXYGEN_VERSION} | tr . _) \
  && curl -sSL -o /tmp/doxygen.tar.gz \
    https://github.com/doxygen/doxygen/archive/refs/tags/${DOXYGEN_TAG}.tar.gz \
  && tar -xzf /tmp/doxygen.tar.gz -C /tmp \
  && cmake -S /tmp/doxygen-${DOXYGEN_TAG} -B /tmp/doxygen-build -G Ninja \
    -DCMAKE_BUILD_TYPE=Release -DCMAKE_INSTALL_PREFIX=/usr/local \
  && ninja -C /tmp/doxygen-build install \
  && rm -rf /tmp/*

ENV UID=1000
ENV GID=1000

# The /work directory is where the persistent volume with the Kudu workspace,
# thirdparty and build artifacts gets mounted. It's created here with proper
# ownership so a fresh named volume inherits it.
RUN groupadd -g ${GID} kudu || groupmod -n kudu $(getent group ${GID} | cut -d: -f1) \
  && useradd --shell /bin/bash -u ${UID} -g ${GID} -m kudu \
  && echo 'kudu ALL=(ALL) NOPASSWD:ALL' >> /etc/sudoers \
  && mkdir -p /work \
  && chown -R kudu:kudu /work

# Without a UTF-8 locale, javadoc fails on non-ASCII characters in sources.
ENV LANG C.UTF-8
ENV PATH /usr/lib/ccache:$PATH
ENV CCACHE_DIR /work/.ccache
ENV GRADLE_USER_HOME /work/.gradle

USER kudu
WORKDIR /work

CMD ["/bin/bash"]

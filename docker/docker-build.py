#!/usr/bin/env python
##########################################################
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
##########################################################
#
# This script handles the coordination of building all of
# the Apache Kudu docker images.
#
# The format for tagging images is:
#   kudu:[image-type]-[version]-[operating-system]
#
# The image type is the name of any target image. However,
# the default image `kudu` will not include the image type
# in the tag because it is redundant with the repository
# name and intended to be the image used by most users.
# This allows us to support the common syntax of
# `kudu:latest` for normal users.
#
# The version tag for release images will be the full semantic
# version. An additional tag with just the minor version will
# also be created to allow for roll-forward semantics of
# maintenance releases. Tags with just the major version are not
# created because automatically rolling forward to a major version
# is not recommended. Images created for a snapshot version
# will use the short git hash in place of the version.
# An additional latest version can be used for the latest build.
#
# The operating system is described with the version name.
# If the operating system version is numeric, the operating
# system name will be included. For example, `centos:7` would
# be `centos7` while `ubuntu:xenial` would be `xenial`.
# An additional tag without the operating system included
# will be generated when the default operating system is used.
#
#   DOCKER_CACHE_FROM:
#      Optional images passed to the `docker build` commands
#      via the `--cache-from` option. This option tells Docker
#      images to consider as cache sources.
##########################################################

import argparse
import datetime
import os
import re
import subprocess
import sys

ME = os.path.abspath(__file__)
ROOT = os.path.abspath(os.path.join(os.path.dirname(ME), ".."))

DEFAULT_OS = 'ubuntu:bionic'
DEFAULT_TARGETS = ['kudu','kudu-python']
DEFAULT_REPOSITORY = 'apache/kudu'
DEFAULT_ACTION = 'load'

REQUIRED_CPUS = 4
REQUIRED_MEMORY_GIB = 4

def parse_args():
  """ Parses the command-line arguments """
  parser = argparse.ArgumentParser(description='Build the Apache Kudu Docker images',
                                   formatter_class=argparse.ArgumentDefaultsHelpFormatter)
  parser.add_argument('--platforms', nargs='+', choices=[
                      'linux/amd64', 'linux/arm64'],
                      help='The platforms to build with. If unspecified, the platform of your '
                           'build machine will be used.')
  parser.add_argument('--bases', nargs='+', default=DEFAULT_OS, choices=[
                      'centos:7', 'centos:8',
                      'ubuntu:bionic', 'ubuntu:focal',
                      'opensuse/leap:15'],
                      help='The base operating systems to build with')
  # These targets are defined in the Dockerfile. Dependent targets of a passed image will be built,
  # but not tagged. Note that if a target is not tagged it is subject removal by Dockers system
  # and image pruning.
  parser.add_argument('--targets', nargs='+', default=DEFAULT_TARGETS, choices=[
                      'runtime', 'dev', 'thirdparty', 'build',
                      'kudu', 'kudu-python', 'impala-build', 'impala'],
                      help='The targets to build and tag')
  parser.add_argument('--repository', default=DEFAULT_REPOSITORY,
                      help='The repository string to use when tagging the image')

  parser.add_argument('--action', default=DEFAULT_ACTION, choices=['load', 'push'],
                      help='The action to take with the built images. "load" will export the '
                           'images to docker so they can be used locally. "push" will push the '
                           'images to the registry.')

  parser.add_argument('--skip-latest', action='store_true',
                      help='If passed, skips adding a tag using `-latest` along with the '
                      'versioned tag')
  parser.add_argument('--tag-hash', action='store_true',
                      help='If passed, keeps the tags using the short git hash as the version '
                      'for non-release builds. Leaving this as false ensures the tags '
                      'containing the short git hash are removed which keeps the '
                      '`docker images` list cleaner when only the latest image is relevant')

  parser.add_argument('--cache-from', default='',
                      help='Optional images passed to the `docker build` commands via the '
                      '`--cache-from` option. This option tells Docker images to '
                      'consider as cache sources')

  parser.add_argument('--ignore-resource-checks', action='store_true',
                      help='Do not fail the script with resource validation errors')
  parser.add_argument('--dry-run', action='store_true',
                      help='Do not execute any Docker commands, only print what would be executed')

  return parser.parse_args()

def run_command(cmd, opts):
  """ Prints the command and run it if not in dry-run mode. """
  print('Running: %s' % cmd)
  if not opts.dry_run:
    subprocess.check_output(cmd, shell=True)

def use_buildx_builder():
  ret = subprocess.call(['docker', 'buildx', 'use', 'kudu-builder'])
  if ret != 0:
    subprocess.check_output(['docker', 'buildx', 'create', '--name', 'kudu-builder', '--use'])

def read_version():
  with open(os.path.join(ROOT, 'version.txt'), 'r') as vfile:
    return vfile.read().strip()

def read_vcs_ref():
  return subprocess.check_output(['git', 'rev-parse', '--short', 'HEAD']).strip().decode('utf-8')

def is_release_version(version):
  return not re.search('-SNAPSHOT', version)

def get_version_tag(version, vcs_ref):
    if is_release_version(version):
        return version
    else:
      if not vcs_ref:
        print('ERROR: Snapshot builds need to be built in a Git working directory')
        sys.exit(1)
      return vcs_ref

def get_minor_version(version):
  major_pos = version.find('.')
  minor_pos = version.find('.', major_pos + 1)
  return version[0:minor_pos]

def get_os_tag(base):
  """ Constructs an OS tag based on the passed base.
      The operating system is described with the version name.
      If the operating system version is numeric, the version will also be appended.
  """
  # If the base contains a "/" remove the prefix. ex: `opensuse/leap:15`
  if "/" in base:
    short_base = base.split('/')[1]
  else:
    short_base = base
  os_name = short_base.split(':')[0]
  os_version = short_base.split(':')[1]
  os_tag = os_name
  if os_version.isdigit():
    os_tag += os_version
  return os_tag

def get_full_tag(repository, target, version_tag, os_tag):
  """ Constructs a tag, excluding the OS_TAG if it is empty.
      Additionally ignores the target when it is the default target "kudu".
      Examples:
        get_tag "kudu" "latest" ""        = apache/kudu:latest
        get_tag "base" "1.8.0" ""         = apache/kudu:base-1.8.0
        get_tag "base" "1.8.0" "centos6"  = apache/kudu:base-1.8.0-centos6
  """
  full_tag = ''
  # Only include the target if this isn't the default.
  if target != 'kudu':
    full_tag += '%s-' % target
  full_tag += '%s-' % version_tag
  if os_tag:
    full_tag += '%s-' %  os_tag
  # Remove the last character to eliminate the extra '-'.
  full_tag = full_tag[:-1]
  return "%s:%s" % (repository, full_tag)

def platforms_csv(opts):
  if type(opts.platforms) is list:
    return ",".join(list(dict.fromkeys(opts.platforms)))
  else:
    return opts.platforms

def unique_bases(opts):
  if type(opts.bases) is list:
    return list(dict.fromkeys(opts.bases))
  else:
    return [opts.bases]

def unique_targets(opts):
  if type(opts.targets) is list:
    return list(dict.fromkeys(opts.targets))
  else:
    return [opts.targets]

def build_args(base, version, vcs_ref, cache_from):
  """ Constructs the build the arguments to pass to the Docker build. """
  args = ''
  args += ' --build-arg BASE_OS="%s"' % base
  args += ' --build-arg DOCKERFILE="docker/Dockerfile"'
  args += ' --build-arg MAINTAINER="Apache Kudu <dev@kudu.apache.org>"'
  args += ' --build-arg URL="https://kudu.apache.org"'
  args += ' --build-arg VERSION="%s"' % version
  args += ' --build-arg VCS_REF="%s"' % vcs_ref
  args += ' --build-arg VCS_TYPE="git"'
  args += ' --build-arg VCS_URL="https://gitbox.apache.org/repos/asf/kudu.git"'
  if cache_from:
    args += ' --cache-from %s' % cache_from
  return args

def build_tags(opts, base, version, vcs_ref, target):
  os_tag = get_os_tag(base)
  version_tag = get_version_tag(version, vcs_ref)

  tags = []
  full_tag = get_full_tag(opts.repository, target, version_tag, os_tag)
  # Don't tag hash versions unless tag_hash is true.
  if is_release_version(version) or opts.tag_hash:
    tags.append(full_tag)
  # If this is the default OS, also tag it without the OS-specific tag.
  if base == DEFAULT_OS:
    default_os_tag = get_full_tag(opts.repository, target, version_tag, '')
    # Don't tag hash versions unless tag_hash is true.
    if is_release_version(version) or opts.tag_hash:
      tags.append(default_os_tag)

  # Add the minor version tag if this is a release version.
  if is_release_version(version):
    minor_version = get_minor_version(version)
    minor_tag = get_full_tag(opts.repository, target, minor_version, os_tag)
    tags.append(minor_tag)
    # Add the default OS tag.
    if base == DEFAULT_OS:
      minor_default_os_tag = get_full_tag(opts.repository, target, minor_version, '')
      tags.append(minor_default_os_tag)

  # Add the latest version tags.
  if not opts.skip_latest:
    latest_tag = get_full_tag(opts.repository, target, 'latest', os_tag)
    tags.append(latest_tag)
    # Add the default OS tag.
    if base == DEFAULT_OS:
      latest_default_os_tag = get_full_tag(opts.repository, target, 'latest', '')
      tags.append(latest_default_os_tag)

  return tags

def verify_docker_resources(opts):
  cpus = int(subprocess.check_output(['docker', 'system', 'info',
                                      '--format', '{{.NCPU}}'])
             .strip().decode('utf-8'))
  if cpus < REQUIRED_CPUS:
    print('ERROR: At least %s CPUs are suggested to be configured for Docker (Found %s). '
          'To ignore this error pass --ignore-resource-checks' % (REQUIRED_CPUS, cpus))
    if not opts.ignore_resource_checks:
      sys.exit(1)

  memory_bytes = int(subprocess.check_output(['docker', 'system', 'info',
                                              '--format', '{{.MemTotal}}'])
                     .strip().decode('utf-8'))
  memory_gib = memory_bytes / (1024 * 1024 * 1024)
  if memory_gib < REQUIRED_MEMORY_GIB:
    print('ERROR: At least %s GiBs of memory is suggested to be configured for Docker'
          ' (Found %s GiBs). To ignore this error pass --ignore-resource-checks'
          % (REQUIRED_MEMORY_GIB, memory_gib))
    if not opts.ignore_resource_checks:
      sys.exit(1)

def main():
  start_time = datetime.datetime.now()
  print('Starting docker build: %s' % start_time.isoformat())
  opts = parse_args()
  verify_docker_resources(opts)

  # Enabled the docker buildkit so we can use advanced features
  # like skipping unused stages and mounting scripts that don't
  # need to remain in the image along with an improvement on
  # performance, storage management, feature functionality, and security.
  # https://docs.docker.com/develop/develop-images/build_enhancements/
  os.environ['DOCKER_BUILDKIT'] = '1'
  # Enable the experimental CLI so we can use `docker buildx` commands.
  os.environ['DOCKER_CLI_EXPERIMENTAL'] = 'enabled'
  # Create/Use a buildx builder to support pushing multi-platform builds.
  use_buildx_builder()

  version = read_version()
  vcs_ref = read_vcs_ref()
  print('Version: %s (%s)' % (version, vcs_ref))

  bases = unique_bases(opts)
  targets = unique_targets(opts)
  print('Bases: %s' % bases)
  print('Targets: %s' % targets)

  if opts.action == 'push' and not is_release_version(version):
    print('ERROR: Only release versions can be pushed. Found version %s (%s)'
          % (version, vcs_ref))
    sys.exit(1)

  for base in bases:
    print('Building targets for %s...' % base)

    for target in targets:
      target_tags = build_tags(opts, base, version, vcs_ref, target)
      # Build the target.
      print('Building %s target...' % target)
      # Note: It isn't currently possible to load multi-arch images into docker,
      # they can only be pushed to docker hub. This isn't expected to be a long
      # lived limitation.
      # https://github.com/docker/buildx/issues/59
      # https://github.com/moby/moby/pull/38738
      docker_build_cmd = 'docker buildx build --%s' % opts.action
      if opts.platforms:
        docker_build_cmd += ' --platform %s' % platforms_csv(opts)
      docker_build_cmd += build_args(base, version, vcs_ref, opts.cache_from)
      docker_build_cmd += ' --file %s' % os.path.join(ROOT, 'docker', 'Dockerfile')
      docker_build_cmd += ' --target %s' % target
      docker_build_cmd += ''.join(' --tag %s' % tag for tag in target_tags)
      docker_build_cmd += ' %s' % ROOT
      run_command(docker_build_cmd, opts)

  end_time = datetime.datetime.now()
  runtime = end_time - start_time
  print('Finished Docker build: %s (%s)' % (end_time.isoformat(), runtime))

if __name__ == '__main__':
    main()

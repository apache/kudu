#!/bin/bash
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

# Helpers for fetching pre-built thirdparty install trees from S3 and for
# creating pre-built tarballs when building from source.
#
# Variables:
#   USE_PREBUILT_THIRDPARTY   when set to 0, always build from source
#                             (default: 1).
#
#   PREBUILT_THIRDPARTY_URL   URL to fetch pre-built archives from
#                             (default: $DEPENDENCY_URL).

if [ -z "$TP_DIR" ]; then
  echo "ERROR: TP_DIR variable is not set, check your scripts" >&2
  exit 1
fi

source $TP_DIR/fetch-utils.sh

# Path to the directory storing freshly built or downloaded 3rd-party pre-built
# archives.
TP_PREBUILT_CACHE=$TP_DIR/prebuilt-cache

# Cached platform metadata used when forming pre-built tarball names.
_TP_PREBUILT_PLATFORM_INITIALIZED=
_TP_PREBUILT_OS_NAME=
_TP_PREBUILT_OS_VERSION=
_TP_PREBUILT_ARCH=
_TP_PREBUILT_TOOLCHAIN=

_ETC_OS_RELEASE=/etc/os-release

# Detect the host OS distribution name.
detect_prebuilt_os_name() {
  if [[ "$OSTYPE" == darwin* ]]; then
    echo "macos"
    return
  fi
  if [ -r $_ETC_OS_RELEASE ]; then
    . $_ETC_OS_RELEASE
    case "$ID" in
      # Translate some of the names into a bit more generic lineage-style
      # classification, and keep the rest as-is.
      centos|rocky|almalinux|ol) echo "rhel" ;;
      opensuse-leap|opensuse-tumbleweed) echo "sles" ;;
      *)
        if [ -n "$ID" ]; then
          echo "$ID"
        else
          echo "linux"
        fi
        ;;
    esac
    return
  fi
  echo "linux"
}

# Detect the host OS version string. Use only major version, ignore minor,
# patch version and the rest, if any.
detect_prebuilt_os_version() {
  if [[ "$OSTYPE" == darwin* ]]; then
    local ver=$(sw_vers -productVersion 2>/dev/null | cut -d. -f1)
    if [ -z "$ver" ]; then
      echo "ERROR: couldn't retrieve product version from sw_vers" >&2
      exit 1
    fi
    echo $ver
    return
  fi
  if [ -r $_ETC_OS_RELEASE ]; then
    . $_ETC_OS_RELEASE
    if [ -n "$VERSION_ID" ]; then
      local ver=$(echo $VERSION_ID | cut -d. -f1)
      if [ -z "$ver" ]; then
        echo "ERROR: couldn't retrieve OS version from /etc/os-release" >&2
        exit 1
      fi
      echo $ver
      return
    fi
  fi
  uname -r
}

# Detect compiler's flavor, major and minor version.
detect_prebuilt_toolchain() {
  local compiler="${CXX:-c++}"
  if ! $compiler -v > /dev/null 2>&1; then
    echo "ERROR: couldn't run compiler $compiler" >&2
    exit 1
  fi

  local version_str="$($compiler -v 2>&1 | grep -oE '[[:alnum:]]+ version [0-9.]+')"
  if [ -z "$version_str" ]; then
    echo "ERROR: unexpected compiler version info format" >&2
    exit 1
  fi
  local num_lines=$(echo "$version_str" | wc -l)
  if [ $num_lines -ne 1 ]; then
    echo "ERROR: unexpected compiler version info format: $version_str" >&2
    exit 1
  fi

  local name=$(echo $version_str | cut -d' ' -f1)
  if [ -z "$name" ]; then
    echo "ERROR: couldn't detect compiler family" >&2
    exit 1
  fi

  local version=$(echo $version_str | cut -d' ' -f3 | cut -d. -f1,2)
  if [ -z "$version" ]; then
    echo "ERROR: couldn't detect compiler version" >&2
    exit 1
  fi

  echo "${name}-${version}"
}

init_prebuilt_platform() {
  # The toolchain might change when building in different configurations
  # (e.g., starting TSAN-instrumented build after building regular binaries),
  # but the platform-related parameters aren't changing once they have been
  # initialized.
  _TP_PREBUILT_TOOLCHAIN="$(detect_prebuilt_toolchain)"
  if [ -n "$_TP_PREBUILT_PLATFORM_INITIALIZED" ]; then
    return
  fi
  _TP_PREBUILT_OS_NAME=$(detect_prebuilt_os_name)
  _TP_PREBUILT_OS_VERSION=$(detect_prebuilt_os_version)
  _TP_PREBUILT_ARCH=$(uname -m)
  _TP_PREBUILT_PLATFORM_INITIALIZED=1
}

# Form the component-specific part of the pre-built tarball's name for the
# given name of a 3rd-party component. That includes the version of the
# component (version of the released source code, etc.) and patch version.
# The former is defined by the component's upstream maintainers, the latter
# is assigned by Kudu maintainers.
component_name_to_tag() {
  local component_name=$1

  local name=$component_name
  case "$component_name" in
    # libcxx and libcxxabi are both parts of the LLVM/CLANG.
    libcxx|libcxxabi) name="llvm" ;;
    *) ;;
  esac
  name=$(echo $name | tr '[:lower:]' '[:upper:]' | tr '-' '_')

  # Building a variable in the xxx_NAME form: vars.sh contains corresponding
  # versioned strings for all the 3rd-party components.
  local version_var="${name}_VERSION"
  local version="${!version_var}"
  if [ -z "$version" ]; then
    echo "ERROR: $version_var isn't defined: check vars.sh" >&2
    exit 1
  fi
  local plevel_var="${name}_PATCHLEVEL"
  local plevel="${!plevel_var}"
  if [ -z "$plevel" ]; then
    echo "ERROR: $plevel_var isn't defined: check vars.sh" >&2
    exit 1
  fi
  echo ${component_name}-${version}.p${plevel}
}

# Form the pre-built tarball's name for a 3rd-party component built for the
# runtime of the host's OS using the current toolchain.
prebuilt_tarball_name() {
  local component=$1

  init_prebuilt_platform
  local comp_string=$(component_name_to_tag $component)
  echo "${comp_string}.${_TP_PREBUILT_OS_NAME}-${_TP_PREBUILT_OS_VERSION}.${_TP_PREBUILT_ARCH}.${_TP_PREBUILT_TOOLCHAIN}${MODE_SUFFIX}.tar.xz"
}

# Extract a pre-built tarball into $TP_INSTALL_DIR and create symlinks
# from pre-defined prefix paths to corresponding sub-directories
# in $TP_INSTALL_DIR.
extract_prebuilt_tarball() {
  local tarball_name=$1
  local install_subdir=$2

  local local_path="$TP_PREBUILT_CACHE/$tarball_name"
  local target_path="$TP_INSTALL_DIR/$install_subdir"
  mkdir -p "$target_path"

  init_tar_cmd
  echo "Unpacking pre-built $tarball_name into $target_path"
  $TAR_CMD xf "$local_path" -C "$target_path"

  # Create symbolic links from PREFIX_DIR to correspondig sub-directory
  # in TP_INSTALL_DIR. This is to align the location of pre-built binaries
  # and libraries with compiled-in PREFIX.
  for PREFIX_DIR in $PREFIX_COMMON $PREFIX_DEPS $PREFIX_DEPS_TSAN; do
    local subdir="$(basename $PREFIX_DIR)"
    if [ "x$subdir" != "x$install_subdir" ]; then
      continue
    fi
    local dirpath="$(dirname $PREFIX_DIR)"
    if [ ! -d "$dirpath" ]; then
      echo "ERROR: $dirpath unexpectedly disappeared" >&2
      exit 1
    fi
    pushd "$dirpath"
    ln -nsf "$target_path" "$subdir"
    popd
  done
}

# Create a pre-built tarball from the result artifacts staged in the staging
# directory.
create_prebuilt_tarball() {
  local staging_root=$1
  local tarball_name=$2

  mkdir -p "$TP_PREBUILT_CACHE"
  local local_path="$TP_PREBUILT_CACHE/$tarball_name"

  init_tar_cmd
  echo "Creating pre-built tarball $tarball_name"
  if ! $TAR_CMD cfJ "$local_path" -C "$staging_root$PREFIX" .; then
    echo "ERROR: failed creating pre-built tarball $tarball_name" >&2
    # Remove the tarball if something breaks: it's most likely corrupted
    rm -f "$local_path"
    exit 1
  fi
}

# Perform steps required after installing a component from a pre-built tarball
# or running 'make install' when building from source.
apply_post_install() {
  local component=$1
  local install_subdir=$2

  if [ "$component" = "llvm" -a "$install_subdir" = "uninstrumented" ]; then
    ln -sfn "$TP_INSTALL_DIR/uninstrumented" "$TP_DIR/clang-toolchain"
  fi
}

# Convert a component identifier into the name of its build_* function.
component_build_func() {
  local component=$1
  echo "build_${component//-/_}"
}

# Return any extra arguments required by a component's build function.
component_build_args() {
  local component=$1
  local install_subdir=$2

  case "$component" in
    llvm|libcxx)
      if [ "$install_subdir" = "tsan" ]; then
        echo tsan
      else
        echo normal
      fi
      ;;
  esac
}

# Invoke the build function corresponding to a component.
invoke_component_build() {
  local component=$1
  local install_subdir=$2

  if [ -z "$component" ]; then
    echo "ERROR: first argument (component name) must not be empty" >&2
    exit 1
  fi

  local build_func=$(component_build_func "$component")
  local build_args=$(component_build_args "$component" "$install_subdir")
  if ! declare -f "$build_func" >/dev/null; then
    echo "ERROR: unknown build function for $component: $build_func" >&2
    exit 1
  fi

  # Set the global variable INSTALL_DESTDIR to use in per-component
  # 'build_xxx()' functions.
  if [ "${USE_PREBUILT_THIRDPARTY:-1}" = "0" ]; then
    INSTALL_DESTDIR=""
  else
    INSTALL_DESTDIR="$TP_STAGING_DIR/$component"
  fi
  if [ -n "$build_args" ]; then
    "$build_func" "$build_args"
  else
    "$build_func"
  fi
}

# On some systems, autotools installs libraries to lib64 rather than lib.
# Fix this by setting up lib64 as a symlink to lib. It's necessary to do
# this step to handle cases where one third-party component depends
# on another, and the other component expects the libraries installed in
# 'lib' instead of 'lib64'.
create_lib_symlink() {
  local dir="$1"
  if [ -z "$dir" ]; then
    echo "ERROR: the first argument (prefix directory) must not be empty" >&2
    exit 1
  fi

  mkdir -p "$dir/lib"
  pushd "$dir"
  ln -nsf lib lib64
  popd
}

# Either fetch a pre-built 3rd-party component's artifacts or build them from
# source, packing the result into an archive (a compressed tarball). In the
# latter case, the produced archive can be uploaded into the appropriate
# location, so it's available for downloading afterwards. In both cases,
# the result archive is expanded, installing the pre-built artifacts under
# $TP_INSTALL_DIR. In addition, there are symbolic links from the compiled-in
# locations according to $PREFIX to the actual location of the files
# under $TP_INSTALL_DIR.
#
# Parameters:
#   $1 - component identifier, as in pre-built tarball names. The
#        corresponding build function is derived as build_<component>, with
#        hyphens converted to underscores: bitshuffle -> build_bitshuffle,
#        gumbo-parser -> build_gumbo_parser, etc.
#   $2 - install subdirectory under $TP_INSTALL_DIR (common,
#        uninstrumented, tsan)
fetch_prebuilt_or_build() {
  local component=$1
  local install_subdir=$2

  if [ -z "$component" ]; then
    echo "ERROR: first argument (component name) must not be empty" >&2
    exit 1
  fi

  if [ "${USE_PREBUILT_THIRDPARTY:-1}" = "0" ]; then
    create_lib_symlink "$PREFIX"
    invoke_component_build "$component" "$install_subdir"
    apply_post_install "$component" "$install_subdir"
    return
  fi

  init_prebuilt_platform

  local tarball_name=$(prebuilt_tarball_name "$component")

  if [ "${REBUILD_PREBUILT_THIRDPARTY:-0}" = "1" ]; then
    echo "REBUILD_PREBUILT_THIRDPARTY is set: rebuilding $component from source"
  else
    mkdir -p "$TP_PREBUILT_CACHE"
    local local_path="$TP_PREBUILT_CACHE/$tarball_name"
    if [ -f "$local_path" ]; then
      echo "Pre-built archive $tarball_name already exists locally"
    else
      echo "Fetching pre-built $tarball_name from $PREBUILT_THIRDPARTY_URL"
      if ! curl --retry 3 -fL -o "$local_path" "$PREBUILT_THIRDPARTY_URL/$tarball_name"; then
        echo "Couldn't download pre-built $tarball_name"
        # Remove the result file (if any): most likely, it's corrupted.
        rm -f "$local_path"
      fi
    fi
    if [ -f "$local_path" ]; then
      extract_prebuilt_tarball "$tarball_name" "$install_subdir"
      apply_post_install "$component" "$install_subdir"
      echo "Successfully installed pre-built artifacts from $tarball_name"
      return
    fi
    echo "Pre-built tarball $tarball_name not found; building $component from source"
  fi

  local staging_root="$TP_STAGING_DIR/$component"
  # Cleanup the staging sub-directory: there might be stale content
  # if the prior build attempt failed or was interrupted.
  rm -rf "$staging_root"

  create_lib_symlink "$staging_root$PREFIX"
  invoke_component_build "$component" "$install_subdir"
  create_prebuilt_tarball "$staging_root" "$tarball_name"
  extract_prebuilt_tarball "$tarball_name" "$install_subdir"
  apply_post_install "$component" "$install_subdir"
  rm -rf "$staging_root"
}

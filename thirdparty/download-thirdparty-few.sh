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

# autoreconf calls are necessary to fix hard-coded aclocal versions in the
# configure scripts that ship with the projects.

set -e

TP_DIR=$(cd "$(dirname "$BASH_SOURCE")"; pwd)

source $TP_DIR/vars.sh

if [[ "$OSTYPE" =~ ^linux ]]; then
  OS_LINUX=1
fi

delete_if_wrong_patchlevel() {
  local DIR=$1
  local PATCHLEVEL=$2
  if [ ! -f $DIR/patchlevel-$PATCHLEVEL ]; then
    echo It appears that $DIR is missing the latest local patches.
    echo Removing it so we re-download it.
    rm -Rf $DIR
  fi
}

unzip_to_source() {
  local FILENAME=$1
  local SOURCE=$2
  unzip -q "$FILENAME"
  # Parse out the unzipped top directory
  DIR_NAME=`unzip -qql "$FILENAME" | awk 'NR==1 {print $4}' | sed -e 's|^[/]*\([^/]*\).*|\1|'`
  # If the unzipped directory is the wrong name, move it.
  if [ "$SOURCE" != "$TP_SOURCE_DIR/$DIR_NAME" ]; then
    mv "$TP_SOURCE_DIR/$DIR_NAME" "$SOURCE"
  fi
}

fetch_and_expand() {
  local FILENAME=$1
  local SOURCE=$2
  local URL_PREFIX=$3

  if [ -z "$FILENAME" ]; then
    echo "Error: Must specify file to fetch"
    exit 1
  fi

  if [ -z "$URL_PREFIX" ]; then
    echo "Error: Must specify url prefix to fetch"
    exit 1
  fi

  TAR_CMD=tar
  if [[ "$OSTYPE" == "darwin"* ]] && which gtar &>/dev/null; then
    TAR_CMD=gtar
  fi

  FULL_URL="${URL_PREFIX}/${FILENAME}"

  SUCCESS=0
  # Loop in case we encounter an error.
  for attempt in 1 2 3; do
    if [ -r "$FILENAME" ]; then
      echo "Archive $FILENAME already exists. Not re-downloading archive."
    else
      echo "Fetching $FILENAME from $FULL_URL"
      if ! curl --retry 3 -L -O "$FULL_URL"; then
        echo "Error downloading $FILENAME"
        rm -f "$FILENAME"

        # Pause for a bit before looping in case the server throttled us.
        sleep 5
        continue
      fi
    fi

    echo "Unpacking $FILENAME to $SOURCE"
    if [[ "$FILENAME" =~ \.zip$ ]]; then
      if ! unzip_to_source "$FILENAME" "$SOURCE"; then
        echo "Error unzipping $FILENAME, removing file"
        rm "$FILENAME"
        continue
      fi
    elif [[ "$FILENAME" =~ \.(tar\.gz|tgz)$ ]]; then
      if ! $TAR_CMD xf "$FILENAME"; then
        echo "Error untarring $FILENAME, removing file"
        rm "$FILENAME"
        continue
      fi
    else
      echo "Error: unknown file format: $FILENAME"
      exit 1
    fi

    SUCCESS=1
    break
  done

  if [ $SUCCESS -ne 1 ]; then
    echo "Error: failed to fetch and unpack $FILENAME"
    exit 1
  fi

  # Allow for not removing previously-downloaded artifacts.
  # Useful on a low-bandwidth connection.
  if [ -z "$NO_REMOVE_THIRDPARTY_ARCHIVES" ]; then
    echo "Removing $FILENAME"
    rm $FILENAME
  fi
  echo
}

fetch_with_url_and_patch() {
  local FILENAME=$1
  local SOURCE=$2
  local PATCH_LEVEL=$3
  local URL_PREFIX=$4
  # Remaining args are expected to be a list of patch commands

  delete_if_wrong_patchlevel $SOURCE $PATCH_LEVEL
  if [ ! -d $SOURCE ]; then
    fetch_and_expand $FILENAME $SOURCE $URL_PREFIX
    pushd $SOURCE
    shift 4
    # Run the patch commands
    for f in "$@"; do
      eval "$f"
    done
    touch patchlevel-$PATCH_LEVEL
    popd
    echo
  fi
}

# Call fetch_with_url_and_patch with the default dependency URL source.
fetch_and_patch() {
  local FILENAME=$1
  local SOURCE=$2
  local PATCH_LEVEL=$3

  shift 3
  fetch_with_url_and_patch \
    $FILENAME \
    $SOURCE \
    $PATCH_LEVEL \
    $DEPENDENCY_URL \
    "$@"
}

mkdir -p $TP_SOURCE_DIR
cd $TP_SOURCE_DIR

#GPERFTOOLS_PATCHLEVEL=2
#fetch_and_patch \
# gperftools-${GPERFTOOLS_VERSION}.tar.gz \
# $GPERFTOOLS_SOURCE \
# $GPERFTOOLS_PATCHLEVEL \
# "patch -p1 < $TP_DIR/patches/gperftools-Replace-namespace-base-with-namespace-tcmalloc.patch" \
# "patch -p1 < $TP_DIR/patches/gperftools-unbreak-memz.patch" \
# "autoreconf -fvi"

RAPIDJSON_PATCHLEVEL=0
fetch_and_patch \
 rapidjson-${RAPIDJSON_VERSION}.zip \
 $RAPIDJSON_SOURCE \
 $RAPIDJSON_PATCHLEVEL

echo "---------------"
echo "Thirdparty dependencies downloaded successfully"

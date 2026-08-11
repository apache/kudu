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

# Shared helpers for downloading and unpacking thirdparty source archives.
# Sourced by download-thirdparty.sh and prebuilt-utils.sh.

if [ -z "$TP_DIR" ]; then
  echo "TP_DIR variable not set, check your scripts"
  exit 1
fi

# Select a tar implementation that supports the flags we rely on.
init_tar_cmd() {
  if [ -n "$TAR_CMD" ]; then
    return
  fi
  TAR_CMD=tar
  if [[ "$OSTYPE" == "darwin"* ]] && which gtar &>/dev/null; then
    TAR_CMD=gtar
  fi
}

delete_if_wrong_patchlevel() {
  local DIR=$1
  local PATCHLEVEL=$2
  if [ ! -f $DIR/patchlevel-$PATCHLEVEL ]; then
    echo It appears that $DIR is missing the latest local patches.
    echo Removing it so we re-download it.
    rm -Rf $DIR
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

  init_tar_cmd

  FULL_URL="${URL_PREFIX}/${FILENAME}"

  SUCCESS=0
  # Loop in case we encounter an error.
  for attempt in 1 2 3; do
    if [ -r "$FILENAME" ]; then
      echo "Archive $FILENAME already exists. Not re-downloading archive."
    else
      echo "Fetching $FILENAME from $FULL_URL"
      if ! curl --retry 3 -fL -O "$FULL_URL"; then
        echo "Error downloading $FILENAME"
        rm -f "$FILENAME"

        # Pause for a bit before looping in case the server throttled us.
        sleep 5
        continue
      fi
    fi

    echo "Unpacking $FILENAME to $SOURCE"
    if [[ "$FILENAME" =~ \.zip$ ]]; then
      # Unzip the archive, replacing files if they already present,
      # overwriting them with the files from the archive.
      if ! unzip -qo "$FILENAME"; then
        echo "Error unzipping $FILENAME, removing file"
        rm -f "$FILENAME"
        continue
      fi
      # Parse out the unzipped top directory
      local DIR_NAME=`unzip -qql "$FILENAME" | awk 'NR==1 {print $4}' | sed -e 's|^[/]*\([^/]*\).*|\1|'`
      if [ -z "$DIR_NAME" ]; then
        echo "Unexpected behavior from unzip on $FILENAME, removing file"
        rm -f "$FILENAME"
        continue
      fi
      # If the unzipped directory has the wrong name, rename/move it.
      if [ "$SOURCE" != "$DIR_NAME" ]; then
        mv -f "$DIR_NAME" "$SOURCE"
      fi
    elif [[ "$FILENAME" =~ \.(tar\.gz|tgz)$ ]]; then
      if ! $TAR_CMD xf "$FILENAME"; then
        echo "Error untarring $FILENAME, removing file"
        rm "$FILENAME"
        continue
      fi
    elif [[ "$FILENAME" =~ \.jar$ ]]; then
      mkdir ${FILENAME%.jar}
      cp $FILENAME ${FILENAME%.jar}/
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

  if [ -z "$TP_SOURCE_DIR" ]; then
    echo "TP_SOURCE_DIR is not defined"
    exit 1
  fi
  mkdir -p $TP_SOURCE_DIR
  pushd $TP_SOURCE_DIR
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
  popd
}

# Call fetch_with_url_and_patch with the default dependency URL source.
fetch_and_patch_src() {
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

# Deduce all the necessary arguments for 'fetch_and_patch_src' function
# given the name of a 3rd-party component, and invoke the function.
fetch_and_patch() {
  local component=$1
  if [ -z "$component" ]; then
    echo "ERROR: first argument (component name) must not be empty" >&2
    exit 1
  fi
  component=$(echo $component | tr '[:lower:]' '[:upper:]' | tr '-' '_')

  # Building variables in the xxx_{NAME,SOURCE,PATCHLEVEL,PATCHES} form
  # using information provided in vars.sh.
  local archive_var="${component}_ARCHIVE"
  local archive="${!archive_var}"
  if [ -z "$archive" ]; then
    echo "ERROR: $archive_var isn't defined or empty: check vars.sh" >&2
    exit 1
  fi

  local src_var="${component}_NAME"
  local src="${!src_var}"
  if [ -z "$src" ]; then
    echo "ERROR: $src_var isn't defined or empty: check vars.sh" >&2
    exit 1
  fi

  local plevel_var="${component}_PATCHLEVEL"
  local plevel="${!plevel_var}"
  if [ -z "$plevel" ]; then
    echo "ERROR: $plevel_var isn't defined or empty: check vars.sh" >&2
    exit 1
  fi

  # Namerefs (declare -n ...) aren't available in bash versions prior to 4.3,
  # so using eval to reconstruct the array locally for wider portability
  # across bash versions.

  # xxx_PATCHES may be empty if there isn't any patches to apply
  eval "local patches=(\"\${${component}_PATCHES[@]}\")"

  # xxx_EXTRA_COMMANDS may be empty if there isn't any extra commands to run
  eval "local extra_commands=(\"\${${component}_EXTRA_COMMANDS[@]}\")"

  fetch_and_patch_src $archive $src $plevel "${patches[@]}" "${extra_commands[@]}"
}

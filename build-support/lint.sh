#!/bin/bash
# Copyright (c) 2014, Cloudera, inc.
# Confidential Cloudera Information: Covered by NDA.

ME=$(dirname $BASH_SOURCE)
ROOT=$(readlink -f $ME/..)

TMP=$(mktemp)
trap "rm $TMP" EXIT

ONLY_CHANGED=false

for flag in "$@" ; do
  case $flag in
    --changed-only | -c)
      ONLY_CHANGED=true
      ;;
    *)
      echo unknown flag: $flag
      exit 1
      ;;
  esac
done

if $ONLY_CHANGED; then
  FILES=$(git diff --name-only $($ME/get-upstream-commit.sh)  \
    | egrep  '\.(cc|h)$' | grep -v "gutil\|trace_event")
  if [ -z "$FILES" ]; then
    echo No source files changed
    exit 0
  fi
else
  FILES=$(find $ROOT/src -name '*.cc' -or -name '*.h' | grep -v "\.pb\.\|\.service\.\|\.proxy\.\|\.krpc\.\|gutil\|trace_event\|kudu_export\.h")
fi

cd $ROOT

$ROOT/thirdparty/installed/bin/cpplint.py \
  --verbose=4 \
  --filter=-whitespace/comments,-readability/todo,-build/header_guard,-build/include_order \
  $FILES 2>&1 | grep -v 'Done processing' | tee $TMP

NUM_ERRORS=$(grep "Total errors found" $TMP | awk '{print $4}')

if [ "$NUM_ERRORS" -ne 0 ]; then
  exit 1
fi

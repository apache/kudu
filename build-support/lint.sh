#!/bin/bash

ME=$(dirname $BASH_SOURCE)
ROOT=$(readlink -f $ME/..)

TMP=$(mktemp)
trap "rm $TMP" EXIT

$ROOT/thirdparty/installed/bin/cpplint.py \
  --verbose=4 \
  --filter=-whitespace/comments,-whitespace/line_length,-readability/todo,-build/header_guard,-build/include_order \
  $(find $ROOT/src -name *.cc -or -name *.h | grep -v .pb. | grep -v gutil) \
  2>&1 | grep -v 'Done processing' | tee $TMP

NUM_ERRORS=$(grep "Total errors found" $TMP | awk '{print $4}')

if [ "$NUM_ERRORS" -ne 0 ]; then
  exit 1
fi

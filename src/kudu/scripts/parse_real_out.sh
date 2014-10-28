#! /usr/bin/env bash
# Copyright (c) 2014, Cloudera, inc.
# Confidential Cloudera Information: Covered by NDA.
#
# This script takes a line in stdin and parses the real time out of it.
# Input example:
#  "Times for Insert 10000000 keys: real 16.438s	user 16.164s	sys 0.229s"
# Output:
#  16.438

awk -F 'real ' '{ print $2 }' | awk -F s '{ print $1 }'

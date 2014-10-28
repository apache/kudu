#!/bin/bash
# Copyright (c) 2014, Cloudera, inc.
# Confidential Cloudera Information: Covered by NDA.
#
# Script which tries to determine the most recent git hash in the current
# branch which was checked in by gerrit. This commit hash is printed to
# stdout.
#
# It does so by looking for the verification signoff from Jenkins.  This is
# more foolproof than trying to guess at the "origin/" branch name, since the
# developer might be working on some local topic branch.
set -e

git log --grep='Tested-by: jenkins' -n1 --pretty=format:%H

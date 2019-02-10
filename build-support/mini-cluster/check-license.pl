#!/usr/bin/perl
###############################################################################
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
###############################################################################
# This script will check a Kudu binary jar distribution to ensure that all
# included shared objects are mentioned in LICENSE.txt;
# The JAR must first be unpacked and this script pointed to the directory
# within the JAR containing the LICENSE.txt file
###############################################################################
use strict;
use warnings;
use File::Basename qw(dirname);

if (scalar @ARGV != 1) {
  print STDERR "Usage: $0 binary-jar-unpacked-prefix-dir\n";
  print STDERR "  Where binary-jar-unpacked-prefix-dir is the directory within the jar\n";
  print STDERR "  containing the LICENSE.txt file.\n";
  exit 1;
}
my $jar_prefix = $ARGV[0];

# Read the CMake config files and parse out the libraries that are part of the
# Kudu project.
my $script_dir = dirname $0;
my $src_root = "$script_dir/../..";
chomp(my @project_deps = `find $src_root/src -name CMakeLists.txt | xargs egrep 'add_library|ADD_EXPORTABLE_LIBRARY'`);
for (@project_deps) {
  s/^.*?://;      # Strip off leading filename from grep.
  s/^[^(]+\(//;   # Strip off CMake function / macro name
  s/ .*//;        # Retain only the first argument to each add_library() call which is the library name.
  s/^/lib/;       # Prepend "lib" to each library name to match the shared object name.
}

# Read the LICENSE.txt file from the binary test jar and parse out the library
# dependencies.
my $jar_lic_file = "$jar_prefix/LICENSE.txt";
open(FILE, "< $jar_lic_file") or die "Cannot open $jar_lic_file: $!";
chomp(my @contents = grep { /^libraries:/ } <FILE>);
close FILE;
my @external_deps;
foreach my $line (@contents) {
  $line =~ s/^libraries: //;
  my @deps = split(/,\s*/, $line);
  push @external_deps, @deps;
}

# Create a regular expression to determine if there are any libraries shipped
# in the jar file that are not accounted for by either the CMake project files
# or the LICENSE.txt file.
my @pats = map { "\\b$_\\b" } @project_deps, @external_deps;
my $pat_str = join("|", @pats);
my $pat_known_deps = qr($pat_str);

# List the libraries in the binary test jar and print any that don't correspond
# to known deps.
my $seen_unknown_deps = 0;
chomp(my @jar_libs = `cd $jar_prefix && find lib/ -type f`);
foreach my $lib (@jar_libs) {
  if ($lib !~ $pat_known_deps) {
    print STDERR "unknown license: $lib\n";
    $seen_unknown_deps++;
  }
}
if (!$seen_unknown_deps) {
  print "OK\n";
  exit 0;
}
print "Found $seen_unknown_deps unknown dependencies\n";
exit 1;

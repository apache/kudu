#!/usr/bin/perl
################################################################################
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
################################################################################
# This script verifies that the dependencies shipped in the Kudu jars do not
# change over time without us noticing.
################################################################################
use strict;
use warnings;

# Prefix for shaded classes.
my $pat_kudu_shaded_prefix = qr{^org/apache/kudu/shaded/};

# Allowed filenames of non-Java files in JARs.
my $pat_allow_non_java =
    qr{(?:\.(?:txt|xml|properties|proto|MF)|
          LICENSE|NOTICE|DEPENDENCIES|
          # The kudu-spark DataSourceRegister file.
          DataSourceRegister)$}x;

# Allowed filenames of shaded dependencies in JARs.
my $pat_allow_kudu_shaded =
    qr{^org/apache/kudu/shaded/
        (?:com/google/(?:common|gson|gradle/osdetector|protobuf|thirdparty/publicsuffix)|
           com/sangupta/murmur|
           kr/motd/maven|
           org/apache/(?:commons|http)|
           org/checkerframework|
           org/HdrHistogram|
           org/jboss/netty|
           scopt)
      }x;

# Allowed paths of unshaded Kudu dependencies in JARs.
# Currently, there is no restriction imposed for org.apache.kudu classes.
my $pat_allow_kudu_unshaded = qr{^org/apache/kudu/.*};

# Allowed paths of unshaded non-Kudu dependencies in JARs.
my $pat_allow_nonkudu_unshaded = qr{^(?:com/databricks/spark/avro|
                                        com/stumbleupon/async/|
                                        org/apache/parquet/|
                                        org/apache/yetus/)}x;

if (scalar @ARGV != 1) {
  print STDERR "Usage: $0 <dest_dir>\n";
  exit 1;
}

my $dest_dir = $ARGV[0];
chdir($dest_dir) or die "cannot chdir to destination directory $dest_dir: $!";
print "Checking jars in directory: " . `pwd`;

chomp(my @jars = `find . -type f -name \*.jar |
                         grep -v build/jars |
                         grep -v tests\.jar |
                         grep -v tests-shaded\.jar |
                         grep -v original |
                         grep -v sources\.jar |
                         grep -v javadoc\.jar |
                         grep -v unshaded\.jar |
                         grep -v buildSrc.jar |
                         grep -v gradle-wrapper.jar |
                         # Ignored because it's test only and unpublished.
                         grep -v kudu-jepsen.*\.jar |
                         # Ignored because it's a tool jar that shades everything.
                         grep -v kudu-backup-tools.*\.jar |
                         # Ignored because it's an internal jar that shades everything.
                         grep -v kudu-subprocess.*\.jar`
                         );

my $num_errors = 0;

foreach my $jar (@jars) {
  print "> $jar\n";
  chomp(my @files = `jar tf $jar`);
  foreach my $file (@files) {
    # In each case, ensure the files match the expected patterns.
    if ($file =~ qr{/$}) {
      # A directory. Skip.
    } elsif ($file !~ qr{\.class$}) {
      # Non-Java stuff.
      if ($file !~ $pat_allow_non_java) {
        $num_errors++;
        print "NON_JAVA $file\n";
      }
    } elsif ($file =~ $pat_kudu_shaded_prefix) {
      # Shaded Kudu classes.
      if ($file !~ $pat_allow_kudu_shaded) {
        $num_errors++;
        print "KUDU_SHADED $file\n";
      }
    } elsif ($file =~ qr{^org/apache/kudu}) {
      # Unshaded Kudu classes.
      if ($file !~ $pat_allow_kudu_unshaded) {
        $num_errors++;
        print "KUDU_UNSHADED $file\n";
      }
    } else {
      # Non-Kudu classes.
      if ($file !~ $pat_allow_nonkudu_unshaded) {
        $num_errors++;
        print "NON_KUDU $file\n";
      }
    }
  }
}

if ($num_errors != 0) {
  print "Found $num_errors errors.\n";
  exit 1;
}
print "OK.\n";
exit 0;
